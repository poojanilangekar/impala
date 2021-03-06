// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "parquet-column-readers.h"

#include <boost/scoped_ptr.hpp>
#include <string>
#include <sstream>
#include <gflags/gflags.h>
#include <gutil/strings/substitute.h>

#include "exec/hdfs-parquet-scanner.h"
#include "exec/parquet-metadata-utils.h"
#include "exec/parquet-scratch-tuple-batch.h"
#include "exec/read-write-util.h"
#include "exec/scanner-context.inline.h"
#include "rpc/thrift-util.h"
#include "runtime/collection-value-builder.h"
#include "runtime/exec-env.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "util/bit-util.h"
#include "util/codec.h"
#include "util/debug-util.h"
#include "util/dict-encoding.h"
#include "util/rle-encoding.h"

#include "common/names.h"

// Provide a workaround for IMPALA-1658.
DEFINE_bool(convert_legacy_hive_parquet_utc_timestamps, false,
    "When true, TIMESTAMPs read from files written by Parquet-MR (used by Hive) will "
    "be converted from UTC to local time. Writes are unaffected.");

// Max dictionary page header size in bytes. This is an estimate and only needs to be an
// upper bound.
static const int MAX_DICT_HEADER_SIZE = 100;

// Max data page header size in bytes. This is an estimate and only needs to be an upper
// bound. It is theoretically possible to have a page header of any size due to string
// value statistics, but in practice we'll have trouble reading string values this large.
// Also, this limit is in place to prevent impala from reading corrupt parquet files.
DEFINE_int32(max_page_header_size, 8*1024*1024, "max parquet page header size in bytes");

// Trigger debug action on every other call of Read*ValueBatch() once at least 128
// tuples have been produced to simulate failure such as exceeding memory limit.
// Triggering it every other call so as not to always fail on the first column reader
// when materializing multiple columns. Failing on non-empty row batch tests proper
// resources freeing by the Parquet scanner.
#ifndef NDEBUG
static int debug_count = 0;
#define SHOULD_TRIGGER_DEBUG_ACTION(num_tuples) \
    ((debug_count++ % 2) == 1 && num_tuples >= 128)
#else
#define SHOULD_TRIGGER_DEBUG_ACTION(x) (false)
#endif

using namespace impala::io;

using parquet::Encoding;

namespace impala {

const string PARQUET_COL_MEM_LIMIT_EXCEEDED =
    "ParquetColumnReader::$0() failed to allocate $1 bytes for $2.";

Status ParquetLevelDecoder::Init(const string& filename,
    Encoding::type encoding, MemPool* cache_pool, int cache_size,
    int max_level, int num_buffered_values, uint8_t** data, int* data_size) {
  DCHECK(*data != nullptr);
  DCHECK_GE(*data_size, 0);
  DCHECK_GE(num_buffered_values, 0);
  DCHECK_GT(cache_size, 0);
  cache_size = BitUtil::RoundUpToPowerOf2(cache_size, 32);
  encoding_ = encoding;
  max_level_ = max_level;
  num_buffered_values_ = num_buffered_values;
  filename_ = filename;
  RETURN_IF_ERROR(InitCache(cache_pool, cache_size));

  // Return because there is no level data to read, e.g., required field.
  if (max_level == 0) return Status::OK();

  int32_t num_bytes = 0;
  switch (encoding) {
    case Encoding::RLE: {
      Status status;
      if (!ReadWriteUtil::Read(data, data_size, &num_bytes, &status)) {
        return status;
      }
      if (num_bytes < 0 || num_bytes > *data_size) {
        return Status(TErrorCode::PARQUET_CORRUPT_RLE_BYTES, filename, num_bytes);
      }
      int bit_width = BitUtil::Log2Ceiling64(max_level + 1);
      rle_decoder_.Reset(*data, num_bytes, bit_width);
      break;
    }
    case parquet::Encoding::BIT_PACKED:
      return Status(TErrorCode::PARQUET_BIT_PACKED_LEVELS, filename);
    default: {
      stringstream ss;
      ss << "Unsupported encoding: " << encoding;
      return Status(ss.str());
    }
  }
  if (UNLIKELY(num_bytes < 0 || num_bytes > *data_size)) {
    return Status(Substitute("Corrupt Parquet file '$0': $1 bytes of encoded levels but "
        "only $2 bytes left in page", filename, num_bytes, *data_size));
  }
  *data += num_bytes;
  *data_size -= num_bytes;
  return Status::OK();
}

Status ParquetLevelDecoder::InitCache(MemPool* pool, int cache_size) {
  num_cached_levels_ = 0;
  cached_level_idx_ = 0;
  // Memory has already been allocated.
  if (cached_levels_ != nullptr) {
    DCHECK_EQ(cache_size_, cache_size);
    return Status::OK();
  }

  cached_levels_ = reinterpret_cast<uint8_t*>(pool->TryAllocate(cache_size));
  if (cached_levels_ == nullptr) {
    return pool->mem_tracker()->MemLimitExceeded(
        nullptr, "Definition level cache", cache_size);
  }
  memset(cached_levels_, 0, cache_size);
  cache_size_ = cache_size;
  return Status::OK();
}

inline int16_t ParquetLevelDecoder::ReadLevel() {
  if (UNLIKELY(!CacheHasNext())) {
    if (UNLIKELY(!FillCache(cache_size_, &num_cached_levels_))) {
      return HdfsParquetScanner::INVALID_LEVEL;
    }
    DCHECK_GE(num_cached_levels_, 0);
    if (UNLIKELY(num_cached_levels_ == 0)) {
      return HdfsParquetScanner::INVALID_LEVEL;
    }
  }
  return CacheGetNext();
}

Status ParquetLevelDecoder::CacheNextBatch(int vals_remaining) {
  /// Fill the cache completely if there are enough values remaining.
  /// Otherwise don't try to read more values than are left.
  int batch_size = min(vals_remaining, cache_size_);
  if (max_level_ > 0) {
    if (UNLIKELY(!FillCache(batch_size, &num_cached_levels_) ||
          num_cached_levels_ < batch_size)) {
      return Status(decoding_error_code_, num_buffered_values_, filename_);
    }
  } else {
    // No levels to read, e.g., because the field is required. The cache was
    // already initialized with all zeros, so we can hand out those values.
    DCHECK_EQ(max_level_, 0);
    cached_level_idx_ = 0;
    num_cached_levels_ = batch_size;
  }
  return Status::OK();
}

bool ParquetLevelDecoder::FillCache(int batch_size, int* num_cached_levels) {
  DCHECK(!CacheHasNext());
  DCHECK(num_cached_levels != nullptr);
  DCHECK_GE(max_level_, 0);
  DCHECK_GE(*num_cached_levels, 0);
  cached_level_idx_ = 0;
  if (max_level_ == 0) {
    // No levels to read, e.g., because the field is required. The cache was
    // already initialized with all zeros, so we can hand out those values.
    *num_cached_levels = batch_size;
    return true;
  }
  DCHECK_EQ(encoding_, parquet::Encoding::RLE);
  *num_cached_levels = rle_decoder_.GetValues(batch_size, cached_levels_);
  return *num_cached_levels > 0;
}

/// Per column type reader. InternalType is the datatype that Impala uses internally to
/// store tuple data and PARQUET_TYPE is the corresponding primitive datatype (as defined
/// in the parquet spec) that is used to store column values in parquet files.
/// If MATERIALIZED is true, the column values are materialized into the slot described
/// by slot_desc. If MATERIALIZED is false, the column values are not materialized, but
/// the position can be accessed.
template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
class ScalarColumnReader : public BaseScalarColumnReader {
 public:
  ScalarColumnReader(HdfsParquetScanner* parent, const SchemaNode& node,
      const SlotDescriptor* slot_desc);
  virtual ~ScalarColumnReader() { }

  virtual bool ReadValue(MemPool* pool, Tuple* tuple) override {
    return ReadValue<true>(tuple);
  }

  virtual bool ReadNonRepeatedValue(MemPool* pool, Tuple* tuple) override {
    return ReadValue<false>(tuple);
  }

  virtual bool ReadValueBatch(MemPool* pool, int max_values, int tuple_size,
      uint8_t* tuple_mem, int* num_values) override {
    return ReadValueBatch<true>(max_values, tuple_size, tuple_mem, num_values);
  }

  virtual bool ReadNonRepeatedValueBatch(MemPool* pool, int max_values, int tuple_size,
      uint8_t* tuple_mem, int* num_values) override {
    return ReadValueBatch<false>(max_values, tuple_size, tuple_mem, num_values);
  }

  virtual DictDecoderBase* GetDictionaryDecoder() override {
    return HasDictionaryDecoder() ? &dict_decoder_ : nullptr;
  }

  virtual bool NeedsConversion() override { return NeedsConversionInline(); }
  virtual bool NeedsValidation() override { return NeedsValidationInline(); }

 protected:
  template <bool IN_COLLECTION>
  inline bool ReadValue(Tuple* tuple);

  /// Implementation of the ReadValueBatch() functions specialized for this
  /// column reader type. This function drives the reading of data pages and
  /// caching of rep/def levels. Once a data page and cached levels are available,
  /// it calls into a more specialized MaterializeValueBatch() for doing the actual
  /// value materialization using the level caches.
  /// Use RESTRICT so that the compiler knows that it is safe to cache member
  /// variables in registers or on the stack (otherwise gcc's alias analysis
  /// conservatively assumes that buffers like 'tuple_mem', 'num_values' or the
  /// 'def_levels_' 'rep_levels_' buffers may alias 'this', especially with
  /// -fno-strict-alias).
  template <bool IN_COLLECTION>
  bool ReadValueBatch(int max_values, int tuple_size, uint8_t* RESTRICT tuple_mem,
      int* RESTRICT num_values) RESTRICT;

  /// Helper function for ReadValueBatch() above that performs value materialization.
  /// It assumes a data page with remaining values is available, and that the def/rep
  /// level caches have been populated. Materializes values into 'tuple_mem' with a
  /// stride of 'tuple_size' and updates 'num_buffered_values_'. Returns the number of
  /// values materialized in 'num_values'.
  /// For efficiency, the simple special case of !MATERIALIZED && !IN_COLLECTION is not
  /// handled in this function.
  /// Use RESTRICT so that the compiler knows that it is safe to cache member
  /// variables in registers or on the stack (otherwise gcc's alias analysis
  /// conservatively assumes that buffers like 'tuple_mem', 'num_values' or the
  /// 'def_levels_' 'rep_levels_' buffers may alias 'this', especially with
  /// -fno-strict-alias).
  template <bool IN_COLLECTION, Encoding::type ENCODING, bool NEEDS_CONVERSION>
  bool MaterializeValueBatch(int max_values, int tuple_size, uint8_t* RESTRICT tuple_mem,
      int* RESTRICT num_values) RESTRICT;

  /// Same as above, but dispatches to the appropriate templated implementation of
  /// MaterializeValueBatch() based on 'page_encoding_' and NeedsConversionInline().
  template <bool IN_COLLECTION>
  bool MaterializeValueBatch(int max_values, int tuple_size, uint8_t* RESTRICT tuple_mem,
      int* RESTRICT num_values) RESTRICT;

  virtual Status CreateDictionaryDecoder(uint8_t* values, int size,
      DictDecoderBase** decoder) override {
    if (!dict_decoder_.template Reset<PARQUET_TYPE>(values, size, fixed_len_size_)) {
        return Status(TErrorCode::PARQUET_CORRUPT_DICTIONARY, filename(),
            slot_desc_->type().DebugString(), "could not decode dictionary");
    }
    dict_decoder_init_ = true;
    *decoder = &dict_decoder_;
    return Status::OK();
  }

  virtual bool HasDictionaryDecoder() override {
    return dict_decoder_init_;
  }

  virtual void ClearDictionaryDecoder() override {
    dict_decoder_init_ = false;
  }

  virtual Status InitDataPage(uint8_t* data, int size) override;

 private:
  /// Writes the next value into the appropriate destination slot in 'tuple'. Returns
  /// false if execution should be aborted for some reason, e.g. parse_error_ is set, the
  /// query is cancelled, or the scan node limit was reached. Otherwise returns true.
  ///
  /// Force inlining - GCC does not always inline this into hot loops.
  template <Encoding::type ENCODING, bool NEEDS_CONVERSION>
  inline ALWAYS_INLINE bool ReadSlot(Tuple* tuple);

  /// Reads position into 'pos' and updates 'pos_current_value_' based on 'rep_level'.
  /// This helper is only used on the batched decoding path where we need to reset
  /// 'pos_current_value_' to 0 based on 'rep_level'.
  inline ALWAYS_INLINE void ReadPositionBatched(int16_t rep_level, int64_t* pos);

  /// Decode one value from *data into 'val' and advance *data. 'data_end' is one byte
  /// past the end of the buffer. Return false and set 'parse_error_' if there is an
  /// error decoding the value.
  template <Encoding::type ENCODING>
  inline ALWAYS_INLINE bool DecodeValue(uint8_t** data, const uint8_t* data_end,
      InternalType* RESTRICT val) RESTRICT;

  /// Most column readers never require conversion, so we can avoid branches by
  /// returning constant false. Column readers for types that require conversion
  /// must specialize this function.
  inline bool NeedsConversionInline() const {
    DCHECK(!needs_conversion_);
    return false;
  }

  /// Similar to NeedsConversion(), most column readers do not require validation,
  /// so to avoid branches, we return constant false. In general, types where not
  /// all possible bit representations of the data type are valid should be
  /// validated.
  inline bool NeedsValidationInline() const {
    return false;
  }

  /// Converts and writes 'src' into 'slot' based on desc_->type()
  bool ConvertSlot(const InternalType* src, void* slot) {
    DCHECK(false);
    return false;
  }

  /// Checks if 'val' is invalid, e.g. due to being out of the valid value range. If it
  /// is invalid, logs the error and returns false. If the error should stop execution,
  /// sets 'parent_->parse_status_'.
  bool ValidateValue(InternalType* val) const {
    DCHECK(false);
    return false;
  }

  /// Pull out slow-path Status construction code
  void __attribute__((noinline)) SetDictDecodeError() {
    parent_->parse_status_ = Status(TErrorCode::PARQUET_DICT_DECODE_FAILURE, filename(),
        slot_desc_->type().DebugString(), stream_->file_offset());
  }

  void __attribute__((noinline)) SetPlainDecodeError() {
    parent_->parse_status_ = Status(TErrorCode::PARQUET_CORRUPT_PLAIN_VALUE, filename(),
        slot_desc_->type().DebugString(), stream_->file_offset());
  }

  /// Dictionary decoder for decoding column values.
  DictDecoder<InternalType> dict_decoder_;

  /// True if dict_decoder_ has been initialized with a dictionary page.
  bool dict_decoder_init_ = false;

  /// true if decoded values must be converted before being written to an output tuple.
  bool needs_conversion_ = false;

  /// The size of this column with plain encoding for FIXED_LEN_BYTE_ARRAY, or
  /// the max length for VARCHAR columns. Unused otherwise.
  int fixed_len_size_;
};

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::ScalarColumnReader(
    HdfsParquetScanner* parent, const SchemaNode& node, const SlotDescriptor* slot_desc)
  : BaseScalarColumnReader(parent, node, slot_desc),
    dict_decoder_(parent->scan_node_->mem_tracker()) {
  if (!MATERIALIZED) {
    // We're not materializing any values, just counting them. No need (or ability) to
    // initialize state used to materialize values.
    DCHECK(slot_desc_ == nullptr);
    return;
  }

  DCHECK(slot_desc_ != nullptr);
  DCHECK_NE(slot_desc_->type().type, TYPE_BOOLEAN);
  if (slot_desc_->type().type == TYPE_DECIMAL
      && PARQUET_TYPE == parquet::Type::FIXED_LEN_BYTE_ARRAY) {
    fixed_len_size_ = node.element->type_length;
  } else if (slot_desc_->type().type == TYPE_VARCHAR) {
    fixed_len_size_ = slot_desc_->type().len;
  } else {
    fixed_len_size_ = -1;
  }
  needs_conversion_ = slot_desc_->type().type == TYPE_CHAR ||
      // TODO: Add logic to detect file versions that have unconverted TIMESTAMP
      // values. Currently all versions have converted values.
      (FLAGS_convert_legacy_hive_parquet_utc_timestamps &&
      slot_desc_->type().type == TYPE_TIMESTAMP &&
      parent->file_version_.application == "parquet-mr");
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
Status ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::InitDataPage(
    uint8_t* data, int size) {
  // Data can be empty if the column contains all NULLs
  DCHECK_GE(size, 0);
  page_encoding_ = current_page_header_.data_page_header.encoding;
  if (page_encoding_ != parquet::Encoding::PLAIN_DICTIONARY &&
      page_encoding_ != parquet::Encoding::PLAIN) {
    return GetUnsupportedDecodingError();
  }

  // If slot_desc_ is NULL, we don't need so decode any values so dict_decoder_ does
  // not need to be initialized.
  if (page_encoding_ == Encoding::PLAIN_DICTIONARY && slot_desc_ != nullptr) {
    if (!dict_decoder_init_) {
      return Status("File corrupt. Missing dictionary page.");
    }
    RETURN_IF_ERROR(dict_decoder_.SetData(data, size));
  }
  // TODO: Perform filter selectivity checks here.
  return Status::OK();
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
template <bool IN_COLLECTION>
bool ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::ReadValue(
    Tuple* tuple) {
  // NextLevels() should have already been called and def and rep levels should be in
  // valid range.
  DCHECK_GE(rep_level_, 0);
  DCHECK_GE(def_level_, 0);
  DCHECK_GE(def_level_, def_level_of_immediate_repeated_ancestor()) <<
      "Caller should have called NextLevels() until we are ready to read a value";

  if (MATERIALIZED) {
    if (def_level_ >= max_def_level()) {
      bool continue_execution;
      if (page_encoding_ == Encoding::PLAIN_DICTIONARY) {
        continue_execution = NeedsConversionInline() ?
            ReadSlot<Encoding::PLAIN_DICTIONARY, true>(tuple) :
            ReadSlot<Encoding::PLAIN_DICTIONARY, false>(tuple);
      } else {
        DCHECK_EQ(page_encoding_, Encoding::PLAIN);
        continue_execution = NeedsConversionInline() ?
            ReadSlot<Encoding::PLAIN, true>(tuple) :
            ReadSlot<Encoding::PLAIN, false>(tuple);
      }
      if (!continue_execution) return false;
    } else {
      tuple->SetNull(null_indicator_offset_);
    }
  }
  return NextLevels<IN_COLLECTION>();
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
template <bool IN_COLLECTION>
bool ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::ReadValueBatch(
    int max_values, int tuple_size, uint8_t* RESTRICT tuple_mem,
    int* RESTRICT num_values) RESTRICT {
  // Repetition level is only present if this column is nested in a collection type.
  if (IN_COLLECTION) {
    DCHECK_GT(max_rep_level(), 0) << slot_desc()->DebugString();
  } else {
    DCHECK_EQ(max_rep_level(), 0) << slot_desc()->DebugString();
  }

  int val_count = 0;
  bool continue_execution = true;
  while (val_count < max_values && !RowGroupAtEnd() && continue_execution) {
    DCHECK_GE(num_buffered_values_, 0);
    // Read next page if necessary.
    if (num_buffered_values_ == 0) {
      if (!NextPage()) {
        continue_execution = parent_->parse_status_.ok();
        continue;
      }
    }

    // Not materializing anything - skip decoding any levels and rely on the value
    // count from page metadata to return the correct number of rows.
    if (!MATERIALIZED && !IN_COLLECTION) {
      int vals_to_add = min(num_buffered_values_, max_values - val_count);
      val_count += vals_to_add;
      num_buffered_values_ -= vals_to_add;
      continue;
    }
    // Fill the rep level cache if needed. We are flattening out the fields of the
    // nested collection into the top-level tuple returned by the scan, so we don't
    // care about the nesting structure unless the position slot is being populated.
    if (IN_COLLECTION && pos_slot_desc_ != nullptr && !rep_levels_.CacheHasNext()) {
      parent_->parse_status_.MergeStatus(
          rep_levels_.CacheNextBatch(num_buffered_values_));
      if (UNLIKELY(!parent_->parse_status_.ok())) return false;
    }

    // Fill def level cache if needed.
    if (!def_levels_.CacheHasNext()) {
      // TODO: add a fast path here if there's a run of repeated values.
      parent_->parse_status_.MergeStatus(
          def_levels_.CacheNextBatch(num_buffered_values_));
      if (UNLIKELY(!parent_->parse_status_.ok())) return false;
    }

    // Read data page and cached levels to materialize values.
    uint8_t* next_tuple = tuple_mem + val_count * tuple_size;
    int remaining_val_capacity = max_values - val_count;
    int ret_val_count = 0;
    continue_execution = MaterializeValueBatch<IN_COLLECTION>(
        remaining_val_capacity, tuple_size, next_tuple, &ret_val_count);
    val_count += ret_val_count;
    if (SHOULD_TRIGGER_DEBUG_ACTION(val_count)) {
      continue_execution &= ColReaderDebugAction(&val_count);
    }
  }
  *num_values = val_count;
  return continue_execution;
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
template <bool IN_COLLECTION, Encoding::type ENCODING, bool NEEDS_CONVERSION>
bool ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::MaterializeValueBatch(
    int max_values, int tuple_size, uint8_t* RESTRICT tuple_mem,
    int* RESTRICT num_values) RESTRICT {
  DCHECK(MATERIALIZED || IN_COLLECTION);
  DCHECK_GT(num_buffered_values_, 0);
  DCHECK(def_levels_.CacheHasNext());
  if (IN_COLLECTION && pos_slot_desc_ != nullptr) DCHECK(rep_levels_.CacheHasNext());
  const int cache_start_idx = def_levels_.CacheCurrIdx();
  uint8_t* curr_tuple = tuple_mem;
  int val_count = 0;
  while (def_levels_.CacheHasNext() && val_count < max_values) {
    Tuple* tuple = reinterpret_cast<Tuple*>(curr_tuple);
    int def_level = def_levels_.CacheGetNext();

    if (IN_COLLECTION) {
      if (def_level < def_level_of_immediate_repeated_ancestor()) {
        // A containing repeated field is empty or NULL. Skip the value but
        // move to the next repetition level if necessary.
        if (pos_slot_desc_ != nullptr) rep_levels_.CacheSkipLevels(1);
        continue;
      }
      if (pos_slot_desc_ != nullptr) {
        ReadPositionBatched(rep_levels_.CacheGetNext(),
            tuple->GetBigIntSlot(pos_slot_desc_->tuple_offset()));
      }
    }

    if (MATERIALIZED) {
      if (def_level >= max_def_level()) {
        bool continue_execution = ReadSlot<ENCODING, NEEDS_CONVERSION>(tuple);
        if (UNLIKELY(!continue_execution)) return false;
      } else {
        tuple->SetNull(null_indicator_offset_);
      }
    }
    curr_tuple += tuple_size;
    ++val_count;
  }
  num_buffered_values_ -= (def_levels_.CacheCurrIdx() - cache_start_idx);
  *num_values = val_count;
  return true;
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
template <bool IN_COLLECTION>
bool ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::MaterializeValueBatch(
    int max_values, int tuple_size, uint8_t* RESTRICT tuple_mem,
    int* RESTRICT num_values) RESTRICT {
  // Dispatch to the correct templated implementation of MaterializeValueBatch().
  if (page_encoding_ == Encoding::PLAIN_DICTIONARY) {
    if (NeedsConversionInline()) {
      return MaterializeValueBatch<IN_COLLECTION, Encoding::PLAIN_DICTIONARY, true>(
          max_values, tuple_size, tuple_mem, num_values);
    } else {
      return MaterializeValueBatch<IN_COLLECTION, Encoding::PLAIN_DICTIONARY, false>(
          max_values, tuple_size, tuple_mem, num_values);
    }
  } else {
    DCHECK_EQ(page_encoding_, Encoding::PLAIN);
    if (NeedsConversionInline()) {
      return MaterializeValueBatch<IN_COLLECTION, Encoding::PLAIN, true>(
          max_values, tuple_size, tuple_mem, num_values);
    } else {
      return MaterializeValueBatch<IN_COLLECTION, Encoding::PLAIN, false>(
          max_values, tuple_size, tuple_mem, num_values);
    }
  }
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
template <Encoding::type ENCODING, bool NEEDS_CONVERSION>
bool ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::ReadSlot(
    Tuple* RESTRICT tuple) RESTRICT {
  void* slot = tuple->GetSlot(tuple_offset_);
  // Use an uninitialized stack allocation for temporary value to avoid running
  // constructors doing work unnecessarily, e.g. if T == StringValue.
  alignas(InternalType) uint8_t val_buf[sizeof(InternalType)];
  InternalType* val_ptr =
      reinterpret_cast<InternalType*>(NEEDS_CONVERSION ? val_buf : slot);

  if (UNLIKELY(!DecodeValue<ENCODING>(&data_, data_end_, val_ptr))) return false;
  if (UNLIKELY(NeedsValidationInline() && !ValidateValue(val_ptr))) {
    if (UNLIKELY(!parent_->parse_status_.ok())) return false;
    // The value is invalid but execution should continue - set the null indicator and
    // skip conversion.
    tuple->SetNull(null_indicator_offset_);
    return true;
  }
  if (NEEDS_CONVERSION && UNLIKELY(!ConvertSlot(val_ptr, slot))) return false;
  return true;
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
template <Encoding::type ENCODING>
bool ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::DecodeValue(
    uint8_t** RESTRICT data, const uint8_t* RESTRICT data_end,
    InternalType* RESTRICT val) RESTRICT {
  DCHECK_EQ(page_encoding_, ENCODING);
  if (ENCODING == Encoding::PLAIN_DICTIONARY) {
    if (UNLIKELY(!dict_decoder_.GetNextValue(val))) {
      SetDictDecodeError();
      return false;
    }
  } else {
    DCHECK_EQ(ENCODING, Encoding::PLAIN);
    int encoded_len = ParquetPlainEncoder::Decode<InternalType, PARQUET_TYPE>(
        *data, data_end, fixed_len_size_, val);
    if (UNLIKELY(encoded_len < 0)) {
      SetPlainDecodeError();
      return false;
    }
    *data += encoded_len;
  }
  return true;
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
void ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>
    ::ReadPositionBatched(int16_t rep_level, int64_t* pos) {
  // Reset position counter if we are at the start of a new parent collection.
  if (rep_level <= max_rep_level() - 1) pos_current_value_ = 0;
  *pos = pos_current_value_++;
}

template <>
inline bool ScalarColumnReader<StringValue, parquet::Type::BYTE_ARRAY, true>
::NeedsConversionInline() const {
  return needs_conversion_;
}

template <>
bool ScalarColumnReader<StringValue, parquet::Type::BYTE_ARRAY, true>::ConvertSlot(
    const StringValue* src, void* slot) {
  DCHECK(slot_desc() != nullptr);
  DCHECK(slot_desc()->type().type == TYPE_CHAR);
  int char_len = slot_desc()->type().len;
  int unpadded_len = min(char_len, src->len);
  char* dst_char = reinterpret_cast<char*>(slot);
  memcpy(dst_char, src->ptr, unpadded_len);
  StringValue::PadWithSpaces(dst_char, char_len, unpadded_len);
  return true;
}

template <>
inline bool ScalarColumnReader<TimestampValue, parquet::Type::INT96, true>
::NeedsConversionInline() const {
  return needs_conversion_;
}

template <>
bool ScalarColumnReader<TimestampValue, parquet::Type::INT96, true>::ConvertSlot(
    const TimestampValue* src, void* slot) {
  // Conversion should only happen when this flag is enabled.
  DCHECK(FLAGS_convert_legacy_hive_parquet_utc_timestamps);
  TimestampValue* dst_ts = reinterpret_cast<TimestampValue*>(slot);
  *dst_ts = *src;
  if (dst_ts->HasDateAndTime()) dst_ts->UtcToLocal();
  return true;
}

template <>
inline bool ScalarColumnReader<TimestampValue, parquet::Type::INT96, true>
::NeedsValidationInline() const {
  return true;
}

template <>
bool ScalarColumnReader<TimestampValue, parquet::Type::INT96, true>::ValidateValue(
    TimestampValue* val) const {
  if (UNLIKELY(!TimestampValue::IsValidDate(val->date()))) {
    ErrorMsg msg(TErrorCode::PARQUET_TIMESTAMP_OUT_OF_RANGE,
        filename(), node_.element->name);
    Status status = parent_->state_->LogOrReturnError(msg);
    if (!status.ok()) parent_->parse_status_ = status;
    return false;
  }
  return true;
}

class BoolColumnReader : public BaseScalarColumnReader {
 public:
  BoolColumnReader(HdfsParquetScanner* parent, const SchemaNode& node,
      const SlotDescriptor* slot_desc)
    : BaseScalarColumnReader(parent, node, slot_desc) {
    if (slot_desc_ != nullptr) DCHECK_EQ(slot_desc_->type().type, TYPE_BOOLEAN);
  }

  virtual ~BoolColumnReader() { }

  virtual bool ReadValue(MemPool* pool, Tuple* tuple) override {
    return ReadValue<true>(tuple);
  }

  virtual bool ReadNonRepeatedValue(MemPool* pool, Tuple* tuple) override {
    return ReadValue<false>(tuple);
  }

 protected:
  virtual Status CreateDictionaryDecoder(uint8_t* values, int size,
      DictDecoderBase** decoder) override {
    DCHECK(false) << "Dictionary encoding is not supported for bools. Should never "
                  << "have gotten this far.";
    return Status::OK();
  }

  virtual bool HasDictionaryDecoder() override {
    // Decoder should never be created for bools.
    return false;
  }

  virtual void ClearDictionaryDecoder() override { }

  virtual Status InitDataPage(uint8_t* data, int size) override;

 private:
  template <bool IN_COLLECTION>
  inline bool ReadValue(Tuple* tuple);

  /// Decodes the next value into 'value'. Returns false and sets
  /// 'parent_->parse_status_' if an error is encountered decoding the
  /// value. Otherwise returns true.
  template <bool IN_COLLECTION>
  inline bool DecodeValue(bool* value);

  /// A buffer to store unpacked values. Must be a multiple of 32 size to use the
  /// batch-oriented interface of BatchedBitReader.
  static const int UNPACKED_BUFFER_LEN = 128;
  bool unpacked_values_[UNPACKED_BUFFER_LEN];

  /// The number of valid values in 'unpacked_values_'.
  int num_unpacked_values_ = 0;

  /// The next value to return from 'unpacked_values_'.
  int unpacked_value_idx_ = 0;

  /// Bit packed decoder, used if 'encoding_' is PLAIN.
  BatchedBitReader bool_values_;

  /// RLE decoder, used if 'encoding_' is RLE.
  RleBatchDecoder<bool> rle_decoder_;
};

Status BoolColumnReader::InitDataPage(uint8_t* data, int size) {
  page_encoding_ = current_page_header_.data_page_header.encoding;
  // Only the relevant decoder is initialized for a given data page.
  switch (page_encoding_) {
    case parquet::Encoding::PLAIN:
      bool_values_.Reset(data, size);
      break;
    case parquet::Encoding::RLE:
      // The first 4 bytes contain the size of the encoded data. This information is
      // redundant, as this is the last part of the data page, and the number of
      // remaining bytes is already known.
      rle_decoder_.Reset(data + 4, size - 4, 1);
      break;
    default:
      return GetUnsupportedDecodingError();
  }
  num_unpacked_values_ = 0;
  unpacked_value_idx_ = 0;
  return Status::OK();
}

template <bool IN_COLLECTION>
bool BoolColumnReader::ReadValue(Tuple* tuple) {
  DCHECK(slot_desc_ != nullptr);
  // Def and rep levels should be in valid range.
  DCHECK_GE(rep_level_, 0);
  DCHECK_GE(def_level_, 0);
  DCHECK_GE(def_level_, def_level_of_immediate_repeated_ancestor()) <<
      "Caller should have called NextLevels() until we are ready to read a value";

  if (def_level_ >= max_def_level()) {
    bool* slot = tuple->GetBoolSlot(tuple_offset_);
    if (UNLIKELY(!DecodeValue<IN_COLLECTION>(slot))) return false;
  } else {
    // Null value
    tuple->SetNull(null_indicator_offset_);
  }
  return NextLevels<IN_COLLECTION>();
}

template <bool IN_COLLECTION>
bool BoolColumnReader::DecodeValue(bool* value) {
  if (LIKELY(unpacked_value_idx_ < num_unpacked_values_)) {
    *value = unpacked_values_[unpacked_value_idx_++];
  } else {
    // Unpack as many values as we can into the buffer. We expect to read at least one
    // value.
    if (page_encoding_ == parquet::Encoding::PLAIN) {
      num_unpacked_values_ =
          bool_values_.UnpackBatch(1, UNPACKED_BUFFER_LEN, &unpacked_values_[0]);
    } else {
      num_unpacked_values_ =
          rle_decoder_.GetValues(UNPACKED_BUFFER_LEN, &unpacked_values_[0]);
    }

    if (UNLIKELY(num_unpacked_values_ == 0)) {
      parent_->parse_status_ = Status("Invalid bool column.");
      return false;
    }
    *value = unpacked_values_[0];
    unpacked_value_idx_ = 1;
  }
  return true;
}

// Change 'val_count' to zero to exercise IMPALA-5197. This verifies the error handling
// path doesn't falsely report that the file is corrupted.
bool ParquetColumnReader::ColReaderDebugAction(int* val_count) {
#ifndef NDEBUG
  Status status = parent_->ScannerDebugAction();
  if (!status.ok()) {
    if (!status.IsCancelled()) parent_->parse_status_.MergeStatus(status);
    *val_count = 0;
    return false;
  }
#endif
  return true;
}

bool ParquetColumnReader::ReadValueBatch(MemPool* pool, int max_values,
    int tuple_size, uint8_t* tuple_mem, int* num_values) {
  // The below loop requires that NextLevels() was called previously to populate
  // 'def_level_' and 'rep_level_'. Ensure it is called at the start of each
  // row group.
  if (def_level_ == HdfsParquetScanner::INVALID_LEVEL && !NextLevels()) return false;

  int val_count = 0;
  bool continue_execution = true;
  while (val_count < max_values && !RowGroupAtEnd() && continue_execution) {
    Tuple* tuple = reinterpret_cast<Tuple*>(tuple_mem + val_count * tuple_size);
    if (def_level_ < def_level_of_immediate_repeated_ancestor()) {
      // A containing repeated field is empty or NULL
      continue_execution = NextLevels();
      continue;
    }
    // Fill in position slot if applicable
    if (pos_slot_desc_ != nullptr) {
      ReadPositionNonBatched(tuple->GetBigIntSlot(pos_slot_desc()->tuple_offset()));
    }
    continue_execution = ReadValue(pool, tuple);
    ++val_count;
    if (SHOULD_TRIGGER_DEBUG_ACTION(val_count)) {
      continue_execution &= ColReaderDebugAction(&val_count);
    }
  }
  *num_values = val_count;
  return continue_execution;
}

bool ParquetColumnReader::ReadNonRepeatedValueBatch(MemPool* pool,
    int max_values, int tuple_size, uint8_t* tuple_mem, int* num_values) {
  // The below loop requires that NextLevels() was called previously to populate
  // 'def_level_' and 'rep_level_'. Ensure it is called at the start of each
  // row group.
  if (def_level_ == HdfsParquetScanner::INVALID_LEVEL && !NextLevels()) return false;

  int val_count = 0;
  bool continue_execution = true;
  while (val_count < max_values && !RowGroupAtEnd() && continue_execution) {
    Tuple* tuple = reinterpret_cast<Tuple*>(tuple_mem + val_count * tuple_size);
    continue_execution = ReadNonRepeatedValue(pool, tuple);
    ++val_count;
    if (SHOULD_TRIGGER_DEBUG_ACTION(val_count)) {
      continue_execution &= ColReaderDebugAction(&val_count);
    }
  }
  *num_values = val_count;
  return continue_execution;
}

void ParquetColumnReader::ReadPositionNonBatched(int64_t* pos) {
  // NextLevels() should have already been called
  DCHECK_GE(rep_level_, 0);
  DCHECK_GE(def_level_, 0);
  DCHECK_GE(pos_current_value_, 0);
  DCHECK_GE(def_level_, def_level_of_immediate_repeated_ancestor()) <<
      "Caller should have called NextLevels() until we are ready to read a value";
  *pos = pos_current_value_++;
}

// In 1.1, we had a bug where the dictionary page metadata was not set. Returns true
// if this matches those versions and compatibility workarounds need to be used.
static bool RequiresSkippedDictionaryHeaderCheck(
    const ParquetFileVersion& v) {
  if (v.application != "impala") return false;
  return v.VersionEq(1,1,0) || (v.VersionEq(1,2,0) && v.is_impala_internal);
}

Status BaseScalarColumnReader::Reset(const HdfsFileDesc& file_desc,
    const parquet::ColumnChunk& col_chunk, int row_group_idx) {
  // Ensure metadata is valid before using it to initialize the reader.
  RETURN_IF_ERROR(ParquetMetadataUtils::ValidateRowGroupColumn(parent_->file_metadata_,
      parent_->filename(), row_group_idx, col_idx(), schema_element(),
      parent_->state_));
  num_buffered_values_ = 0;
  data_ = nullptr;
  data_end_ = nullptr;
  stream_ = nullptr;
  io_reservation_ = 0;
  metadata_ = &col_chunk.meta_data;
  num_values_read_ = 0;
  def_level_ = HdfsParquetScanner::INVALID_LEVEL;
  // See ColumnReader constructor.
  rep_level_ = max_rep_level() == 0 ? 0 : HdfsParquetScanner::INVALID_LEVEL;
  pos_current_value_ = HdfsParquetScanner::INVALID_POS;

  if (metadata_->codec != parquet::CompressionCodec::UNCOMPRESSED) {
    RETURN_IF_ERROR(Codec::CreateDecompressor(
        nullptr, false, ConvertParquetToImpalaCodec(metadata_->codec), &decompressor_));
  }
  int64_t col_start = col_chunk.meta_data.data_page_offset;
  if (col_chunk.meta_data.__isset.dictionary_page_offset) {
    // Already validated in ValidateColumnOffsets()
    DCHECK_LT(col_chunk.meta_data.dictionary_page_offset, col_start);
    col_start = col_chunk.meta_data.dictionary_page_offset;
  }
  int64_t col_len = col_chunk.meta_data.total_compressed_size;
  if (col_len <= 0) {
    return Status(Substitute("File '$0' contains invalid column chunk size: $1",
        filename(), col_len));
  }
  int64_t col_end = col_start + col_len;

  // Already validated in ValidateColumnOffsets()
  DCHECK_GT(col_end, 0);
  DCHECK_LT(col_end, file_desc.file_length);
  const ParquetFileVersion& file_version = parent_->file_version_;
  if (file_version.application == "parquet-mr" && file_version.VersionLt(1, 2, 9)) {
    // The Parquet MR writer had a bug in 1.2.8 and below where it didn't include the
    // dictionary page header size in total_compressed_size and total_uncompressed_size
    // (see IMPALA-694). We pad col_len to compensate.
    int64_t bytes_remaining = file_desc.file_length - col_end;
    int64_t pad = min<int64_t>(MAX_DICT_HEADER_SIZE, bytes_remaining);
    col_len += pad;
  }

  // TODO: this will need to change when we have co-located files and the columns
  // are different files.
  if (!col_chunk.file_path.empty() && col_chunk.file_path != filename()) {
    return Status(Substitute("Expected parquet column file path '$0' to match "
        "filename '$1'", col_chunk.file_path, filename()));
  }

  const ScanRange* metadata_range = parent_->metadata_range_;
  int64_t partition_id = parent_->context_->partition_descriptor()->id();
  const ScanRange* split_range =
      static_cast<ScanRangeMetadata*>(metadata_range->meta_data())->original_split;
  // Determine if the column is completely contained within a local split.
  bool col_range_local = split_range->expected_local()
      && col_start >= split_range->offset()
      && col_end <= split_range->offset() + split_range->len();
  scan_range_ = parent_->scan_node_->AllocateScanRange(metadata_range->fs(),
      filename(), col_len, col_start, partition_id, split_range->disk_id(),
      col_range_local,
      BufferOpts(split_range->try_cache(), file_desc.mtime));
  ClearDictionaryDecoder();
  return Status::OK();
}

Status BaseScalarColumnReader::StartScan() {
  DCHECK(scan_range_ != nullptr) << "Must Reset() before starting scan.";
  DiskIoMgr* io_mgr = ExecEnv::GetInstance()->disk_io_mgr();
  ScannerContext* context = parent_->context_;
  DCHECK_GT(io_reservation_, 0);
  bool needs_buffers;
  RETURN_IF_ERROR(io_mgr->StartScanRange(
      parent_->scan_node_->reader_context(), scan_range_, &needs_buffers));
  if (needs_buffers) {
    RETURN_IF_ERROR(io_mgr->AllocateBuffersForRange(
        parent_->scan_node_->reader_context(), context->bp_client(),
        scan_range_, io_reservation_));
  }
  stream_ = parent_->context_->AddStream(scan_range_, io_reservation_);
  DCHECK(stream_ != nullptr);
  return Status::OK();
}

Status BaseScalarColumnReader::ReadPageHeader(bool peek,
    parquet::PageHeader* next_page_header, uint32_t* next_header_size, bool* eos) {
  DCHECK(stream_ != nullptr);
  *eos = false;

  uint8_t* buffer;
  int64_t buffer_size;
  RETURN_IF_ERROR(stream_->GetBuffer(true, &buffer, &buffer_size));
  // check for end of stream
  if (buffer_size == 0) {
    // The data pages contain fewer values than stated in the column metadata.
    DCHECK(stream_->eosr());
    DCHECK_LT(num_values_read_, metadata_->num_values);
    // TODO for 2.3: node_.element->name isn't necessarily useful
    ErrorMsg msg(TErrorCode::PARQUET_COLUMN_METADATA_INVALID, metadata_->num_values,
        num_values_read_, node_.element->name, filename());
    RETURN_IF_ERROR(parent_->state_->LogOrReturnError(msg));
    *eos = true;
    return Status::OK();
  }

  // We don't know the actual header size until the thrift object is deserialized.  Loop
  // until we successfully deserialize the header or exceed the maximum header size.
  uint32_t header_size;
  Status status;
  while (true) {
    header_size = buffer_size;
    status = DeserializeThriftMsg(buffer, &header_size, true, next_page_header);
    if (status.ok()) break;

    if (buffer_size >= FLAGS_max_page_header_size) {
      stringstream ss;
      ss << "ParquetScanner: could not read data page because page header exceeded "
         << "maximum size of "
         << PrettyPrinter::Print(FLAGS_max_page_header_size, TUnit::BYTES);
      status.AddDetail(ss.str());
      return status;
    }

    // Didn't read entire header, increase buffer size and try again
    int64_t new_buffer_size = max<int64_t>(buffer_size * 2, 1024);
    status = Status::OK();
    bool success = stream_->GetBytes(
        new_buffer_size, &buffer, &new_buffer_size, &status, /* peek */ true);
    if (!success) {
      DCHECK(!status.ok());
      return status;
    }
    DCHECK(status.ok());

    // Even though we increased the allowed buffer size, the number of bytes
    // read did not change. The header is not limited by the buffer space,
    // so it must be incomplete in the file.
    if (buffer_size == new_buffer_size) {
      DCHECK_NE(new_buffer_size, 0);
      return Status(TErrorCode::PARQUET_HEADER_EOF, filename());
    }
    DCHECK_GT(new_buffer_size, buffer_size);
    buffer_size = new_buffer_size;
  }

  *next_header_size = header_size;

  // Successfully deserialized current_page_header_
  if (!peek && !stream_->SkipBytes(header_size, &status)) return status;

  int data_size = next_page_header->compressed_page_size;
  if (UNLIKELY(data_size < 0)) {
    return Status(Substitute("Corrupt Parquet file '$0': negative page size $1 for "
        "column '$2'", filename(), data_size, schema_element().name));
  }
  int uncompressed_size = next_page_header->uncompressed_page_size;
  if (UNLIKELY(uncompressed_size < 0)) {
    return Status(Substitute("Corrupt Parquet file '$0': negative uncompressed page "
        "size $1 for column '$2'", filename(), uncompressed_size,
        schema_element().name));
  }

  return Status::OK();
}

Status BaseScalarColumnReader::InitDictionary() {
  // Peek at the next page header
  bool eos;
  parquet::PageHeader next_page_header;
  uint32_t next_header_size;
  DCHECK(stream_ != nullptr);
  DCHECK(!HasDictionaryDecoder());

  RETURN_IF_ERROR(ReadPageHeader(true /* peek */, &next_page_header,
                                 &next_header_size, &eos));
  if (eos) return Status::OK();
  // The dictionary must be the first data page, so if the first page
  // is not a dictionary, then there is no dictionary.
  if (next_page_header.type != parquet::PageType::DICTIONARY_PAGE) return Status::OK();

  current_page_header_ = next_page_header;
  Status status;
  if (!stream_->SkipBytes(next_header_size, &status)) return status;

  int data_size = current_page_header_.compressed_page_size;
  if (slot_desc_ == nullptr) {
    // Skip processing the dictionary page if we don't need to decode any values. In
    // addition to being unnecessary, we are likely unable to successfully decode the
    // dictionary values because we don't necessarily create the right type of scalar
    // reader if there's no slot to read into (see CreateReader()).
    if (!stream_->SkipBytes(data_size, &status)) return status;
    return Status::OK();
  }

  if (node_.element->type == parquet::Type::BOOLEAN) {
    return Status("Unexpected dictionary page. Dictionary page is not"
       " supported for booleans.");
  }

  const parquet::DictionaryPageHeader* dict_header = nullptr;
  if (current_page_header_.__isset.dictionary_page_header) {
    dict_header = &current_page_header_.dictionary_page_header;
  } else {
    if (!RequiresSkippedDictionaryHeaderCheck(parent_->file_version_)) {
      return Status("Dictionary page does not have dictionary header set.");
    }
  }
  if (dict_header != nullptr &&
      dict_header->encoding != Encoding::PLAIN &&
      dict_header->encoding != Encoding::PLAIN_DICTIONARY) {
    return Status("Only PLAIN and PLAIN_DICTIONARY encodings are supported "
                  "for dictionary pages.");
  }

  if (!stream_->ReadBytes(data_size, &data_, &status)) return status;
  data_end_ = data_ + data_size;

  // The size of dictionary can be 0, if every value is null. The dictionary still has to
  // be reset in this case.
  DictDecoderBase* dict_decoder;
  if (current_page_header_.uncompressed_page_size == 0) {
    return CreateDictionaryDecoder(nullptr, 0, &dict_decoder);
  }

  // There are 3 different cases from the aspect of memory management:
  // 1. If the column type is string, the dictionary will contain pointers to a buffer,
  //    so the buffer's lifetime must be as long as any row batch that references it.
  // 2. If the column type is not string, and the dictionary page is compressed, then a
  //    temporary buffer is needed for the uncompressed values.
  // 3. If the column type is not string, and the dictionary page is not compressed,
  //    then no buffer is necessary.
  ScopedBuffer uncompressed_buffer(parent_->dictionary_pool_->mem_tracker());
  uint8_t* dict_values = nullptr;
  if (decompressor_.get() != nullptr || slot_desc_->type().IsStringType()) {
    int buffer_size = current_page_header_.uncompressed_page_size;
    if (slot_desc_->type().IsStringType()) {
      dict_values = parent_->dictionary_pool_->TryAllocate(buffer_size); // case 1.
    } else if (uncompressed_buffer.TryAllocate(buffer_size)) {
      dict_values = uncompressed_buffer.buffer(); // case 2
    }
    if (UNLIKELY(dict_values == nullptr)) {
      string details = Substitute(PARQUET_COL_MEM_LIMIT_EXCEEDED, "InitDictionary",
          buffer_size, "dictionary");
      return parent_->dictionary_pool_->mem_tracker()->MemLimitExceeded(
               parent_->state_, details, buffer_size);
    }
  } else {
    dict_values = data_; // case 3.
  }

  if (decompressor_.get() != nullptr) {
    int uncompressed_size = current_page_header_.uncompressed_page_size;
    RETURN_IF_ERROR(decompressor_->ProcessBlock32(true, data_size, data_,
                    &uncompressed_size, &dict_values));
    VLOG_FILE << "Decompressed " << data_size << " to " << uncompressed_size;
    if (current_page_header_.uncompressed_page_size != uncompressed_size) {
      return Status(Substitute("Error decompressing dictionary page in file '$0'. "
               "Expected $1 uncompressed bytes but got $2", filename(),
               current_page_header_.uncompressed_page_size, uncompressed_size));
    }
  } else {
    if (current_page_header_.uncompressed_page_size != data_size) {
      return Status(Substitute("Error reading dictionary page in file '$0'. "
                               "Expected $1 bytes but got $2", filename(),
                               current_page_header_.uncompressed_page_size, data_size));
    }
    if (slot_desc_->type().IsStringType()) memcpy(dict_values, data_, data_size);
  }

  RETURN_IF_ERROR(CreateDictionaryDecoder(
      dict_values, current_page_header_.uncompressed_page_size, &dict_decoder));
  if (dict_header != nullptr &&
      dict_header->num_values != dict_decoder->num_entries()) {
    return Status(TErrorCode::PARQUET_CORRUPT_DICTIONARY, filename(),
                  slot_desc_->type().DebugString(),
                  Substitute("Expected $0 entries but data contained $1 entries",
                             dict_header->num_values, dict_decoder->num_entries()));
  }

  return Status::OK();
}

Status BaseScalarColumnReader::InitDictionaries(
    const vector<BaseScalarColumnReader*> readers) {
  for (BaseScalarColumnReader* reader : readers) {
    RETURN_IF_ERROR(reader->InitDictionary());
  }
  return Status::OK();
}

Status BaseScalarColumnReader::ReadDataPage() {
  // We're about to move to the next data page.  The previous data page is
  // now complete, free up any memory allocated for it. If the data page contained
  // strings we need to attach it to the returned batch.
  if (PageContainsTupleData(page_encoding_)) {
    parent_->scratch_batch_->aux_mem_pool.AcquireData(data_page_pool_.get(), false);
  } else {
    data_page_pool_->FreeAll();
  }
  // We don't hold any pointers to earlier pages in the stream - we can safely free
  // any I/O or boundary buffer.
  stream_->ReleaseCompletedResources(false);

  // Read the next data page, skipping page types we don't care about.
  // We break out of this loop on the non-error case (a data page was found or we read all
  // the pages).
  while (true) {
    DCHECK_EQ(num_buffered_values_, 0);
    if (num_values_read_ == metadata_->num_values) {
      // No more pages to read
      // TODO: should we check for stream_->eosr()?
      break;
    } else if (num_values_read_ > metadata_->num_values) {
      ErrorMsg msg(TErrorCode::PARQUET_COLUMN_METADATA_INVALID,
          metadata_->num_values, num_values_read_, node_.element->name, filename());
      RETURN_IF_ERROR(parent_->state_->LogOrReturnError(msg));
      return Status::OK();
    }

    bool eos;
    uint32_t header_size;
    RETURN_IF_ERROR(ReadPageHeader(false /* peek */, &current_page_header_,
                                   &header_size, &eos));
    if (eos) return Status::OK();

    if (current_page_header_.type == parquet::PageType::DICTIONARY_PAGE) {
      // Any dictionary is already initialized, as InitDictionary has already
      // been called. There are two possibilities:
      // 1. The parquet file has two dictionary pages
      // OR
      // 2. The parquet file does not have the dictionary as the first data page.
      // Both are errors in the parquet file.
      if (HasDictionaryDecoder()) {
        return Status(Substitute("Corrupt Parquet file '$0': multiple dictionary pages "
            "for column '$1'", filename(), schema_element().name));
      } else {
        return Status(Substitute("Corrupt Parquet file: '$0': dictionary page for "
            "column '$1' is not the first page", filename(), schema_element().name));
      }
    }

    Status status;
    int data_size = current_page_header_.compressed_page_size;
    if (current_page_header_.type != parquet::PageType::DATA_PAGE) {
      // We can safely skip non-data pages
      if (!stream_->SkipBytes(data_size, &status)) return status;
      continue;
    }

    // Read Data Page
    // TODO: when we start using page statistics, we will need to ignore certain corrupt
    // statistics. See IMPALA-2208 and PARQUET-251.
    if (!stream_->ReadBytes(data_size, &data_, &status)) return status;
    data_end_ = data_ + data_size;
    int num_values = current_page_header_.data_page_header.num_values;
    if (num_values < 0) {
      return Status(Substitute("Error reading data page in Parquet file '$0'. "
          "Invalid number of values in metadata: $1", filename(), num_values));
    }
    num_buffered_values_ = num_values;
    num_values_read_ += num_buffered_values_;

    int uncompressed_size = current_page_header_.uncompressed_page_size;
    if (decompressor_.get() != nullptr) {
      SCOPED_TIMER(parent_->decompress_timer_);
      uint8_t* decompressed_buffer;
      RETURN_IF_ERROR(AllocateUncompressedDataPage(
            uncompressed_size, "decompressed data", &decompressed_buffer));
      RETURN_IF_ERROR(decompressor_->ProcessBlock32(true,
          current_page_header_.compressed_page_size, data_, &uncompressed_size,
          &decompressed_buffer));
      VLOG_FILE << "Decompressed " << current_page_header_.compressed_page_size
                << " to " << uncompressed_size;
      if (current_page_header_.uncompressed_page_size != uncompressed_size) {
        return Status(Substitute("Error decompressing data page in file '$0'. "
            "Expected $1 uncompressed bytes but got $2", filename(),
            current_page_header_.uncompressed_page_size, uncompressed_size));
      }
      data_ = decompressed_buffer;
      data_size = current_page_header_.uncompressed_page_size;
      data_end_ = data_ + data_size;
    } else {
      DCHECK_EQ(metadata_->codec, parquet::CompressionCodec::UNCOMPRESSED);
      if (current_page_header_.compressed_page_size != uncompressed_size) {
        return Status(Substitute("Error reading data page in file '$0'. "
            "Expected $1 bytes but got $2", filename(),
            current_page_header_.compressed_page_size, uncompressed_size));
      }
      if (PageContainsTupleData(current_page_header_.data_page_header.encoding)) {
        // In this case returned batches will have pointers into the data page itself.
        // We don't transfer disk I/O buffers out of the scanner so we need to copy
        // the page data so that it can be attached to output batches.
        uint8_t* copy_buffer;
        RETURN_IF_ERROR(AllocateUncompressedDataPage(
              uncompressed_size, "uncompressed variable-length data", &copy_buffer));
        memcpy(copy_buffer, data_, uncompressed_size);
        data_ = copy_buffer;
        data_end_ = data_ + uncompressed_size;
      }
    }

    // Initialize the repetition level data
    RETURN_IF_ERROR(rep_levels_.Init(filename(),
        current_page_header_.data_page_header.repetition_level_encoding,
        parent_->perm_pool_.get(), parent_->state_->batch_size(),
        max_rep_level(), num_buffered_values_,
        &data_, &data_size));

    // Initialize the definition level data
    RETURN_IF_ERROR(def_levels_.Init(filename(),
        current_page_header_.data_page_header.definition_level_encoding,
        parent_->perm_pool_.get(), parent_->state_->batch_size(),
        max_def_level(), num_buffered_values_, &data_, &data_size));

    // Data can be empty if the column contains all NULLs
    RETURN_IF_ERROR(InitDataPage(data_, data_size));
    break;
  }

  return Status::OK();
}

Status BaseScalarColumnReader::AllocateUncompressedDataPage(int64_t size,
    const char* err_ctx, uint8_t** buffer) {
  *buffer = data_page_pool_->TryAllocate(size);
  if (*buffer == nullptr) {
    string details =
        Substitute(PARQUET_COL_MEM_LIMIT_EXCEEDED, "ReadDataPage", size, err_ctx);
    return data_page_pool_->mem_tracker()->MemLimitExceeded(
        parent_->state_, details, size);
  }
  return Status::OK();
}

template <bool ADVANCE_REP_LEVEL>
bool BaseScalarColumnReader::NextLevels() {
  if (!ADVANCE_REP_LEVEL) DCHECK_EQ(max_rep_level(), 0) << slot_desc()->DebugString();

  if (UNLIKELY(num_buffered_values_ == 0)) {
    if (!NextPage()) return parent_->parse_status_.ok();
  }
  --num_buffered_values_;

  // Definition level is not present if column and any containing structs are required.
  def_level_ = max_def_level() == 0 ? 0 : def_levels_.ReadLevel();
  // The compiler can optimize these two conditions into a single branch by treating
  // def_level_ as unsigned.
  if (UNLIKELY(def_level_ < 0 || def_level_ > max_def_level())) {
    SetLevelDecodeError("def", def_level_, max_def_level());
    return false;
  }

  if (ADVANCE_REP_LEVEL && max_rep_level() > 0) {
    // Repetition level is only present if this column is nested in any collection type.
    rep_level_ = rep_levels_.ReadLevel();
    if (UNLIKELY(rep_level_ < 0 || rep_level_ > max_rep_level())) {
      SetLevelDecodeError("rep", rep_level_, max_rep_level());
      return false;
    }
    // Reset position counter if we are at the start of a new parent collection.
    if (rep_level_ <= max_rep_level() - 1) pos_current_value_ = 0;
  }

  return parent_->parse_status_.ok();
}

Status BaseScalarColumnReader::GetUnsupportedDecodingError() {
  return Status(Substitute(
      "File '$0' is corrupt: unexpected encoding: $1 for data page of column '$2'.",
      filename(), PrintThriftEnum(page_encoding_), schema_element().name));
}

bool BaseScalarColumnReader::NextPage() {
  parent_->assemble_rows_timer_.Stop();
  parent_->parse_status_ = ReadDataPage();
  if (UNLIKELY(!parent_->parse_status_.ok())) return false;
  if (num_buffered_values_ == 0) {
    rep_level_ = HdfsParquetScanner::ROW_GROUP_END;
    def_level_ = HdfsParquetScanner::ROW_GROUP_END;
    pos_current_value_ = HdfsParquetScanner::INVALID_POS;
    return false;
  }
  parent_->assemble_rows_timer_.Start();
  return true;
}

void BaseScalarColumnReader::SetLevelDecodeError(const char* level_name,
    int decoded_level, int max_level) {
  if (decoded_level < 0) {
    DCHECK_EQ(decoded_level, HdfsParquetScanner::INVALID_LEVEL);
    parent_->parse_status_.MergeStatus(Status(Substitute("Corrupt Parquet file '$0': "
        "could not read all $1 levels for column '$2'", filename(),
        level_name, schema_element().name)));
  } else {
    parent_->parse_status_.MergeStatus(Status(Substitute("Corrupt Parquet file '$0': "
        "invalid $1 level $2 > max $1 level $3 for column '$4'", filename(),
        level_name, decoded_level, max_level, schema_element().name)));
  }
}

bool CollectionColumnReader::NextLevels() {
  DCHECK(!children_.empty());
  DCHECK_LE(rep_level_, new_collection_rep_level());
  for (int c = 0; c < children_.size(); ++c) {
    do {
      // TODO(skye): verify somewhere that all column readers are at end
      if (!children_[c]->NextLevels()) return false;
    } while (children_[c]->rep_level() > new_collection_rep_level());
  }
  UpdateDerivedState();
  return true;
}

bool CollectionColumnReader::ReadValue(MemPool* pool, Tuple* tuple) {
  DCHECK_GE(rep_level_, 0);
  DCHECK_GE(def_level_, 0);
  DCHECK_GE(def_level_, def_level_of_immediate_repeated_ancestor()) <<
      "Caller should have called NextLevels() until we are ready to read a value";

  if (tuple_offset_ == -1) {
    return CollectionColumnReader::NextLevels();
  } else if (def_level_ >= max_def_level()) {
    return ReadSlot(tuple->GetCollectionSlot(tuple_offset_), pool);
  } else {
    // Null value
    tuple->SetNull(null_indicator_offset_);
    return CollectionColumnReader::NextLevels();
  }
}

bool CollectionColumnReader::ReadNonRepeatedValue(MemPool* pool, Tuple* tuple) {
  return CollectionColumnReader::ReadValue(pool, tuple);
}

bool CollectionColumnReader::ReadSlot(CollectionValue* slot, MemPool* pool) {
  DCHECK(!children_.empty());
  DCHECK_LE(rep_level_, new_collection_rep_level());

  // Recursively read the collection into a new CollectionValue.
  *slot = CollectionValue();
  CollectionValueBuilder builder(
      slot, *slot_desc_->collection_item_descriptor(), pool, parent_->state_);
  bool continue_execution = parent_->AssembleCollection(
      children_, new_collection_rep_level(), &builder);
  if (!continue_execution) return false;

  // AssembleCollection() advances child readers, so we don't need to call NextLevels()
  UpdateDerivedState();
  return true;
}

void CollectionColumnReader::UpdateDerivedState() {
  // We don't need to cap our def_level_ at max_def_level(). We always check def_level_
  // >= max_def_level() to check if the collection is defined.
  // TODO(skye): consider capping def_level_ at max_def_level()
  def_level_ = children_[0]->def_level();
  rep_level_ = children_[0]->rep_level();

  // All children should have been advanced to the beginning of the next collection
  for (int i = 0; i < children_.size(); ++i) {
    DCHECK_EQ(children_[i]->rep_level(), rep_level_);
    if (def_level_ < max_def_level()) {
      // Collection not defined
      FILE_CHECK_EQ(children_[i]->def_level(), def_level_);
    } else {
      // Collection is defined
      FILE_CHECK_GE(children_[i]->def_level(), max_def_level());
    }
  }

  if (RowGroupAtEnd()) {
    // No more values
    pos_current_value_ = HdfsParquetScanner::INVALID_POS;
  } else if (rep_level_ <= max_rep_level() - 2) {
    // Reset position counter if we are at the start of a new parent collection (i.e.,
    // the current collection is the first item in a new parent collection).
    pos_current_value_ = 0;
  }
}

/// Returns a column reader for decimal types based on its size and parquet type.
static ParquetColumnReader* GetDecimalColumnReader(const SchemaNode& node,
    const SlotDescriptor* slot_desc, HdfsParquetScanner* parent) {
  switch (node.element->type) {
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      switch (slot_desc->type().GetByteSize()) {
      case 4:
        return new ScalarColumnReader<Decimal4Value, parquet::Type::FIXED_LEN_BYTE_ARRAY,
            true>(parent, node, slot_desc);
      case 8:
        return new ScalarColumnReader<Decimal8Value, parquet::Type::FIXED_LEN_BYTE_ARRAY,
            true>(parent, node, slot_desc);
      case 16:
        return new ScalarColumnReader<Decimal16Value, parquet::Type::FIXED_LEN_BYTE_ARRAY,
            true>(parent, node, slot_desc);
      }
      break;
    case parquet::Type::BYTE_ARRAY:
      switch (slot_desc->type().GetByteSize()) {
      case 4:
        return new ScalarColumnReader<Decimal4Value, parquet::Type::BYTE_ARRAY, true>(
            parent, node, slot_desc);
      case 8:
        return new ScalarColumnReader<Decimal8Value, parquet::Type::BYTE_ARRAY, true>(
            parent, node, slot_desc);
      case 16:
        return new ScalarColumnReader<Decimal16Value, parquet::Type::BYTE_ARRAY, true>(
            parent, node, slot_desc);
      }
      break;
    default:
      DCHECK(false) << "Invalid decimal primitive type";
  }
  DCHECK(false) << "Invalid decimal type";
  return nullptr;
}

ParquetColumnReader* ParquetColumnReader::Create(const SchemaNode& node,
    bool is_collection_field, const SlotDescriptor* slot_desc,
    HdfsParquetScanner* parent) {
  ParquetColumnReader* reader = nullptr;
  if (is_collection_field) {
    // Create collection reader (note this handles both NULL and non-NULL 'slot_desc')
    reader = new CollectionColumnReader(parent, node, slot_desc);
  } else if (slot_desc != nullptr) {
    // Create the appropriate ScalarColumnReader type to read values into 'slot_desc'
    switch (slot_desc->type().type) {
      case TYPE_BOOLEAN:
        reader = new BoolColumnReader(parent, node, slot_desc);
        break;
      case TYPE_TINYINT:
        reader = new ScalarColumnReader<int8_t, parquet::Type::INT32, true>(parent, node,
            slot_desc);
        break;
      case TYPE_SMALLINT:
        reader = new ScalarColumnReader<int16_t, parquet::Type::INT32, true>(parent, node,
            slot_desc);
        break;
      case TYPE_INT:
        reader = new ScalarColumnReader<int32_t, parquet::Type::INT32, true>(parent, node,
            slot_desc);
        break;
      case TYPE_BIGINT:
        reader = new ScalarColumnReader<int64_t, parquet::Type::INT64, true>(parent, node,
            slot_desc);
        break;
      case TYPE_FLOAT:
        reader = new ScalarColumnReader<float, parquet::Type::FLOAT, true>(parent, node,
            slot_desc);
        break;
      case TYPE_DOUBLE:
        reader = new ScalarColumnReader<double, parquet::Type::DOUBLE, true>(parent, node,
            slot_desc);
        break;
      case TYPE_TIMESTAMP:
        reader = new ScalarColumnReader<TimestampValue, parquet::Type::INT96, true>(
            parent, node, slot_desc);
        break;
      case TYPE_STRING:
      case TYPE_VARCHAR:
      case TYPE_CHAR:
        reader = new ScalarColumnReader<StringValue, parquet::Type::BYTE_ARRAY, true>(
            parent, node, slot_desc);
        break;
      case TYPE_DECIMAL:
        reader = GetDecimalColumnReader(node, slot_desc, parent);
        break;
      default:
        DCHECK(false) << slot_desc->type().DebugString();
    }
  } else {
    // Special case for counting scalar values (e.g. count(*), no materialized columns in
    // the file, only materializing a position slot). We won't actually read any values,
    // only the rep and def levels, so it doesn't matter what kind of reader we make.
    reader = new ScalarColumnReader<int8_t, parquet::Type::INT32, false>(parent, node,
        slot_desc);
  }
  return parent->obj_pool_.Add(reader);
}

}
