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

#include "exec/parquet-index-filter.h"

#include <algorithm>
#include <sstream>

#include "common/logging.h"
#include "exec/parquet-column-stats.h"
#include "exprs/scalar-expr.h"
#include "rpc/thrift-util.h"
#include "util/debug-util.h"
#include "util/error-util.h"

using namespace impala;

namespace impala {

Status ParquetIndexFilter::EvaluateIndexConjuncts(bool* skip_pages,
    vector<FilteredPageInfos>& valid_pages, vector<RowRange>& valid_ranges,
    vector<ScannerContext::Stream*>& column_idx_streams,
    ScannerContext::Stream* offset_idx_stream) {
  *skip_pages = false;
  if (!min_max_tuple_desc_) return Status::OK();

  int tuple_size = min_max_tuple_desc_->byte_size();
  min_max_tuple_->Init(tuple_size);
  DCHECK_EQ(min_max_tuple_desc_->slots().size(), min_max_conjunct_evals_.size());

  // Tracks the ranges to be skipped.
  vector<RowRange> skip_ranges;

  if (min_max_conjunct_evals_.size() > 0) {
    // Initialize row_group_offsetindex to determine the range of rows to be skipped.
    RETURN_IF_ERROR(InitOffsetIndex(offset_idx_stream));
  }
  for (int i = 0; i < min_max_conjunct_evals_.size(); ++i) {
    SlotDescriptor* slot_desc = min_max_tuple_desc_->slots()[i];
    ScalarExprEvaluator* eval = min_max_conjunct_evals_[i];

    // Resolve column path to determine col_idx
    SchemaNode* node = nullptr;
    bool pos_field;
    bool missing_field;
    RETURN_IF_ERROR(schema_resolver_->ResolvePath(
        slot_desc->col_path(), &node, &pos_field, &missing_field));
    // Since the index filter is invoked only if the row_group is known to contain every
    // field in the conjuncts, the slot_desc can never be a missing field.
    DCHECK(!missing_field);
    if (pos_field) {
      // We are selecting a column that is not in the file. We would set its slot to NULL
      // during the scan, so any predicate would evaluate to false. Return early. NULL
      // comparisons cannot happen here, since predicates with NULL literals are filtered
      // in the frontend.
      std::stringstream err;
      err << "Statistics not supported for pos fields: " << slot_desc->DebugString();
      DCHECK(false) << err.str();
      return Status(err.str());
    }

    int col_idx = node->col_idx;
    DCHECK(col_idx < column_idx_streams.size());

    const parquet::ColumnOrder* col_order = nullptr;
    if (col_idx < column_orders_.size()) col_order = &column_orders_[col_idx];

    const ColumnType& col_type = slot_desc->type();
    void* slot = min_max_tuple_->GetSlot(slot_desc->tuple_offset());

    const string& fn_name = eval->root().function_name();
    // Initialize the ColumnIndex for the require col_idx.
    RETURN_IF_ERROR(InitColumnIndex(column_idx_streams, col_idx));
    parquet::ColumnIndex column_index = column_indexes_[col_idx];

    DCHECK_EQ(column_index.valid_values.size(), column_index.min_values.size());
    bool ordered_column = (column_index.max_values.size() == 0);

    const parquet::ColumnChunk col_chunk = row_group_.columns[col_idx];
    const parquet::OffsetIndex offset_index =
        row_group_offset_index_.offset_indexes[col_idx];

    for (int page_idx = 0; page_idx < column_index.valid_values.size(); ++page_idx) {
      bool value_read = false;
      if (column_index.valid_values[page_idx]) {
        if (fn_name == "lt" || fn_name == "le") {
          // We need to get min value.
          value_read = ColumnStatsBase::ReadFromString(
              col_type, &column_index.min_values[page_idx], slot);
        } else if (fn_name == "gt" || fn_name == "ge") {
          if (ordered_column) {
            // Ordered column. For any page other than the last page, get the min value
            // of the next page. For the last page, get the max value of the RowGroup.
            if (page_idx == (column_index.valid_values.size() - 1)) {
              value_read = ColumnStatsBase::ReadFromThrift(
                  col_chunk, col_type, col_order, ColumnStatsBase::StatsField::MAX, slot);
            } else {
              value_read = ColumnStatsBase::ReadFromString(
                  col_type, &column_index.min_values[page_idx + 1], slot);
            }
          } else {
            // Unordered column, we need to get the max value.
            value_read = ColumnStatsBase::ReadFromString(
                col_type, &column_index.max_values[page_idx], slot);
          }
        } else {
          DCHECK(false) << "Unsupported function name for statistics evaluation: "
                        << fn_name;
          break;
        }
      }
      if (value_read) {
        TupleRow row;
        row.SetTuple(0, min_max_tuple_);
        if (!ExecNode::EvalPredicate(eval, &row)) {
          int64_t begin_row, end_row;
          // The skip always ends with the first_row_index of the page.
          begin_row = offset_index.page_locations[page_idx].first_row_index;
          // For the last page in the RowGroup, the skip ends with the number of rows in
          // the RowGroup. For all other pages, it end at first_row_index of the next
          // page -1.
          if (page_idx == (column_index.valid_values.size() - 1)) {
            end_row = row_group_.num_rows - 1;
          } else {
            end_row = offset_index.page_locations[page_idx + 1].first_row_index - 1;
          }
          skip_ranges.push_back(std::make_pair(begin_row, end_row));
        }
      }
    }
  }

  if (skip_ranges.size()) {
    // Sort the skip ranges in order to consolidate the valid rows.
    std::sort(skip_ranges.begin(), skip_ranges.end(), std::less<RowRange>());

    // This block computes the union of the valid row ranges which need to be scanned.
    // It merges overlapping 'skip_ranges' and appends a row_range to the list of valid
    // valid ranges only if two adjacent skip ranges are non-overlapping. The 'skip_end'
    // variable keeps track of the end of the previous skip range.
    int64_t skip_end = -1; // Assume no page is currently valid.
    for (RowRange row_range : skip_ranges) {
      // If the skip begins before or one row after the previous skip ended, then the
      // skips can be merged. Else, add all the rows which lie between the previous
      // skip and the current skip to the set of valid rows.
      if ((skip_end + 1) >= row_range.first) {
        // Modify the skip end if the new skip ends after the previous skip.
        if (skip_end < row_range.second) skip_end = row_range.second;
      } else {
        valid_ranges.push_back(std::make_pair(skip_end + 1, row_range.first - 1));
        skip_end = row_range.second;
      }
    }

    // If the last skip ended before the end of the row_group, add the remaining rows to
    // the valid range.
    if (skip_end < row_group_.num_rows - 1) {
      valid_ranges.push_back(std::make_pair(skip_end + 1, row_group_.num_rows - 1));
    }

  } else {
    // None of the rows can be skipped. Scan entire RowGroup.
    return Status::OK();
  }

  if (valid_ranges.size()) {
    valid_pages.resize(row_group_.columns.size());
    // For each column in the row group, determine the set of valid pages to be read.
    for (int col_idx = 0; col_idx < row_group_.columns.size(); ++col_idx) {
      parquet::OffsetIndex offset_index = row_group_offset_index_.offset_indexes[col_idx];
      vector<parquet::PageLocation> page_locations = offset_index.page_locations;
      vector<RowRange>::iterator it = valid_ranges.begin();

      for (int page_idx = 0; page_idx < page_locations.size(); ++page_idx) {
        int64_t first_row_index = page_locations[page_idx].first_row_index;
        int64_t last_row_index;

        if (page_idx == page_locations.size() - 1) {
          last_row_index = row_group_.num_rows - 1;
        } else
          last_row_index = page_locations[page_idx + 1].first_row_index - 1;

        FilteredPageInfo page{.page_id = page_idx,
            .total_page_size = page_locations[page_idx].total_page_size,
            .offset = page_locations[page_idx].offset,
            .first_row_index = first_row_index};
        vector<RowRange> ranges;
        while (it != valid_ranges.end()) {
          int64_t start_offset = -1;
          int64_t end_offset = -1;
          // Set the start offset within the page if a part/whole of the range lies
          // within the page.
          if ((it->first >= first_row_index) && (it->first <= last_row_index)) {
            start_offset = it->first;
          } else if ((first_row_index > it->first) && (it->first <= last_row_index)) {
            start_offset = first_row_index;
          }
          if (last_row_index <= it->second) {
            end_offset = last_row_index;
          } else {
            end_offset = it->second;
          }

          // For valid ranges, add a range within the page.
          if (start_offset != -1) {
            ranges.push_back(std::make_pair(start_offset, end_offset));
          }
          // If the current page extends beyond the current range, check the next range.
          if (last_row_index >= it->second) ++it;
          else break;
        }
        // Add the current page to the set of valid pages if it contains atleast one valid
        // range which needs to be scanned.
        if (ranges.size()) {
          valid_pages[col_idx].push_back(page);
        }
        if (it == valid_ranges.end()) break;
      }
    }
    *skip_pages = true;
  }
  return Status::OK();
}

Status ParquetIndexFilter::InitOffsetIndex(ScannerContext::Stream* offset_idx_stream) {
  Status status;
  uint8_t* buffer;
  uint32_t idx_len = row_group_.offset_index_length;
  DCHECK(offset_idx_stream != nullptr);
  if (!offset_idx_stream->ReadBytes(idx_len, &buffer, &status)) return status;
  RETURN_IF_ERROR(DeserializeThriftMsg(buffer, &idx_len, true, &row_group_offset_index_));
  return Status::OK();
}

Status ParquetIndexFilter::InitColumnIndex(
    vector<ScannerContext::Stream*> column_idx_streams, int col_idx) {
  Status status;
  uint8_t* buffer;
  parquet::ColumnChunk col_chunk = row_group_.columns[col_idx];
  uint32_t idx_len = col_chunk.column_index_length;
  // If column_indexes_ vector is not yet initialized, resize it to accommodate all the
  // columns in the row group.
  if (column_indexes_.size() != column_idx_streams.size()) {
    column_indexes_.resize(column_idx_streams.size());
  }
  DCHECK(column_idx_streams[col_idx] != nullptr);

  // If the ColumnIndex at col_idx has not been read from, read and deserialize it
  // from Thrift.
  if (!column_idx_streams[col_idx]->eosr()) {
    if (!column_idx_streams[col_idx]->ReadBytes(idx_len, &buffer, &status)) {
      return status;
    }
    RETURN_IF_ERROR(
        DeserializeThriftMsg(buffer, &idx_len, true, &column_indexes_[col_idx]));
  }
  return Status::OK();
}
}
