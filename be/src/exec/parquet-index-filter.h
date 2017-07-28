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

#ifndef IMPALA_EXEC_PARQUET_INDEX_FILTER_H
#define IMPALA_EXEC_PARQUET_INDEX_FILTER_H

#include <utility>
#include <vector>

#include "common/status.h"
#include "exec/hdfs-scan-node-base.h"
#include "exec/parquet-common.h"
#include "exec/parquet-metadata-utils.h"
#include "exec/scanner-context.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"

namespace impala {
class Expr;
class ScalarExprEvaluator;
class ThriftSerializer;

/// Pair of int64_t to represent the start and end of a range of rows. Each boundary is
/// inclusive.
typedef std::pair<int64_t, int64_t> RowRange;

/// Structure of pages to indicate the set of rows to be read by PqrquetColumnReader.
struct FilteredPageInfo {
  int page_id;
  int32_t total_page_size;
  int64_t offset;
  int64_t first_row_index;
};

typedef vector<FilteredPageInfo> FilteredPageInfos;

/// The ParquetIndexFilter is used to evaluate the min/max conjuncts of the queries for
/// each page in the given RowGroup to determine the range of rows which need to be
/// scanned. The EvaluateIndexConjuncts filter iterates over all the ScalarExprEvaluators
/// and evaluates each conjunct by reading the index of the corresponding columns. If any
/// page may be skipped based on a conjunct, its RowRange is added to the list of rages to
/// be skipped (skip_ranges). Once all the conjuncts are evaluated, the list of skip
/// ranges are sorted and the union of all the valid ranges after filtering is computed.
/// For each column in the RowGroup, a list of valid pages which need to be scanned is
/// constructed and this list is returned to the calling HdfsParquetScanner.
class ParquetIndexFilter {
 public:
  ParquetIndexFilter(const parquet::RowGroup& row_group,
      const vector<parquet::ColumnOrder>& column_orders,
      const HdfsScanNodeBase* scan_node, Tuple* min_max_tuple,
      const vector<ScalarExprEvaluator*>& min_max_conjunct_evals,
      const ParquetSchemaResolver* schema_resolver)
    : row_group_(row_group),
      column_orders_(column_orders),
      min_max_tuple_desc_(scan_node->min_max_tuple_desc()),
      min_max_tuple_(min_max_tuple),
      min_max_conjunct_evals_(min_max_conjunct_evals),
      schema_resolver_(schema_resolver) {}

  ~ParquetIndexFilter() {}

  /// Evaluates the Conjuncts against the ColumnIndex stats present in the
  /// column_index_streams. Once the valid set of RowRanges are determined,
  /// the valid_pages are populated for each column to be processed by the
  /// HdfsParquetScanner and passed on to the ParquetColumnReader.
  Status EvaluateIndexConjuncts(bool* skip_pages, vector<FilteredPageInfos>& valid_pages,
      vector<RowRange>& valid_ranges, vector<ScannerContext::Stream*>& column_idx_streams,
      ScannerContext::Stream* offset_idx_stream);

 private:
  /// Initializes the ColumnIndex for the given col_idx by reading the required stream.
  Status InitColumnIndex(vector<ScannerContext::Stream*> column_idx_streams, int col_idx);

  /// Initializes the OffsetIndex for the RowGroup by reading the stream.
  Status InitOffsetIndex(ScannerContext::Stream* offset_idx_stream);

  /// RowGroup to be filtered.
  const parquet::RowGroup& row_group_;

  /// ColumnOrder of the columns present in the RowGroup.
  const std::vector<parquet::ColumnOrder>& column_orders_;

  /// Buffer to evaluate the conjuncts on the ColumnIndex.
  const TupleDescriptor* min_max_tuple_desc_;

  /// Tuple to store the statistics values which are read from the ColumnIndex.
  Tuple* min_max_tuple_;

  /// The min/max statistics conjuncts evaluators used by the calling HdfsParquetScanner.
  const vector<ScalarExprEvaluator*>& min_max_conjunct_evals_;

  /// Used to resolve the schema of the slots in the min_max_tuple_desc.
  const ParquetSchemaResolver* schema_resolver_;

  /// RowGroupOffsetIndex to hold the PageLocations of each column in the RowGroup.
  parquet::RowGroupOffsetIndex row_group_offset_index_;

  /// ColumnIndex used to determine the stats values for each page of a column.
  vector<parquet::ColumnIndex> column_indexes_;
};
}

#endif
