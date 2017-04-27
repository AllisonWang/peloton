//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column_catalog.h
//
// Identification: src/include/catalog/column_stats_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_column_stats
//
// Schema: (column offset: column_name)
// 0: database_id (pkey)
// 1: table_id (pkey)
// 2: column_id (pkey)
// 3: num_row
// 4: cardinality
// 5: frac_null
// 6: most_common_vals
// 7: most_common_freqs
// 8: histogram_bounds
//
// Indexes: (index offset: indexed columns)
// 0: name & database_oid (unique & primary key)
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"
#include "statistics/query_metric.h"

#define COLUMN_STATS_CATALOG_NAME "pg_column_stats"

namespace peloton {

namespace optimizer {
class ColumnStats;
}

namespace catalog {

class ColumnStatsCatalog : public AbstractCatalog {
 public:
  ~ColumnStatsCatalog();

  // Global Singleton
  static ColumnStatsCatalog *GetInstance(
      concurrency::Transaction *txn = nullptr);

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertColumnStats(oid_t database_id, oid_t table_id, oid_t column_id,
                         int num_row, double cardinality, double frac_null,
                         std::string most_common_vals, double most_common_freqs,
                         std::string histogram_bounds, type::AbstractPool *pool,
                         concurrency::Transaction *txn);
  bool DeleteColumnStats(oid_t database_id, oid_t table_id, oid_t column_id,
                         concurrency::Transaction *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  std::unique_ptr<std::vector<type::Value>> GetColumnStats(
      oid_t database_id, oid_t table_id, oid_t column_id,
      concurrency::Transaction *txn);
  // TODO: add more if needed

  enum ColumnId {
    DATABASE_ID = 0,
    TABLE_ID = 1,
    COLUMN_ID = 2,
    NUM_ROW = 3,
    CARDINALITY = 4,
    FRAC_NULL = 5,
    MOST_COMMON_VALS = 6,
    MOST_COMMON_FREQS = 7,
    HISTOGRAM_BOUNDS = 8,
    // Add new columns here in creation order
  };

 private:
  ColumnStatsCatalog(concurrency::Transaction *txn);

  enum IndexId {
    SECONDARY_KEY_0 = 0,
    // Add new indexes here in creation order
  };
};

}  // End catalog namespace
}  // End peloton namespace