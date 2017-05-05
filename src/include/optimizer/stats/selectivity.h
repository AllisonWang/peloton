//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// selectivity.h
//
// Identification: src/include/optimizer/stats/selectivity.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cmath>
#include <vector>
#include <algorithm>

#include "type/value.h"
#include "common/macros.h"
#include "common/logger.h"
#include "storage/data_table.h"
#include "optimizer/stats/stats_storage.h"
#include "optimizer/stats/stats_util.h"
#include "optimizer/stats/tuple_samples_storage.h"
#include "catalog/column_catalog.h"
#include "catalog/catalog.h"

namespace peloton {
namespace optimizer {

static constexpr double DEFAULT_SELECTIVITY = 0.5;

class Selectivity {
 public:
  // Get expression selectivity.
  static double GetSelectivity(oid_t database_id, oid_t table_id,
                               oid_t column_id, const type::Value &value,
                               ExpressionType type) {
    switch (type) {
      case ExpressionType::COMPARE_EQUAL:
        return GetEqualSelectivity(database_id, table_id, column_id, value);
      case ExpressionType::COMPARE_NOTEQUAL:
        return GetNotEqualSelectivity(database_id, table_id, column_id, value);
      case ExpressionType::COMPARE_LESSTHAN:
        return GetLessThanSelectivity(database_id, table_id, column_id, value);
      case ExpressionType::COMPARE_GREATERTHAN:
        return GetGreaterThanSelectivity();
      case ExpressionType::COMPARE_LESSTHANOREQUALTO:
        return GetLessThanOrEqualToSelectivity();
      case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
        return GetGreaterThanOrEqualToSelectivity();
      default:
        return -1;  // TODO: return something more meaningful
    }
  }

  // Selectivity of '<' expression.
  static double GetLessThanSelectivity(oid_t database_id, oid_t table_id,
                                       oid_t column_id,
                                       const type::Value &value) {
    // Convert peloton value to double
    UNUSED_ATTRIBUTE double v = PelotonValueToNumericValue(value);

    // Get column stats and histogram from stats storage
    auto stats_storage = optimizer::StatsStorage::GetInstance();
    auto column_stats =
        stats_storage->GetColumnStatsByID(database_id, table_id, column_id);
    if (column_stats == nullptr) {
      return DEFAULT_SELECTIVITY;
    }
    std::vector<double> histogram = column_stats->histogram_bounds;
    size_t n = histogram.size();
    PL_ASSERT(n > 0);

    // find correspond bin using binary serach
    auto it = std::lower_bound(histogram.begin(), histogram.end(), v);
    return (it - histogram.begin()) * 1.0 / n;
  }

  // Selectivity of '<=' expression.
  static double GetLessThanOrEqualToSelectivity() { return 0; }

  // Selectivity of '>' expression.
  static double GetGreaterThanSelectivity() { return 0; }

  // Selectivity of '>=' expression.
  static double GetGreaterThanOrEqualToSelectivity() { return 0; }

  static double GetEqualSelectivity() { return 0; }

  static double GetNotEqualSelectivity() { return 0; }

  // equal operator between left and right
  static double GetEqualSelectivity(oid_t database_id, oid_t table_id,
                                    oid_t column_id, const type::Value &value) {
    auto stats_storage = optimizer::StatsStorage::GetInstance();
    auto column_stats =
        stats_storage->GetColumnStatsByID(database_id, table_id, column_id);
    if (column_stats == nullptr) {
      return DEFAULT_SELECTIVITY;
    }
    size_t numrows = column_stats->num_row;

    // For now only double is supported in stats storage
    std::vector<double> most_common_vals = column_stats->most_common_vals;
    std::vector<double> most_common_freqs = column_stats->most_common_freqs;
    std::vector<double>::iterator first = most_common_vals.begin(),
                                  last = most_common_vals.end();

    while (first != last) {
      // For now only double is supported in stats storage
      if (*first == value.GetAs<double>()) {
        break;
      }
      ++first;
    }

    if (first != last) {
      // the target value for equality comparison (param value) is
      // found in most common values
      size_t idx = first - most_common_vals.begin();

      return most_common_freqs[idx] / (double)numrows;
    } else {
      // the target value for equality comparison (parm value) is
      // NOT found in most common values

      // (1 - sum(mvf))/(num_distinct - num_mcv)
      double sum_mvf = 0;
      std::vector<double>::iterator first = most_common_freqs.begin(),
                                    last = most_common_freqs.end();
      while (first != last) {
        sum_mvf += *first;
        ++first;
      }

      return (1 - sum_mvf / (double)numrows) /
             (column_stats->cardinality - most_common_vals.size());
    }
  }

  // inequal operator between left and right
  static double GetNotEqualSelectivity(oid_t database_id, oid_t table_id,
                                       oid_t column_id,
                                       const type::Value &value) {
    return 1 - GetEqualSelectivity(database_id, table_id, column_id, value);
  }

  // Selectivity for '~~'(LIKE) expression. The column type must be VARCHAR.
  static double GetLikeSelectivity(oid_t database_id, oid_t table_id,
                                   oid_t column_id,
                                   UNUSED_ATTRIBUTE const std::string &pattern,
                                   bool check_type = true) {
    if (check_type) {
      auto column_catalog = catalog::ColumnCatalog::GetInstance();
      auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      auto txn = txn_manager.BeginTransaction();
      type::Type::TypeId column_type =
          column_catalog->GetColumnType(table_id, column_id, txn);
      txn_manager.CommitTransaction(txn);

      if (column_type != type::Type::TypeId::VARCHAR) {
        return 0;
      }
    }

    std::vector<type::Value> column_samples;
    // auto catalog = catalog::Catalog::GetInstance();
    // auto data_table = catalog->GetTableWithOid(database_id, table_id);
    auto tuple_storage = optimizer::TupleSamplesStorage::GetInstance();
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    tuple_storage->GetColumnSamples(database_id, table_id, column_id,
                                    column_samples);
    txn_manager.CommitTransaction(txn);

    for (size_t i = 0; i < column_samples.size(); i++) {
      LOG_DEBUG("Value: %s", column_samples[i].GetInfo().c_str());
    }

    return 0;
  }

  static double GetNotLikeSelectivity(oid_t database_id, oid_t table_id,
                                      oid_t column_id,
                                      const std::string &pattern) {
    auto column_catalog = catalog::ColumnCatalog::GetInstance();
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    type::Type::TypeId column_type =
        column_catalog->GetColumnType(table_id, column_id, txn);
    txn_manager.CommitTransaction(txn);

    if (column_type != type::Type::TypeId::VARCHAR) {
      return 0;
    }

    return 1 -
           GetLikeSelectivity(database_id, table_id, column_id, pattern, false);
  }
};

} /* namespace optimizer */
} /* namespace peloton */
