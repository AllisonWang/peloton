//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpch_stats.cpp
//
// Identification: src/main/tpch/tpch_stats.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "benchmark/tpch/tpch_database.h"

#include "benchmark/tpch/tpch_workload.h"
#include "benchmark/tpch/tpch_stats.h"

#include "catalog/catalog.h"
#include "common/logger.h"
#include "optimizer/stats/selectivity.h"
#include "optimizer/stats/tuple_samples_storage.h"
#include "optimizer/stats/stats_storage.h"
#include "optimizer/stats/value_condition.h"
#include "optimizer/stats/table_stats.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "catalog/catalog.h"
#include "catalog/column_catalog.h"
#include "executor/testing_executor_util.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace benchmark {
namespace tpch {

TPCHStats::TPCHStats(const Configuration &config, TPCHDatabase &db)
          : config_(config), db_(db) {}

void TPCHStats::RunTest() {
  std::vector<benchmark::tpch::TableId> tpch_table_ids = {
    TableId::Part,
    //TableId::Supplier,
    //TableId::PartSupp,
    TableId::Customer,
    TableId::Nation,
    TableId::Lineitem,
    //TableId::Region,
    //TableId::Orders
  };
  
  for (auto tid : tpch_table_ids) {
    db_.LoadTable(tid);
  }

  auto stats_storage = optimizer::StatsStorage::GetInstance();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  stats_storage->AnalyzeStatsForAllTables(txn);
  txn_manager.CommitTransaction(txn);

  auto catalog = catalog::Catalog::GetInstance();
  auto database = catalog->GetDatabaseWithOid(44);

  for (auto tid : tpch_table_ids) {
    //if (db_.TableIsLoaded(tid)) {
    uint32_t t_id = static_cast<uint32_t>(tid);
    auto table = catalog->GetTableWithOid(44, t_id);
    oid_t db_id = database->GetOid();
    oid_t table_id = table->GetOid();
    
    auto table_stats = stats_storage->GetTableStats(db_id, table_id);

    // type::Value value1 = type::ValueFactory::GetIntegerValue(150);
    // optimizer::ValueCondition condition{column_id, ExpressionType::COMPARE_LESSTHAN, value1};

    // Get updated table stats and check new selectivity
    table_stats = stats_storage->GetTableStats(db_id, table_id);
    //double less_than_sel = optimizer::Selectivity::ComputeSelectivity(table_stats, condition);

    
    //LOG_INFO("%1.5f; %1.5f; %lu", less_than_sel, column_stats->cardinality, column_stats->num_rows);
    // LOG_INFO("%1.5f; %1.5f", less_than_sel, table_stats->GetCardinality(0));

    // table_id, CARDI, CARDERR, NUMROWS 
    // LOG_INFO("%u\t%1.5f\t%lu", t_id, table_stats->GetCardinality(0), table_stats->num_rows);

    oid_t column_id = 0;  // first column
    auto column_stats = table_stats->GetColumnStats(column_id);
    // table_id, column_id, CARDI, num_rows, FRAC_NULL
    LOG_INFO("%u\t%u\t%1.5f\t%lu\t%1.5f", t_id, column_id, column_stats->cardinality, column_stats->num_rows, column_stats->frac_null);
    
    // most common vals & freqs
    std::vector<double> most_common_vals = column_stats->most_common_vals;
    std::vector<double> most_common_freqs = column_stats->most_common_freqs;
    std::vector<double>::iterator first = most_common_vals.begin(), last = most_common_vals.end(), 
      ffirst = most_common_freqs.begin(), flast = most_common_freqs.end();

    LOG_INFO("t-%u: most_common_vals & freqs: \n", t_id);
    while ((first != last) &&(ffirst != flast)) {
      // val : freq
      LOG_INFO("%1.5f\t%1.5f", *first, *ffirst);
      ++first;
      ++ffirst;
    }

    std::vector<double> histogram_bounds = column_stats->histogram_bounds;
    first = histogram_bounds.begin();
    last = histogram_bounds.end();
    LOG_INFO("t-%u: histogram_bounds: \n", t_id);
    while(first != last) {
      LOG_INFO("%1.5f", *first);
      ++first;
    }
    
    
    //}
    

  // txn_manager.CommitTransaction(txn);
  }
  
}

}  // namespace tpch
}  // namespace benchmark
}  // namespace peloton

