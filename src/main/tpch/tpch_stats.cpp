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

namespace peloton {
namespace benchmark {
namespace tpch {

TPCHStats::TPCHStats(const Configuration &config, TPCHDatabase &db)
          : config_(config), db_(db) {}

void TPCHStats::RunTest() {
  std::vector<benchmark::tpch::TableId> tpch_table_ids = {
    TableId::Customer,
    TableId::Part,
    TableId::Supplier,
    TableId::PartSupp,
    TableId::Customer,
    TableId::Nation,
    TableId::Lineitem,
    TableId::Region,
    TableId::Orders
  };

  for (auto tid : tpch_table_ids) {
    db_.LoadTable(tid);
  }
}

}  // namespace tpch
}  // namespace benchmark
}  // namespace peloton
