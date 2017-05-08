//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpch_stats.h
//
// Identification: src/include/benchmark/tpch/tpch_stats.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "benchmark/tpch/tpch_configuration.h"
#include "benchmark/tpch/tpch_database.h"

namespace peloton {
namespace benchmark {
namespace tpch {

class TPCHStats {
 public:
  TPCHStats(const Configuration &config, TPCHDatabase &db);

  // Run the stats test
  void RunTest();

 private:

  // The benchmark configuration
  const Configuration &config_;

  // The TPCH database
  TPCHDatabase &db_;

};

}  // namespace tpch
}  // namespace benchmark
}  // namespace peloton