//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_storage.cpp
//
// Identification: src/optimizer/stats/stats_storage.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/stats/stats_storage.h"
#include "catalog/catalog.h"
#include "type/value.h"
#include "storage/data_table.h"
#include "storage/table_factory.h"
#include "catalog/column_stats_catalog.h"

namespace peloton {
namespace optimizer {

// Get instance of the global stats storage
StatsStorage *StatsStorage::GetInstance(void) {
  static std::unique_ptr<StatsStorage> global_stats_storage(new StatsStorage());
  return global_stats_storage.get();
}

/**
 * StatsStorage - Constructor of StatsStorage.
 * In the construcotr, `stats` table and `samples_db` database are created.
 */
StatsStorage::StatsStorage() {
  pool_.reset(new type::EphemeralPool());
  CreateStatsCatalog();
  CreateSamplesDatabase();
}

/**
 * ~StatsStorage - Deconstructor of StatsStorage.
 * It deletes(drops) the 'samples_db' from catalog in case of memory leak.
 * TODO: Remove this when catalog frees the databases memory in its
 * deconstructor
 * in the future.
 */
StatsStorage::~StatsStorage() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(SAMPLES_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

/**
 * CreateStatsCatalog - Create 'stats' table in the catalog database.
 */
void StatsStorage::CreateStatsCatalog() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::ColumnStatsCatalog::GetInstance(txn);
  txn_manager.CommitTransaction(txn);
}

/**
 * AddOrUpdateTableStats - Add or update all column stats of a table.
 * This function iterates all column stats in the table stats and insert column
 * stats tuples into the 'stats' table in the catalog database.
 * This function only add table stats to the catalog for now.
 * TODO: Implement stats UPDATE if the column stats already exists.
 */
void StatsStorage::AddOrUpdateTableStats(storage::DataTable *table,
                                         TableStats *table_stats) {
  // Add or update column stats sequentially.
  oid_t database_id = table->GetDatabaseOid();
  oid_t table_id = table->GetOid();
  size_t num_row = table_stats->GetActiveTupleCount();

  oid_t column_count = table_stats->GetColumnCount();
  for (oid_t column_id = 0; column_id < column_count; column_id++) {
    ColumnStats *column_stats = table_stats->GetColumnStats(column_id);
    double cardinality = column_stats->GetCardinality();
    double frac_null = column_stats->GetFracNull();
    // Currently, we only store the most common value and its frequency in stats
    // table because Peloton doesn't support ARRAY type now.
    // TODO: Store multiple common values and freqs in stats table.
    std::vector<ValueFrequencyPair> most_common_val_freqs =
        column_stats->GetCommonValueAndFrequency();
    std::vector<double> histogram_bounds = column_stats->GetHistogramBound();

    std::string most_common_val_str, histogram_bounds_str;
    double most_common_freq = 0;
    if (most_common_val_freqs.size() > 0) {
      most_common_val_str = most_common_val_freqs[0].first.ToString();
      most_common_freq = most_common_val_freqs[0].second;
    }
    if (histogram_bounds.size() > 0) {
      histogram_bounds_str = ConvertDoubleArrayToString(histogram_bounds);
    }

    AddOrUpdateColumnStats(database_id, table_id, column_id, num_row,
                           cardinality, frac_null, most_common_val_str,
                           most_common_freq, histogram_bounds_str);
  }
}

void StatsStorage::AddOrUpdateColumnStats(oid_t database_id, oid_t table_id,
                                          oid_t column_id, int num_row,
                                          double cardinality, double frac_null,
                                          std::string most_common_vals,
                                          double most_common_freqs,
                                          std::string histogram_bounds) {
  auto column_stats_catalog = catalog::ColumnStatsCatalog::GetInstance(nullptr);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  column_stats_catalog->DeleteColumnStats(database_id, table_id, column_id,
                                          txn);
  column_stats_catalog->InsertColumnStats(
      database_id, table_id, column_id, num_row, cardinality, frac_null,
      most_common_vals, most_common_freqs, histogram_bounds, pool_.get(), txn);
  txn_manager.CommitTransaction(txn);
}

/**
 * GetColumnStatsByID - Query the 'stats' table to get the column stats by IDs.
 * TODO: Implement this function.
 */
std::unique_ptr<std::vector<type::Value>> StatsStorage::GetColumnStatsByID(
    oid_t database_id, oid_t table_id, oid_t column_id) {
  auto column_stats_catalog = catalog::ColumnStatsCatalog::GetInstance(nullptr);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto column_stats = column_stats_catalog->GetColumnStats(
      database_id, table_id, column_id, txn);
  txn_manager.CommitTransaction(txn);
  return std::move(column_stats);
}

/**
 * CollectStatsForAllTables - This function iterates all databases and
 * datatables
 * to collect their stats and store them in the 'stats' table.
 */
void StatsStorage::CollectStatsForAllTables() {
  auto catalog = catalog::Catalog::GetInstance();

  oid_t database_count = catalog->GetDatabaseCount();
  for (oid_t db_offset = 0; db_offset < database_count; db_offset++) {
    auto database =
        catalog::Catalog::GetInstance()->GetDatabaseWithOffset(db_offset);
    oid_t table_count = database->GetTableCount();
    for (oid_t table_offset = 0; table_offset < table_count; table_offset++) {
      auto table = database->GetTable(table_offset);
      std::unique_ptr<TableStats> table_stats(new TableStats(table));
      table_stats->CollectColumnStats();
      AddOrUpdateTableStats(table, table_stats.get());
    }
  }
}

/**
 * CreateSamplesDatabase - Create a database for storing samples tables.
 */
void StatsStorage::CreateSamplesDatabase() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(SAMPLES_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

/**
 * AddSamplesTable - Add a samples table into the 'samples_db'.
 * The table name is generated by concatenating db_id and table_id with '_'.
 */
void StatsStorage::AddSamplesTable(
    storage::DataTable *data_table,
    std::vector<std::unique_ptr<storage::Tuple>> &sampled_tuples) {
  auto schema = data_table->GetSchema();
  auto schema_copy = catalog::Schema::CopySchema(schema);
  std::unique_ptr<catalog::Schema> schema_ptr(schema_copy);
  auto catalog = catalog::Catalog::GetInstance();
  bool is_catalog = false;
  std::string samples_table_name = GenerateSamplesTableName(
      data_table->GetDatabaseOid(), data_table->GetOid());

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog->CreateTable(std::string(SAMPLES_DB_NAME), samples_table_name,
                       std::move(schema_ptr), txn, is_catalog);

  auto samples_table = catalog->GetTableWithName(std::string(SAMPLES_DB_NAME),
                                                 samples_table_name, txn);

  for (auto &tuple : sampled_tuples) {
    InsertSampleTuple(samples_table, std::move(tuple), txn);
  }
  txn_manager.CommitTransaction(txn);
}

bool StatsStorage::InsertSampleTuple(storage::DataTable *samples_table,
                                     std::unique_ptr<storage::Tuple> tuple,
                                     concurrency::Transaction *txn) {
  if (txn == nullptr) {
    return false;
  }

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  planner::InsertPlan node(samples_table, std::move(tuple));
  executor::InsertExecutor executor(&node, context.get());
  executor.Init();
  bool status = executor.Execute();

  return status;
}

/**
 * GetTupleSamples - Query tuple samples by db_id and table_id.
 * Implement this function.
 */
void StatsStorage::GetTupleSamples(
    UNUSED_ATTRIBUTE oid_t database_id, UNUSED_ATTRIBUTE oid_t table_id,
    UNUSED_ATTRIBUTE std::vector<storage::Tuple> &tuple_samples) {}

/**
 * GetColumnSamples - Query column samples by db_id, table_id and column_id.
 * TODO: Implement this function.
 */
void StatsStorage::GetColumnSamples(
    UNUSED_ATTRIBUTE oid_t database_id, UNUSED_ATTRIBUTE oid_t table_id,
    UNUSED_ATTRIBUTE oid_t column_id,
    UNUSED_ATTRIBUTE std::vector<type::Value> &column_samples) {}

} /* namespace optimizer */
} /* namespace peloton */
