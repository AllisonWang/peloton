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

namespace peloton {
namespace optimizer {

// Get instance of the global stats storage
StatsStorage *StatsStorage::GetInstance(void) {
  static std::unique_ptr<StatsStorage> global_stats_storage(new StatsStorage());
  return global_stats_storage.get();
}

StatsStorage::StatsStorage() {
  pool_.reset(new type::EphemeralPool());
  CreateStatsCatalog();
  CreateSamplesDatabase();
}

StatsStorage::~StatsStorage() {
  catalog::Catalog::GetInstance()->DropDatabaseWithName(SAMPLES_DB_NAME,
                                                        nullptr);
}

void StatsStorage::CreateStatsCatalog() {
  auto catalog = catalog::Catalog::GetInstance();
  auto catalog_db = catalog->GetDatabaseWithName(CATALOG_DATABASE_NAME);
  auto catalog_db_oid = catalog_db->GetOid();
  bool own_schema = true;
  bool adapt_table = false;
  bool is_catalog = true;

  // Create table for stats
  auto stats_schema = InitializeStatsSchema();
  std::unique_ptr<storage::DataTable> table(storage::TableFactory::GetDataTable(
      catalog_db_oid, catalog->GetNextOid(), stats_schema.release(),
      STATS_TABLE_NAME, DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table,
      is_catalog));
  catalog_db->AddTable(table.release(), true);
}

std::unique_ptr<catalog::Schema> StatsStorage::InitializeStatsSchema() {
  const std::string not_null_constraint_name = "not_null";
  catalog::Constraint not_null_constraint(ConstraintType::NOTNULL,
                                          not_null_constraint_name);
  oid_t integer_type_size = type::Type::GetTypeSize(type::Type::INTEGER);
  type::Type::TypeId integer_type = type::Type::INTEGER;
  oid_t decimal_type_size = type::Type::GetTypeSize(type::Type::DECIMAL);
  type::Type::TypeId decimal_type = type::Type::DECIMAL;
  oid_t varchar_type_size = type::Type::GetTypeSize(type::Type::VARCHAR);
  type::Type::TypeId varchar_type = type::Type::VARCHAR;

  auto database_id_column =
      catalog::Column(integer_type, integer_type_size, "database_id", true);
  database_id_column.AddConstraint(not_null_constraint);
  auto table_id_column =
      catalog::Column(integer_type, integer_type_size, "table_id", true);
  table_id_column.AddConstraint(not_null_constraint);
  auto column_id_column =
      catalog::Column(integer_type, integer_type_size, "column_id", true);
  column_id_column.AddConstraint(not_null_constraint);

  auto num_row_column =
      catalog::Column(integer_type, integer_type_size, "num_row", true);
  num_row_column.AddConstraint(not_null_constraint);
  auto cardinality_column =
      catalog::Column(decimal_type, decimal_type_size, "cardinality", true);
  cardinality_column.AddConstraint(not_null_constraint);
  auto frac_null_column =
      catalog::Column(decimal_type, decimal_type_size, "frac_null", true);
  frac_null_column.AddConstraint(not_null_constraint);

  auto most_common_vals_column =
      catalog::Column(varchar_type, varchar_type_size, "most_common_vals");

  auto most_common_freqs_column =
      catalog::Column(decimal_type, decimal_type_size, "most_common_freqs");

  auto histogram_bounds_column =
      catalog::Column(varchar_type, varchar_type_size, "histogram_bounds");

  std::unique_ptr<catalog::Schema> table_schema(new catalog::Schema(
      {database_id_column, table_id_column, column_id_column, num_row_column,
       cardinality_column, frac_null_column, most_common_vals_column,
       most_common_freqs_column, histogram_bounds_column}));
  return table_schema;
}

storage::DataTable *StatsStorage::GetStatsTable() {
  auto catalog = catalog::Catalog::GetInstance();
  storage::Database *catalog_db =
      catalog->GetDatabaseWithName(CATALOG_DATABASE_NAME);
  auto stats_table = catalog_db->GetTableWithName(STATS_TABLE_NAME);

  return stats_table;
}

void StatsStorage::AddOrUpdateTableStats(storage::DataTable *table,
                                         TableStats *table_stats) {
  // All tuples are inserted in a single txn
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  storage::DataTable *stats_table = GetStatsTable();

  oid_t database_id = table->GetDatabaseOid();
  oid_t table_id = table->GetOid();
  size_t num_row = table_stats->GetActiveTupleCount();

  oid_t column_count = table_stats->GetColumnCount();
  for (oid_t column_id = 0; column_id < column_count; column_id++) {
    ColumnStats *column_stats = table_stats->GetColumnStats(column_id);
    (void)column_stats;
    double cardinality = column_stats->GetCardinality();
    double frac_null = column_stats->GetFracNull();
    // TODO: Get most_common_vals, most_common_freqs and histogram_bounds from
    // column_stats.
    std::vector<ValueFrequencyPair> most_common_val_freqs =
        column_stats->GetCommonValueAndFrequency();
    std::vector<double> histogram_bounds = column_stats->GetHistogramBound();

    // Generate and insert the tuple.
    auto table_tuple = GetColumnStatsTuple(
        stats_table->GetSchema(), database_id, table_id, column_id, num_row,
        cardinality, frac_null, most_common_val_freqs, histogram_bounds);

    catalog::InsertTuple(stats_table, std::move(table_tuple), txn);
  }

  txn_manager.CommitTransaction(txn);
}

/**
 * Generate a column stats tuple.
 * TODO: deal with array type.
 */
std::unique_ptr<storage::Tuple> StatsStorage::GetColumnStatsTuple(
    const catalog::Schema *schema, oid_t database_id, oid_t table_id,
    oid_t column_id, int num_row, double cardinality, double frac_null,
    std::vector<ValueFrequencyPair> &most_common_val_freqs,
    std::vector<double> &histogram_bounds) {
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
  auto val_db_id = type::ValueFactory::GetIntegerValue(database_id);
  auto val_table_id = type::ValueFactory::GetIntegerValue(table_id);
  auto val_column_id = type::ValueFactory::GetIntegerValue(column_id);
  auto val_num_row = type::ValueFactory::GetIntegerValue(num_row);
  auto val_cardinality = type::ValueFactory::GetDecimalValue(cardinality);
  auto val_frac_null = type::ValueFactory::GetDecimalValue(frac_null);

  LOG_DEBUG("Most cmmon val count: %lu", most_common_val_freqs.size());
  // Currently, only store the most common value and its frequency
  // TODO: support array
  type::Value val_common_val, val_common_freq;
  if (most_common_val_freqs.size() > 0) {
    val_common_val = type::ValueFactory::GetVarcharValue(
        most_common_val_freqs[0].first.ToString());
    val_common_freq =
        type::ValueFactory::GetDecimalValue(most_common_val_freqs[0].second);
  } else {
    val_common_val =
        type::ValueFactory::GetNullValueByType(type::Type::VARCHAR);
    val_common_freq =
        type::ValueFactory::GetNullValueByType(type::Type::DECIMAL);
  }
  // Convert the double array to a string by concatening them with ","
  type::Value val_hist_bounds;
  if (histogram_bounds.size() > 0) {
    val_hist_bounds = type::ValueFactory::GetVarcharValue(
        ConvertDoubleArrayToString(histogram_bounds));
  } else {
    val_hist_bounds =
        type::ValueFactory::GetNullValueByType(type::Type::VARCHAR);
  }

  tuple->SetValue(0, val_db_id, nullptr);
  tuple->SetValue(1, val_table_id, nullptr);
  tuple->SetValue(2, val_column_id, nullptr);
  tuple->SetValue(3, val_num_row, nullptr);
  tuple->SetValue(4, val_cardinality, nullptr);
  tuple->SetValue(5, val_frac_null, nullptr);
  tuple->SetValue(6, val_common_val, pool_.get());
  tuple->SetValue(7, val_common_freq, nullptr);
  tuple->SetValue(8, val_hist_bounds, pool_.get());
  return std::move(tuple);
}

std::unique_ptr<ColumnStats> StatsStorage::GetColumnStatsByID(
    UNSUED_ATTRIBUTE oid_t database_id, UNSUED_ATTRIBUTE oid_t table_id,
    UNSUED_ATTRIBUTE oid_t column_id) {
  return nullptr;
}

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

void StatsStorage::CreateSamplesDatabase() {
  catalog::Catalog::GetInstance()->CreateDatabase(SAMPLES_DB_NAME, nullptr);
}

void StatsStorage::AddSamplesTable(
    storage::DataTable *data_table,
    std::vector<std::unique_ptr<storage::Tuple>> &sampled_tuples) {
  auto schema = data_table->GetSchema();
  auto schema_copy = catalog::Schema::CopySchema(schema);

  auto catalog = catalog::Catalog::GetInstance();
  auto samples_db = catalog->GetDatabaseWithName(SAMPLES_DB_NAME);
  auto samples_db_oid = samples_db->GetOid();
  bool own_schema = true;
  bool adapt_table = false;
  bool is_catalog = true;

  std::string samples_table_name = GenerateSamplesTableName(
      data_table->GetDatabaseOid(), data_table->GetOid());

  storage::DataTable *table = storage::TableFactory::GetDataTable(
      samples_db_oid, catalog->GetNextOid(), schema_copy, samples_table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table, is_catalog);
  samples_db->AddTable(table, true);

  for (auto &tuple : sampled_tuples) {
    catalog::InsertTuple(table, std::move(tuple), nullptr);
  }
}

void StatsStorage::GetTupleSamples(
    UNUSED_ATTRIBUTE std::vector<storage::Tuple> &tuple_samples) {}

void StatsStorage::GetColumnSamples(
    UNUSED_ATTRIBUTE std::vector<type::Value> &column_samples) {}

} /* namespace optimizer */
} /* namespace peloton */
