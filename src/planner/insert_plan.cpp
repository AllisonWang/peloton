//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// insert_plan.cpp
//
// Identification: src/planner/insert_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/insert_plan.h"

#include "catalog/catalog.h"
#include "expression/constant_value_expression.h"
#include "storage/data_table.h"
#include "type/ephemeral_pool.h"
#include "type/value_factory.h"

namespace peloton {
namespace planner {

InsertPlan::InsertPlan(storage::DataTable *table,
    const std::vector<std::string> *columns,
    const std::vector<std::vector<
        std::unique_ptr<expression::AbstractExpression>>> *insert_values)
    : target_table_(table), bulk_insert_count_(insert_values->size()) {
  LOG_TRACE("Creating an Insert Plan with multiple expressions");
  PL_ASSERT(target_table_);
  parameter_vector_.reset(new std::vector<std::tuple<oid_t, oid_t, oid_t>>());
  params_value_type_.reset(new std::vector<type::TypeId>);

  auto *schema = target_table_->GetSchema();
  // INSERT INTO table_name VALUES (val2, val2, ...)
  if (columns->empty()) {
    for (uint32_t tuple_idx = 0; tuple_idx < insert_values->size();
         tuple_idx++) {
      auto &values = (*insert_values)[tuple_idx];
      PL_ASSERT(values.size() <= schema->GetColumnCount());
      std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
      int column_id = 0, param_idx = 0;
      for (uint32_t idx = 0; idx < values.size(); idx++, column_id++) {
        auto &exp = values[idx];
        if (exp == nullptr) {
          type::Value *v = schema->GetDefaultValue(column_id);
          if (v == nullptr)
            tuple->SetValue(column_id, type::ValueFactory::GetNullValueByType(
                schema->GetColumn(column_id).GetType()), nullptr);
          else
            tuple->SetValue(column_id, *v, nullptr);
        } else if (exp->GetExpressionType() ==
                   ExpressionType::VALUE_PARAMETER) {
          std::tuple<oid_t, oid_t, oid_t> pair =
              std::make_tuple(tuple_idx, column_id, param_idx++);
          parameter_vector_->push_back(pair);
          params_value_type_->push_back(
              schema->GetColumn(column_id).GetType());
        } else {
          PL_ASSERT(exp->GetExpressionType() == ExpressionType::VALUE_CONSTANT);
          auto *const_exp =
              dynamic_cast<expression::ConstantValueExpression *>(exp.get());
          type::Value value = const_exp->GetValue();
          auto type = const_exp->GetValueType();
          type::AbstractPool *data_pool = nullptr;
          if (type == type::TypeId::VARCHAR || type == type::TypeId::VARBINARY)
            data_pool = GetPlanPool();
          tuple->SetValue(column_id, value, data_pool);
        }
      }
      tuples_.push_back(std::move(tuple));
    }
  }
  // INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...);
  else {
    PL_ASSERT(columns->size() <= schema->GetColumnCount());
    for (uint32_t tuple_idx = 0; tuple_idx < insert_values->size();
         tuple_idx++) {
      auto &values = (*insert_values)[tuple_idx];
      std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
      auto &table_columns = schema->GetColumns();
      auto columns_cnt = columns->size();
      int param_idx = 0;

      // Update parameter info in the specified columns order
      for (size_t pos = 0; pos < columns_cnt; pos++) {
        auto column_id = schema->GetColumnID(columns->at(pos));
        PL_ASSERT(column_id != INVALID_OID);

        auto type = schema->GetColumn(column_id).GetType();
        type::AbstractPool *data_pool = nullptr;
        if (type == type::TypeId::VARCHAR || type == type::TypeId::VARBINARY)
          data_pool = GetPlanPool();

        LOG_TRACE("Column %d found in INSERT, ExpressionType: %s", column_id,
                  ExpressionTypeToString(values.at(pos)->GetExpressionType())
                      .c_str());

        if (values.at(pos)->GetExpressionType() ==
            ExpressionType::VALUE_PARAMETER) {
          std::tuple<oid_t, oid_t, oid_t> pair =
              std::make_tuple(tuple_idx, column_id, param_idx++);
          parameter_vector_->push_back(pair);
          params_value_type_->push_back(schema->GetColumn(column_id).GetType());
        } else {
          expression::ConstantValueExpression *const_exp =
              dynamic_cast<expression::ConstantValueExpression *>(
                  values.at(pos).get());
          tuple->SetValue(column_id, const_exp->GetValue(), data_pool);
        }
      }

      // Insert a null value for non-specified columns
      auto table_columns_cnt = schema->GetColumnCount();
      if (columns_cnt < table_columns_cnt) {
        for (size_t column_id = 0; column_id < table_columns_cnt; column_id++) {
          auto col = table_columns[column_id];
          if (std::find_if(columns->begin(), columns->end(),
                  [&col](const std::string &x) { return col.GetName() == x; })
              == columns->end()) {
            type::Value *v = schema->GetDefaultValue(column_id);
            if (v == nullptr)
              tuple->SetValue(column_id, type::ValueFactory::GetNullValueByType(
                  col.GetType()), nullptr);
            else
              tuple->SetValue(column_id, *v, nullptr);
          }
        }
      }
      LOG_TRACE("Tuple to be inserted: %s", tuple->GetInfo().c_str());
      tuples_.push_back(std::move(tuple));
    }
  }
}

type::AbstractPool *InsertPlan::GetPlanPool() {
  if (pool_.get() == nullptr)
    pool_.reset(new type::EphemeralPool());
  return pool_.get();
}

void InsertPlan::SetParameterValues(std::vector<type::Value> *values) {
  LOG_TRACE("Set Parameter Values in Insert");
  PL_ASSERT(values->size() == parameter_vector_->size());
  for (unsigned int i = 0; i < values->size(); i++) {
    auto type = params_value_type_->at(i);
    auto &param_info = parameter_vector_->at(i);
    auto tuple_idx = std::get<0>(param_info);
    auto column_id = std::get<1>(param_info);
    auto param_idx = std::get<2>(param_info);
    type::Value value = values->at(param_idx).CastAs(type);
    if (type == type::TypeId::VARCHAR || type == type::TypeId::VARBINARY)
      tuples_[tuple_idx]->SetValue(column_id, value, GetPlanPool());
    else
      tuples_[tuple_idx]->SetValue(column_id, value);
  }
}

}  // namespace planner
}  // namespace peloton
