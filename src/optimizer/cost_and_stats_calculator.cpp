//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost_and_stats_calculator.h
//
// Identification: src/include/optimizer/cost_and_stats_calculator.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/stats/stats_storage.h"
#include "optimizer/cost_and_stats_calculator.h"
#include "optimizer/column_manager.h"
#include "optimizer/stats.h"
#include "optimizer/properties.h"
#include "optimizer/stats/column_stats.h"
#include "optimizer/stats/cost.h"
#include "type/types.h"

namespace peloton {
  namespace optimizer {
    //===----------------------------------------------------------------------===//
    // Helper functions
    //===----------------------------------------------------------------------===//
    // Generate output stats based on the input stats and the property column
    std::shared_ptr<TableStats> generateOutputStat(std::shared_ptr<TableStats>& input_table_stats,
                                                   const PropertyColumns* columns_prop) {
      std::vector<std::shared_ptr<ColumnStats>> output_column_stats;
      if (columns_prop->HasStarExpression()) {
        for (size_t i = 0; i < input_table_stats->GetColumnCount(); i++) {
          output_column_stats.push_back(input_table_stats->GetColumnStats((oid_t)i));
        }
      } else {
        for (size_t i = 0; i < columns_prop->GetSize(); i++) {
          LOG_DEBUG("column expr %s", columns_prop->GetColumn(i)->GetInfo().c_str());
          auto expr = columns_prop->GetColumn(i);
          if (expr->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
            auto column_name = (std::dynamic_pointer_cast<expression::TupleValueExpression>(expr))->GetColumnName();
            auto column_stat = input_table_stats->GetColumnStats(column_name);
            if (column_stat != nullptr) {
              output_column_stats.push_back(column_stat);
            };
          } else {

          }

        }
      }
      auto output_table_stats = std::make_shared<TableStats>(input_table_stats->num_rows, output_column_stats);
      return output_table_stats;
    }

    // Update output stats num_rows for conjunctions based on predicate
    double updateMultipleConjuctionStats(const std::shared_ptr<TableStats> &input_stats,
                                         const expression::AbstractExpression *expr,
                                         std::shared_ptr<TableStats> &output_stats, bool enable_index) {
      if (expr->GetChild(0)->GetExpressionType() == ExpressionType::VALUE_TUPLE ||
          expr->GetChild(1)->GetExpressionType() == ExpressionType::VALUE_TUPLE) {

        int right_index = expr->GetChild(0)->GetExpressionType() == ExpressionType::VALUE_TUPLE?1:0;
        auto left_expr = reinterpret_cast<const expression::TupleValueExpression *>(
          right_index == 1? expr->GetChild(0):expr->GetChild(1));
        auto expr_type = expr->GetExpressionType();
        if (right_index == 0) {
          if (expr_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
            expr_type = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
          } else if (expr_type == ExpressionType::COMPARE_LESSTHAN) {
            expr_type = ExpressionType::COMPARE_GREATERTHAN;
          } else if (expr_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
            expr_type = ExpressionType::COMPARE_LESSTHANOREQUALTO;
          } else if (expr_type == ExpressionType::COMPARE_GREATERTHAN) {
            expr_type = ExpressionType::COMPARE_LESSTHAN;
          }
        }

        auto column_id = left_expr->GetColumnId();

        type::Value value;
        if (expr->GetChild(right_index)->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
          value = reinterpret_cast<expression::ConstantValueExpression*>(
            expr->GetModifiableChild(right_index))->GetValue();
        } else {
          value = type::ValueFactory::GetParameterOffsetValue(
            reinterpret_cast<expression::ParameterValueExpression*>(
              expr->GetModifiableChild(right_index))->GetValueIdx()).Copy();
        }
        ValueCondition condition(column_id, expr_type, value);
        if (enable_index) {
          return Cost::SingleConditionIndexScanCost(input_stats, condition, output_stats);
        } else {
          return Cost::SingleConditionSeqScanCost(input_stats, condition, output_stats);
        }

      } else {
        auto lhs = std::make_shared<TableStats>();
        auto rhs = std::make_shared<TableStats>();
        double left_cost = updateMultipleConjuctionStats(input_stats, expr->GetChild(0), lhs, enable_index);
        double right_cost = updateMultipleConjuctionStats(input_stats, expr->GetChild(1), rhs, enable_index);

        Cost::CombineConjunctionStats(lhs, rhs, input_stats->num_rows, expr->GetExpressionType(), output_stats);

        if (enable_index) {
          return left_cost+right_cost;
        } else {
          return left_cost;
        }
      }

    }

    double getCostOfChildren(std::vector<double>& child_costs) {
      double cost = 0;
      for (size_t i = 0; i < child_costs.size(); i++) {
        cost += child_costs[i];
      }
      return cost;
    }


    void CostAndStatsCalculator::CalculateCostAndStats(
      std::shared_ptr<GroupExpression> gexpr,
      const PropertySet *output_properties,
      const std::vector<PropertySet> *input_properties_list,
      std::vector<std::shared_ptr<Stats>> child_stats,
      std::vector<double> child_costs) {
      gexpr_ = gexpr;
      output_properties_ = output_properties;
      input_properties_list_ = input_properties_list;
      child_stats_ = child_stats;
      child_costs_ = child_costs;

      gexpr->Op().Accept(this);
    }

    void CostAndStatsCalculator::Visit(const DummyScan *) {
      // TODO: Replace with more accurate cost
      output_stats_.reset(new Stats(nullptr));
      output_cost_ = 0;
    }

    void CostAndStatsCalculator::Visit(const PhysicalSeqScan * op) {
      // TODO : Replace with more accurate cost
      // retrieve table_stats from catalog by db_id and table_id
      output_cost_ = getCostOfChildren(child_costs_);

      auto stats_storage = StatsStorage::GetInstance();
      auto table_stats =
        stats_storage->GetTableStats(op->table_->GetDatabaseOid(), op->table_->GetOid());

      // No table stats available
      if (table_stats->GetColumnCount() == 0) {
        output_stats_.reset(new Stats(nullptr));
        output_cost_ = 1;
        return;
      }

      auto property_ = output_properties_->GetPropertyOfType(PropertyType::COLUMNS)->As<PropertyColumns>();
      auto output_stats = generateOutputStat(table_stats, property_);
      auto predicate_prop =
        output_properties_->GetPropertyOfType(PropertyType::PREDICATE)
          ->As<PropertyPredicate>();
      if (predicate_prop == nullptr) {
        output_cost_ += Cost::NoConditionSeqScanCost(table_stats);
        output_stats_ = output_stats;
        return;
      }

      expression::AbstractExpression *predicate = predicate_prop->GetPredicate();
      output_cost_ += updateMultipleConjuctionStats(table_stats, predicate, output_stats, false);
      output_stats_ = output_stats;
    };

    void CostAndStatsCalculator::Visit(const PhysicalIndexScan *op) {
      // Simple cost function
      // indexSearchable ? Index : SeqScan
      // TODO : Replace with more accurate cost
      output_cost_ = getCostOfChildren(child_costs_);
      auto predicate_prop =
        output_properties_->GetPropertyOfType(PropertyType::PREDICATE)
          ->As<PropertyPredicate>();

      auto stats_storage = StatsStorage::GetInstance();
      auto table_stats = std::dynamic_pointer_cast<TableStats>(
        stats_storage->GetTableStats(op->table_->GetDatabaseOid(), op->table_->GetOid()));

      std::vector<oid_t> key_column_ids;
      std::vector<ExpressionType> expr_types;
      std::vector<type::Value> values;
      oid_t index_id = 0;

      expression::AbstractExpression *predicate = predicate_prop == nullptr? nullptr:predicate_prop->GetPredicate();
      bool index_searchable = predicate == nullptr? false :
                              util::CheckIndexSearchable(
                                op->table_, predicate, key_column_ids, expr_types, values, index_id);
      // No table stats available
      if (table_stats->GetColumnCount() == 0) {
        output_stats_.reset(new Stats(nullptr));
        if (index_searchable) {
          output_cost_ = 0;
        } else {
          output_cost_ = 2;
        }
        return;
      }

      auto property_ = output_properties_->GetPropertyOfType(PropertyType::COLUMNS)->As<PropertyColumns>();
      auto output_stats = generateOutputStat(table_stats, property_);

      if (predicate_prop == nullptr) {
        output_cost_ += Cost::NoConditionSeqScanCost(table_stats);
      } else {
        if (index_searchable) {
          output_cost_ += updateMultipleConjuctionStats(table_stats, predicate, output_stats, true);
        } else {
          output_cost_ += updateMultipleConjuctionStats(table_stats, predicate, output_stats, false);
        }
      }
      output_stats_ = output_stats;
    }


    void CostAndStatsCalculator::Visit(const PhysicalProject *) {
      // TODO: Replace with more accurate cost
      output_cost_ = getCostOfChildren(child_costs_);
      PL_ASSERT(child_stats_.size() == 1);
      auto table_stats_ptr = std::dynamic_pointer_cast<TableStats>(child_stats_.at(0));
      if (table_stats_ptr == nullptr || table_stats_ptr->GetColumnCount() == 0) {
        output_stats_.reset(new Stats(nullptr));
        output_cost_ = 0;
        return;
      }
      auto property_ = output_properties_->GetPropertyOfType(PropertyType::COLUMNS)->As<PropertyColumns>();
      auto output_stats = generateOutputStat(table_stats_ptr, property_);

      output_cost_ += Cost::ProjectCost(std::dynamic_pointer_cast<TableStats>(child_stats_.at(0)),
                                        std::vector <oid_t>(), output_stats);
      output_stats_ = output_stats;
    }
    void CostAndStatsCalculator::Visit(const PhysicalOrderBy *) {
      // TODO: Replace with more accurate cost
      LOG_DEBUG("order by finish");
      PL_ASSERT(child_stats_.size() == 1);
      output_cost_ = getCostOfChildren(child_costs_);
      auto table_stats_ptr = std::dynamic_pointer_cast<TableStats>(child_stats_.at(0));
      if (table_stats_ptr == nullptr || table_stats_ptr->GetColumnCount() == 0) {
        output_cost_ = 0;
        return;
      }
      LOG_DEBUG("order by table stats rows %zu", table_stats_ptr->num_rows);
      auto property_ = output_properties_->GetPropertyOfType(PropertyType::COLUMNS)->As<PropertyColumns>();
      auto output_stats = generateOutputStat(table_stats_ptr, property_);
      auto sort_prop = std::dynamic_pointer_cast<PropertySort>(
        output_properties_->GetPropertyOfType(PropertyType::SORT));
      std::vector<std::string> columns;
      std::vector<bool> orders;
      for(size_t i = 0; i < sort_prop->GetSortColumnSize(); i++) {
        auto expr = sort_prop->GetSortColumn(i);
        if (expr->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
          auto column = std::dynamic_pointer_cast<expression::TupleValueExpression>(
            sort_prop->GetSortColumn(i))->GetColumnName();

          // In case the first column is not a tuple expression
          if (i != 0 && columns.empty()) {
            columns.push_back(column);
            orders.push_back(false);
          }

          columns.push_back(column);
          orders.push_back(sort_prop->GetSortAscending(i));
        }
      }
      output_cost_ += Cost::OrderByCost(table_stats_ptr, columns, orders, output_stats);
      output_stats_ = output_stats;
      LOG_DEBUG("order by output stats rows %zu cost %f", output_stats->num_rows, output_cost_);
    }

    void CostAndStatsCalculator::Visit(const PhysicalLimit *) {
      // TODO: Replace with more accurate cost
      // lack of limit number
      output_cost_ = getCostOfChildren(child_costs_);
      PL_ASSERT(child_stats_.size() == 1);
      auto table_stats_ptr = std::dynamic_pointer_cast<TableStats>(child_stats_.at(0));
      if (table_stats_ptr == nullptr || table_stats_ptr->GetColumnCount() == 0) {
        output_cost_ = 0;
        return;
      }
      auto property_ = output_properties_->GetPropertyOfType(PropertyType::COLUMNS)->As<PropertyColumns>();
      auto output_stats = generateOutputStat(table_stats_ptr, property_);

      size_t limit = (size_t) std::dynamic_pointer_cast<PropertyLimit>(
        output_properties_->GetPropertyOfType(PropertyType::LIMIT))->GetLimit();
      output_cost_ += Cost::LimitCost(std::dynamic_pointer_cast<TableStats>(child_stats_.at(0)),limit, output_stats);
      output_stats_ = output_stats;
    }
    void CostAndStatsCalculator::Visit(const PhysicalFilter *){};
    void CostAndStatsCalculator::Visit(const PhysicalInnerNLJoin *){
      // TODO: Replace with more accurate cost
      output_cost_ = 1;
    };
    void CostAndStatsCalculator::Visit(const PhysicalLeftNLJoin *){};
    void CostAndStatsCalculator::Visit(const PhysicalRightNLJoin *){};
    void CostAndStatsCalculator::Visit(const PhysicalOuterNLJoin *){};
    void CostAndStatsCalculator::Visit(const PhysicalInnerHashJoin *){
      // TODO: Replace with more accurate cost
      output_cost_ = 0;
    };
    void CostAndStatsCalculator::Visit(const PhysicalLeftHashJoin *){};
    void CostAndStatsCalculator::Visit(const PhysicalRightHashJoin *){};
    void CostAndStatsCalculator::Visit(const PhysicalOuterHashJoin *){};
    void CostAndStatsCalculator::Visit(const PhysicalInsert *) {
      // TODO: Replace with more accurate cost
      output_cost_ = 0;
    };
    void CostAndStatsCalculator::Visit(const PhysicalInsertSelect *) {
      // TODO: Replace with more accurate cost
      output_cost_ = 0;
    };
    void CostAndStatsCalculator::Visit(const PhysicalDelete *) {
      // TODO: Replace with more accurate cost
      output_cost_ = 0;
    };
    void CostAndStatsCalculator::Visit(const PhysicalUpdate *) {
      // TODO: Replace with more accurate cost
      output_cost_ = 0;
    };
    void CostAndStatsCalculator::Visit(const PhysicalHashGroupBy * op) {
      // TODO: Replace with more accurate cost
      LOG_DEBUG("hash group by start");
      output_cost_ = getCostOfChildren(child_costs_);
      LOG_DEBUG("hash child cost %f", output_cost_);
      PL_ASSERT(child_stats_.size() == 1);
      auto table_stats_ptr = std::dynamic_pointer_cast<TableStats>(child_stats_.at(0));
      if (table_stats_ptr == nullptr || table_stats_ptr->GetColumnCount() == 0) {
        output_cost_ = 0;
        return;
      }

      auto property_ = output_properties_->GetPropertyOfType(PropertyType::COLUMNS)->As<PropertyColumns>();
      auto output_stats = generateOutputStat(table_stats_ptr, property_);

      std::vector<std::string> column_names;
//      std::vector<oid_t> column_ids;
      for (auto column: op->columns) {
        if (column->GetExpressionType() == ExpressionType::VALUE_TUPLE) {

          std::string column_name = std::dynamic_pointer_cast<expression::TupleValueExpression>(
            column)->GetColumnName();
          auto column_id = std::dynamic_pointer_cast<expression::TupleValueExpression>(
            column)->GetColumnId();

          LOG_DEBUG("hash group by column, %s, %d", column_name.c_str(), column_id);
          column_names.push_back(column_name);
//          column_ids.push_back((oid_t) column_id);
//          LOG_DEBUG("column id %zu", table_stats_ptr->)
//          LOG_DEBUG("hash group by column, %s", column_name.c_str());
//          column_names.push_back(column_name);
        }
      }
      output_cost_ += Cost::HashGroupByCost(table_stats_ptr, column_names, output_stats);
      output_stats_ = output_stats;

      LOG_DEBUG("hash group by cost %f", output_cost_);
      LOG_DEBUG("hash group by output rows %zu column size %zu", output_stats->num_rows, output_stats->GetColumnCount());
    };
    void CostAndStatsCalculator::Visit(const PhysicalSortGroupBy * op) {

      // TODO: Replace with more accurate cost
      LOG_DEBUG("sort group by start");
      output_cost_ = getCostOfChildren(child_costs_);
      LOG_DEBUG("child cost %f", output_cost_);
      PL_ASSERT(child_stats_.size() == 1);
      auto table_stats_ptr = std::dynamic_pointer_cast<TableStats>(child_stats_.at(0));
      if (table_stats_ptr == nullptr || table_stats_ptr->GetColumnCount() == 0) {
        output_cost_ = 0;
        return;
      }

      auto property_ = output_properties_->GetPropertyOfType(PropertyType::COLUMNS)->As<PropertyColumns>();
      auto output_stats = generateOutputStat(table_stats_ptr, property_);

      std::vector<std::string> column_names;
//      std::vector<oid_t> column_ids;
      for (auto column: op->columns) {
        if (column->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
          std::string column_name = std::dynamic_pointer_cast<expression::TupleValueExpression>(
            column)->GetColumnName();
          auto column_id = std::dynamic_pointer_cast<expression::TupleValueExpression>(
            column)->GetColumnId();

          LOG_DEBUG("sort group by column, %s, %d", column_name.c_str(), column_id);
          column_names.push_back(column_name);
//          column_ids.push_back((oid_t) column_id);
        }
      }
      output_cost_ += Cost::SortGroupByCost(table_stats_ptr, column_names, output_stats);

      output_stats_ = output_stats;
      LOG_DEBUG("sort group by cost %f", output_cost_);

      LOG_DEBUG("sort group by output rows %zu %zu", output_stats->num_rows, output_stats->GetColumnCount());

    };
    void CostAndStatsCalculator::Visit(const PhysicalAggregate *) {

      LOG_DEBUG("aggregate");
      // TODO: Replace with more accurate cost
      output_cost_ = getCostOfChildren(child_costs_);
      PL_ASSERT(child_stats_.size() == 1);
      auto table_stats_ptr = std::dynamic_pointer_cast<TableStats>(child_stats_.at(0));
      if (table_stats_ptr == nullptr || table_stats_ptr->GetColumnCount() == 0) {
        output_cost_ = 0;
        return;
      }
      auto property_ = output_properties_->GetPropertyOfType(PropertyType::COLUMNS)->As<PropertyColumns>();
      auto output_stats = generateOutputStat(table_stats_ptr, property_);
      output_cost_ += Cost::AggregateCost(table_stats_ptr);
      output_stats_ = output_stats;
      LOG_DEBUG("aggregate cost %f column number %zu", output_cost_, output_stats->GetColumnCount());
    };
    void CostAndStatsCalculator::Visit(const PhysicalDistinct *) {

      // TODO: Replace with more accurate cost
      output_cost_ = getCostOfChildren(child_costs_);
      PL_ASSERT(child_stats_.size() == 1);
      auto table_stats_ptr = std::dynamic_pointer_cast<TableStats>(child_stats_.at(0));
      if (table_stats_ptr == nullptr || table_stats_ptr->GetColumnCount() == 0) {
        output_cost_ = 0;
        return;
      }
      auto property_ = output_properties_->GetPropertyOfType(PropertyType::COLUMNS)->As<PropertyColumns>();
      auto output_stats = generateOutputStat(table_stats_ptr, property_);

      auto distinct_prop = output_properties_->GetPropertyOfType(PropertyType::DISTINCT)->As<PropertyDistinct>();
      if (distinct_prop->GetSize() == 1 &&
        distinct_prop->GetDistinctColumn(0)->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
        auto tv_expr = std::dynamic_pointer_cast<expression::TupleValueExpression>(distinct_prop->GetDistinctColumn(0));
        oid_t column_id = table_stats_ptr->GetColumnStats(tv_expr->GetColumnName())->column_id;
        output_cost_ += Cost::DistinctCost(table_stats_ptr, column_id, output_stats);
      }
      output_stats_ = output_stats;
    };

  }  // namespace optimizer
}  // namespace peloton