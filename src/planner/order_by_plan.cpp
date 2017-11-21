//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// order_by_plan.cpp
//
// Identification: src/planner/order_by_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "planner/order_by_plan.h"

#include "expression/abstract_expression.h"

namespace peloton {
namespace planner {

OrderByPlan::OrderByPlan(const std::vector<oid_t> &sort_keys,
                         const std::vector<bool> &descend_flags,
                         const std::vector<oid_t> &output_column_ids)
    : sort_keys_(sort_keys),
      descend_flags_(descend_flags),
      output_column_ids_(output_column_ids) {}

void OrderByPlan::PerformBinding(BindingContext &binding_context) {
  // Let the child do its binding first
  BindingContext input_context;

  // TODO(boweic): In the optimizer we decide the output column and input column
  // may be different, so we need to redo the binding here for now. We may want
  // to discuss if we should keep the input and output column of order by
  // unchanged
  const auto &children = GetChildren();
  PL_ASSERT(children.size() == 1);

  children[0]->PerformBinding(input_context);
  oid_t idx = 0;
  for (const oid_t col_id : GetOutputColumnIds()) {
    auto *ai = input_context.Find(col_id);
    LOG_DEBUG("Dest col %u binds to AI %p", col_id, ai);
    PL_ASSERT(ai != nullptr);
    output_ais_.push_back(ai);
    binding_context.BindNew(idx++, ai);
  }

  for (const oid_t sort_key_col_id : GetSortKeys()) {
    auto *ai = input_context.Find(sort_key_col_id);
    LOG_DEBUG("Sort col %u binds to AI %p", sort_key_col_id, ai);
    PL_ASSERT(ai != nullptr);
    sort_key_ais_.push_back(ai);
  }
}

}  // namespace planner
}  // namespace peloton
