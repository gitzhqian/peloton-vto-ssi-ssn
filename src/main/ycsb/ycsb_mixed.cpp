//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_mixed.cpp
//
// Identification: src/main/ycsb/ycsb_mixed.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <include/planner/aggregate_plan.h>
#include <include/planner/limit_plan.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <ctime>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "benchmark/ycsb/ycsb_configuration.h"
#include "benchmark/ycsb/ycsb_loader.h"
#include "benchmark/ycsb/ycsb_workload.h"
#include "catalog/manager.h"
#include "catalog/schema.h"
#include "common/container_tuple.h"
#include "common/generator.h"
#include "common/logger.h"
#include "common/timer.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/abstract_executor.h"
#include "executor/executor_context.h"
#include "executor/index_scan_executor.h"
#include "executor/insert_executor.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "executor/materialization_executor.h"
#include "executor/update_executor.h"
#include "expression/abstract_expression.h"
#include "expression/comparison_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/expression_util.h"
#include "expression/tuple_value_expression.h"
#include "include/executor/hybrid_scan_executor.h"
#include "include/planner/hybrid_scan_plan.h"
#include "index/index_factory.h"
#include "logging/log_manager.h"
#include "planner/abstract_plan.h"
#include "planner/index_scan_plan.h"
#include "planner/insert_plan.h"
#include "planner/materialization_plan.h"
#include "planner/update_plan.h"
#include "storage/data_table.h"
#include "storage/table_factory.h"
#include "type/types.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace peloton {
namespace benchmark {
namespace ycsb {
//
//static int GetLowerBound() {
//  int tuple_count = state.scale_factor * state.tuples_per_tilegroup;
//  int predicate_offset = 0.1 * tuple_count;
//
//  LOG_TRACE("Tuple count : %d", tuple_count);
//
//  int lower_bound = predicate_offset;
//  return lower_bound;
//}
//
//static int GetUpperBound() {
//  int tuple_count = state.scale_factor * state.tuples_per_tilegroup;
//  int selected_tuple_count = state.selectivity * tuple_count;
//  int predicate_offset = 0.1 * tuple_count;
//
//  int upper_bound = predicate_offset + selected_tuple_count;
//  return upper_bound;
//}
static expression::AbstractExpression *CreateSimpleScanPredicate(
    oid_t key_attr, ExpressionType expression_type, oid_t constant) {
  // First, create tuple value expression.
  oid_t left_tuple_idx = 0;
  expression::AbstractExpression *tuple_value_expr_left =
      expression::ExpressionUtil::TupleValueFactory(type::Type::INTEGER,
                                                    left_tuple_idx, key_attr);

  // Second, create constant value expression.
  type::Value constant_value_left;
  if(state.key_string_mode){
    std::string constant_str = eml_keys[constant];
    constant_value_left =
        type::ValueFactory::GetVarcharValue(constant_str);
  }else{
//    constant_value_left =
//        type::ValueFactory::GetIntegerValue(constant);
    constant_value_left =
        type::ValueFactory::GetVarcharValue(std::to_string(constant));
  }

  expression::AbstractExpression *constant_value_expr_left =
      expression::ExpressionUtil::ConstantValueFactory(constant_value_left);

  // Finally, link them together using an greater than expression.
  expression::AbstractExpression *predicate =
      expression::ExpressionUtil::ComparisonFactory(
          expression_type, tuple_value_expr_left, constant_value_expr_left);

  return predicate;
}
static int GetCurrentTupleCount(const size_t thread_id) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  concurrency::Transaction *txn = txn_manager.BeginTransaction(thread_id);

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  std::vector<expression::AbstractExpression *> runtime_keys;
  // Column ids to be added to logical tile.
  std::vector<oid_t> column_ids;
  oid_t column_count = state.column_count + 1;

  // read all the attributes in a tuple.
  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(col_itr);
  }
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  key_column_ids.push_back(0);
  expr_types.push_back(ExpressionType::COMPARE_GREATERTHANOREQUALTO);

  /////////////////////////////////////////////////////////
  // PERFORM READ
  /////////////////////////////////////////////////////////

  // set up parameter values
  std::vector<type::Value> values;

  auto lookup_key = 0;

//  values.push_back(type::ValueFactory::GetIntegerValue(lookup_key).Copy());
  values.push_back(type::ValueFactory::GetVarcharValue(std::to_string(lookup_key)).Copy());

  auto ycsb_pkey_index =
      user_table->GetIndexWithOid(user_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      ycsb_pkey_index, key_column_ids, expr_types, values, runtime_keys);

  // Create plan node.
  auto predicate = nullptr;

  planner::IndexScanPlan index_scan_node(user_table, predicate, column_ids,
                                         index_scan_desc);

  // Run the executor
  executor::IndexScanExecutor index_scan_executor(&index_scan_node,
                                                  context.get());

  auto ret = ExecuteRead(&index_scan_executor);
  int current_count = ret.size();

  if (txn->GetResult() != ResultType::SUCCESS) {
    txn_manager.AbortTransaction(txn);
    return false;
  }
  return current_count;
}
/**
 * @brief Create the scan predicate given a set of attributes. The predicate
 * will be attr >= LOWER_BOUND AND attr < UPPER_BOUND.
 * LOWER_BOUND and UPPER_BOUND are determined by the selectivity config.
 */
static expression::AbstractExpression *CreateScanPredicate(
    std::vector<oid_t> key_attrs) {
//  const int tuple_start_offset = GetLowerBound();
//  const int tuple_end_offset = GetUpperBound();
  int tuple_count = state.scale_factor * state.tuples_per_tilegroup;
//  int selected_tuple_count = state.selectivity * tuple_count;
  int selected_tuple_count = 1;

  tuple_count = user_table->GetTupleCount();
//  LOG_INFO("user count = %lu", user_table->GetTupleCount());

  ZipfDistribution zipf((tuple_count) - 1, state.zipf_theta);

  const int tuple_start_offset = zipf.GetNextNumber();
  const int tuple_end_offset = tuple_start_offset + selected_tuple_count;

//  LOG_DEBUG("Lower bound : %d", tuple_start_offset);
//  LOG_DEBUG("Upper bound : %d", tuple_end_offset);

  expression::AbstractExpression *predicate = nullptr;

  // Go over all key_attrs
  for (auto key_attr : key_attrs) {
    // ATTR >= LOWER_BOUND && < UPPER_BOUND

    expression::AbstractExpression *left_predicate = nullptr;
    expression::AbstractExpression *right_predicate = nullptr;
    expression::AbstractExpression *attr_predicate = nullptr;

    if (state.key_string_mode){

      left_predicate = CreateSimpleScanPredicate(
          key_attr, ExpressionType::COMPARE_GREATERTHANOREQUALTO,
          tuple_start_offset);
      attr_predicate = left_predicate;
    }else{
       left_predicate = CreateSimpleScanPredicate(
          key_attr, ExpressionType::COMPARE_GREATERTHANOREQUALTO,
          tuple_start_offset);

      right_predicate = CreateSimpleScanPredicate(
          key_attr, ExpressionType::COMPARE_LESSTHAN, tuple_end_offset);

      attr_predicate =
          expression::ExpressionUtil::ConjunctionFactory(
              ExpressionType::CONJUNCTION_AND, left_predicate, right_predicate);
    }

    // Build complex predicate
    if (predicate == nullptr) {
      predicate = attr_predicate;
    } else {
      // Join predicate with given attribute predicate
      predicate = expression::ExpressionUtil::ConjunctionFactory(
          ExpressionType::CONJUNCTION_AND, predicate, attr_predicate);
    }

  }

  return predicate;
}
static void CreateIndexScanPredicate(std::vector<oid_t> key_attrs,
                                     std::vector<oid_t> &key_column_ids,
                                     std::vector<ExpressionType> &expr_types,
                                     std::vector<type::Value> &values) {
//  const int tuple_start_offset = GetLowerBound();
//  const int tuple_end_offset = GetUpperBound();
  int tuple_count = state.scale_factor * state.tuples_per_tilegroup;
//  int selected_tuple_count = state.selectivity * tuple_count;
  int selected_tuple_count = 100;

  tuple_count =  user_table->GetTupleCount();

  ZipfDistribution zipf((state.scale_factor * state.tuples_per_tilegroup) - 1,
                        state.zipf_theta);

  const int tuple_start_offset = zipf.GetNextNumber();
  const int tuple_end_offset = tuple_start_offset + selected_tuple_count;

  // Go over all key_attrs
  for (auto key_attr : key_attrs) {
    key_column_ids.push_back(key_attr);
    expr_types.push_back(ExpressionType::COMPARE_GREATERTHANOREQUALTO);
//    type::Value vl0 = type::ValueFactory::GetIntegerValue(tuple_start_offset);
    type::Value vl0 = type::ValueFactory::GetVarcharValue(std::to_string(tuple_start_offset));
    values.push_back(vl0);

    key_column_ids.push_back(key_attr);
    expr_types.push_back(ExpressionType::COMPARE_LESSTHAN);
//    type::Value vl1 = type::ValueFactory::GetIntegerValue(tuple_end_offset);
    type::Value vl1 = type::ValueFactory::GetVarcharValue(std::to_string(tuple_end_offset));
    values.push_back(vl1);
  }
}
static void CreateSimpleIndexScanPredicate(std::vector<oid_t> key_attrs,
                                     std::vector<oid_t> &key_column_ids,
                                     std::vector<ExpressionType> &expr_types,
                                     std::vector<type::Value> &values) {

  ZipfDistribution zipf((state.scale_factor * state.tuples_per_tilegroup) - 1,
                        state.zipf_theta);

  const int tuple_start_offset = zipf.GetNextNumber();

  // Go over all key_attrs
  for (auto key_attr : key_attrs) {
      key_column_ids.push_back(key_attr);
      expr_types.push_back(ExpressionType::COMPARE_GREATERTHANOREQUALTO);
      if (state.key_string_mode){
        std::string val_str = eml_keys[tuple_start_offset];
        type::Value vl0 = type::ValueFactory::GetVarcharValue(val_str);
        values.push_back(vl0);
      }else{
//        type::Value vl0 = type::ValueFactory::GetIntegerValue(tuple_start_offset);
        type::Value vl0 = type::ValueFactory::GetVarcharValue(std::to_string(tuple_start_offset));
        values.push_back(vl0);
      }
  }
}

/**
 * Test YCSB scan/insert
 * @param thread_id
 * @param zipf
 * @param rng
 * @return
 */
bool RunMixedScanInsert(const size_t thread_id, ZipfDistribution &zipf, FastRandom &rng) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  concurrency::Transaction *txn =  txn_manager.BeginTransaction(thread_id);

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  // Column ids to be added to logical tile.
  std::vector<oid_t> column_ids;
  std::vector<oid_t> scan_column_ids;
  oid_t column_count = state.column_count + 1;
  oid_t scan_column_count = state.column_count * state.projectivity;

  // read all the attributes in a tuple.
  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(col_itr);
  }
  for (oid_t col_itr = 0; col_itr < scan_column_count; col_itr++) {
    scan_column_ids.push_back(col_itr);
  }

  for (int i = 0; i < state.operation_count; i++) {
    auto rng_val = rng.NextUniform();

    if (rng_val < state.update_ratio) {
      /////////////////////////////////////////////////////////
      // PERFORM Insert
      /////////////////////////////////////////////////////////
      // set up parameter values
      auto table_schema = user_table->GetSchema();
      std::unique_ptr<type::AbstractPool> pool(new type::EphemeralPool());
      const bool allocate = true;
      FastRandom rnd_var(rand());
      auto rowid = GetCurrentTupleCount(thread_id) +1;

      std::unique_ptr<storage::Tuple> tuple(
          new storage::Tuple(table_schema, allocate));
      if (state.key_string_mode){
        std::string eml_str = rnd_var.next_string(16);
        auto primary_key_value = type::ValueFactory::GetVarcharValue(eml_str);
        tuple->SetValue(0, primary_key_value, pool.get());
        eml_keys[rowid] = eml_str;
      }else{
        auto primary_key_value = type::ValueFactory::GetIntegerValue(rowid);
        tuple->SetValue(0, primary_key_value, pool.get());
      }

      if (state.string_mode == true) {
        auto key_value = type::ValueFactory::GetVarcharValue(std::string(100, 'z'));
        for (oid_t col_itr = 1; col_itr < column_count; col_itr++) {
          tuple->SetValue(col_itr, key_value, pool.get());
        }
      } else {
        auto key_value = type::ValueFactory::GetIntegerValue(rowid);
        for (oid_t col_itr = 1; col_itr < column_count; col_itr++) {
          tuple->SetValue(col_itr, key_value, nullptr);
        }
      }

      planner::InsertPlan node(user_table, std::move(tuple));
      executor::InsertExecutor executor(&node, context.get());
      executor.Execute();

      if (txn->GetResult() != ResultType::SUCCESS) {
        txn_manager.AbortTransaction(txn);
        return false;
      }

    } else {
      /////////////////////////////////////////////////////////
      // PERFORM Scan
      /////////////////////////////////////////////////////////
      // set up parameter values
      std::vector<expression::AbstractExpression *> runtime_keys;
      // Create and set up index scan executor
      std::vector<oid_t> key_column_ids_s;
      std::vector<ExpressionType> expr_types_s;
      column_count = column_count * state.projectivity;

      std::vector<type::Value> values_s = {};
      std::vector<oid_t> tuple_key_attrs = {0};
      std::vector<oid_t> index_key_attrs = {0};

      auto predicate = nullptr;

      auto ycsb_pkey_index =
          user_table->GetIndexWithOid(user_table_pkey_index_oid);

      std::vector<oid_t> key_column_ids;
      std::vector<ExpressionType> expr_types;
      key_column_ids.push_back(0);
      expr_types.push_back(ExpressionType::COMPARE_GREATERTHANOREQUALTO);
      auto lookup_key = zipf.GetNextNumber();
      std::vector<type::Value> values;
      if (state.key_string_mode){
        auto lookup_key_s = eml_keys[lookup_key];
        values.push_back(type::ValueFactory::GetVarcharValue(lookup_key_s).Copy());
      }else{
        values.push_back(type::ValueFactory::GetIntegerValue(lookup_key).Copy());
      }
      planner::IndexScanPlan::IndexScanDesc index_scan_desc(
          ycsb_pkey_index, key_column_ids, expr_types, values, runtime_keys);

      planner::IndexScanPlan index_scan_node(user_table, predicate, scan_column_ids,
                                             index_scan_desc);
      int selected_tuple_count = 1000;
      index_scan_node.SetLimit(true);
      index_scan_node.SetLimitNumber(selected_tuple_count);
      index_scan_node.SetLimitOffset(-1);
      // Run the executor
      executor::IndexScanExecutor index_scan_executor(&index_scan_node,
                                                      context.get());

      ExecuteScan(&index_scan_executor);

      if (txn->GetResult() != ResultType::SUCCESS) {
        txn_manager.AbortTransaction(txn);
        return false;
      }
    }

    ++num_rw_ops;
  }

  // transaction passed execution.
  PL_ASSERT(txn->GetResult() == ResultType::SUCCESS);

  auto result = txn_manager.CommitTransaction(txn);

  if (result == ResultType::SUCCESS) {
    return true;

  } else {
    // transaction failed commitment.
    PL_ASSERT(result == ResultType::ABORTED || result == ResultType::FAILURE);
    return false;
  }

}
bool RunMixed(const size_t thread_id, ZipfDistribution &zipf, FastRandom &rng) {
  if (state.scan_rate>0){
        return RunMixedScanInsert(thread_id,zipf,rng);
  }else{
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    concurrency::Transaction *txn =  txn_manager.BeginTransaction(thread_id);
//    if (state.read_only){
//      txn->SetDeclaredReadOnly();
//    }

    std::unique_ptr<executor::ExecutorContext> context(
        new executor::ExecutorContext(txn));

    // Column ids to be added to logical tile.
    std::vector<oid_t> column_ids;
    oid_t column_count = state.column_count + 1;

    // read all the attributes in a tuple.
    for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
      column_ids.push_back(col_itr);
    }
    std::vector<oid_t> key_column_ids;
    std::vector<ExpressionType> expr_types;
    key_column_ids.push_back(0);
    expr_types.push_back(ExpressionType::COMPARE_EQUAL);
    std::vector<expression::AbstractExpression *> runtime_keys;

    for (int i = 0; i < state.operation_count; i++) {
      auto rng_val = rng.NextUniform();

      if (rng_val < state.update_ratio) {
        /////////////////////////////////////////////////////////
        // PERFORM UPDATE
        /////////////////////////////////////////////////////////
        // Create and set up index scan executor
        // set up parameter values
        std::vector<type::Value> values;

        auto lookup_key = 0;
        if(state.random_mode){
          lookup_key = (rand() % ((state.scale_factor * 1000)-0+1))+0;
        }else{
          lookup_key = zipf.GetNextNumber();
        }

        if (state.key_string_mode){
          auto lookup_key_s = eml_keys[lookup_key];
          values.push_back(type::ValueFactory::GetVarcharValue(lookup_key_s).Copy());
        }else{
          values.push_back(type::ValueFactory::GetIntegerValue(lookup_key).Copy());
        }

        //index
        auto ycsb_pkey_index =
            user_table->GetIndexWithOid(user_table_pkey_index_oid);

        planner::IndexScanPlan::IndexScanDesc index_scan_desc(
            ycsb_pkey_index, key_column_ids, expr_types, values, runtime_keys);

        // Create plan node.
        auto predicate = nullptr;

        planner::IndexScanPlan index_scan_node(user_table, predicate, column_ids,
                                               index_scan_desc);

        // Run the executor
        executor::IndexScanExecutor index_scan_executor(&index_scan_node,
                                                        context.get());

        TargetList target_list;
        DirectMapList direct_map_list;

        // update multiple attributes
        for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
          if (col_itr == 1) {
            if (state.string_mode == true) {
              std::string update_raw_value(100, 'a');
              type::Value update_val =
                  type::ValueFactory::GetVarcharValue(update_raw_value).Copy();
              target_list.emplace_back(
                  col_itr,
                  expression::ExpressionUtil::ConstantValueFactory(update_val));

            } else {
              int update_raw_value = 1;
              type::Value update_val =
                  type::ValueFactory::GetIntegerValue(update_raw_value).Copy();
              target_list.emplace_back(
                  col_itr,
                  expression::ExpressionUtil::ConstantValueFactory(update_val));
            }
          } else {
            direct_map_list.emplace_back(col_itr,
                                         std::pair<oid_t, oid_t>(0, col_itr));
          }
        }

        std::unique_ptr<const planner::ProjectInfo> project_info(
            new planner::ProjectInfo(std::move(target_list),
                                     std::move(direct_map_list)));
        planner::UpdatePlan update_node(user_table, std::move(project_info));

        executor::UpdateExecutor update_executor(&update_node, context.get());

        update_executor.AddChild(&index_scan_executor);

        ExecuteUpdate(&update_executor);

        if (txn->GetResult() != ResultType::SUCCESS) {
          txn_manager.AbortTransaction(txn);
          return false;
        }

      } else {
        /////////////////////////////////////////////////////////
        // PERFORM READ
        /////////////////////////////////////////////////////////

        // set up parameter values
        std::vector<type::Value> values;

        auto lookup_key = 0;
        if(state.random_mode){
          lookup_key = (rand() % ((state.scale_factor * 1000)-0+1))+0;
        }else{
          lookup_key = zipf.GetNextNumber();
        }

        if (state.key_string_mode){
          auto lookup_key_s = eml_keys[lookup_key];
          values.push_back(type::ValueFactory::GetVarcharValue(lookup_key_s).Copy());
        }else{
          values.push_back(type::ValueFactory::GetIntegerValue(lookup_key).Copy());
        }

        //index
        auto ycsb_pkey_index =
            user_table->GetIndexWithOid(user_table_pkey_index_oid);

        planner::IndexScanPlan::IndexScanDesc index_scan_desc(
            ycsb_pkey_index, key_column_ids, expr_types, values, runtime_keys);

        // Create plan node.
        auto predicate = nullptr;

        planner::IndexScanPlan index_scan_node(user_table, predicate, column_ids,
                                               index_scan_desc);

        // Run the executor
        executor::IndexScanExecutor index_scan_executor(&index_scan_node,
                                                        context.get());

        ExecuteRead(&index_scan_executor);

        if (txn->GetResult() != ResultType::SUCCESS) {
          txn_manager.AbortTransaction(txn);
          return false;
        }
      }

      ++num_rw_ops;
    }

    // transaction passed execution.
    PL_ASSERT(txn->GetResult() == ResultType::SUCCESS);

    auto result = txn_manager.CommitTransaction(txn);

    if (result == ResultType::SUCCESS) {
      return true;

    } else {
      // transaction failed commitment.
      PL_ASSERT(result == ResultType::ABORTED || result == ResultType::FAILURE);
      return false;
    }
  }

}

bool RunScanMixed(const size_t thread_id) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  concurrency::Transaction *txn = nullptr;
  txn = txn_manager.BeginTransaction(thread_id);
//  if (state.scan_only){
//    txn->SetDeclaredReadOnly();
//  }

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  std::vector<expression::AbstractExpression *> runtime_keys;

  /////////////////////////////////////////////////////////
  // SEQ SCAN + PREDICATE
  /////////////////////////////////////////////////////////

  // Column ids to be added to logical tile after scan.
  // We need all columns because projection can require any column
  const bool is_inlined = true;
  std::vector<oid_t> column_ids;
  oid_t column_count = state.column_count+1;
  column_count = column_count * state.projectivity;

  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(col_itr);
  }

  column_count = state.projectivity * column_count;
  column_ids.resize(column_count);

  std::vector<oid_t> tuple_key_attrs;
  std::vector<oid_t> index_key_attrs;

  tuple_key_attrs = {0};
  index_key_attrs = {0};

  // Create and set up seq scan executor
  auto predicate = CreateScanPredicate(tuple_key_attrs);

  planner::IndexScanPlan::IndexScanDesc index_scan_desc;

  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<type::Value> values;

  // Create index scan predicate
  CreateIndexScanPredicate(index_key_attrs, key_column_ids, expr_types,
                           values);

  // Determine hybrid scan type
  auto hybrid_scan_type = HybridScanType::SEQUENTIAL;
  if (state.index_scan) {
    auto ycsb_pkey_index =
        user_table->GetIndexWithOid(user_table_pkey_index_oid);

    index_scan_desc = planner::IndexScanPlan::IndexScanDesc(
        ycsb_pkey_index, key_column_ids, expr_types, values, runtime_keys);

    hybrid_scan_type = HybridScanType::INDEX;
  }

  std::shared_ptr<planner::HybridScanPlan> hybrid_scan_node(
      new planner::HybridScanPlan(user_table, predicate, column_ids,
                                  index_scan_desc, hybrid_scan_type));

  executor::HybridScanExecutor hybrid_scan_executor(hybrid_scan_node.get(),
                                                    context.get());

  /////////////////////////////////////////////////////////
  // AGGREGATION
  /////////////////////////////////////////////////////////

  // Resize column ids to contain only columns
  // over which we compute aggregates

  // (1-5) Setup plan node

  // 1) Set up group-by columns
  std::vector<oid_t> group_by_columns;

  // 2) Set up project info
  DirectMapList direct_map_list;
  oid_t col_itr = 0;
  oid_t tuple_idx = 1;  // tuple2
  for (col_itr = 0; col_itr < column_count; col_itr++) {
    direct_map_list.push_back({col_itr, {tuple_idx, col_itr}});
    col_itr++;
  }

  std::unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list)));

  // 3) Set up aggregates
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  for (col_itr = 0; col_itr < column_count; col_itr++) {
    planner::AggregatePlan::AggTerm max_column_agg(
        ExpressionType::AGGREGATE_MAX,
        expression::ExpressionUtil::TupleValueFactory(type::Type::INTEGER, 0,
                                                      col_itr),
        false);
    agg_terms.push_back(max_column_agg);
  }

  // 4) Set up predicate (empty)
  std::unique_ptr<const expression::AbstractExpression> aggregate_predicate(
      nullptr);

  // 5) Create output table schema
  auto data_table_schema = user_table->GetSchema();
  std::vector<catalog::Column> columns;
  for (auto column_id : column_ids) {
    columns.push_back(data_table_schema->GetColumn(column_id));
  }

  std::shared_ptr<const catalog::Schema> output_table_schema(
      new catalog::Schema(columns));

  // OK) Create the plan node
  planner::AggregatePlan aggregation_node(
      std::move(proj_info), std::move(aggregate_predicate),
      std::move(agg_terms), std::move(group_by_columns), output_table_schema,
      AggregateType::PLAIN);

  executor::AggregateExecutor aggregation_executor(&aggregation_node,
                                                   context.get());

  aggregation_executor.AddChild(&hybrid_scan_executor);

  /////////////////////////////////////////////////////////
  // MATERIALIZE
  /////////////////////////////////////////////////////////

  // Create and set up materialization executor
  std::vector<catalog::Column> output_columns;
  std::unordered_map<oid_t, oid_t> old_to_new_cols;
  col_itr = 0;
  for (auto column_id : column_ids) {
    auto column = catalog::Column(type::Type::INTEGER,
                                  type::Type::GetTypeSize(type::Type::INTEGER),
                                  std::to_string(column_id), is_inlined);
    output_columns.push_back(column);

    old_to_new_cols[col_itr] = col_itr;

    col_itr++;
  }

  std::shared_ptr<const catalog::Schema> output_schema(
      new catalog::Schema(output_columns));
  bool physify_flag = true;  // is going to create a physical tile
  planner::MaterializationPlan mat_node(old_to_new_cols, output_schema,
                                        physify_flag);

  executor::MaterializationExecutor mat_executor(&mat_node, nullptr);

  mat_executor.AddChild(&aggregation_executor);

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  std::vector<executor::AbstractExecutor *> executors;
  executors.push_back(&mat_executor);


  bool status = false;
  // Run all the executors
  for (auto executor : executors) {
    status = executor->Init();
    if (status == false) {
      throw Exception("Init failed");
    }

    std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;

    while (executor->Execute() == true) {
      std::unique_ptr<executor::LogicalTile> result_tile(executor->GetOutput());
      result_tiles.emplace_back(result_tile.release());
    }

    size_t sum = 0;
    for (auto &result_tile : result_tiles) {
      if (result_tile != nullptr) sum += result_tile->GetTupleCount();
    }

    LOG_DEBUG("result tiles have %d tuples", (int)sum);

    // Execute stuff
    executor->Execute();
  }


  if (txn->GetResult() != ResultType::SUCCESS) {
    txn_manager.AbortTransaction(txn);
    return false;
  }

  // transaction passed execution.
  PL_ASSERT(txn->GetResult() == ResultType::SUCCESS);

  auto result = txn_manager.CommitTransaction(txn);

  if (result == ResultType::SUCCESS) {
    return true;

  } else {
    // transaction failed commitment.
    PL_ASSERT(result == ResultType::ABORTED || result == ResultType::FAILURE);
    return false;
  }
}

bool RunScanSimpleMixed(const size_t thread_id, ZipfDistribution &zipf) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  concurrency::Transaction *txn = txn_manager.BeginTransaction(thread_id);
  if (state.scan_only){
    txn->SetDeclaredReadOnly();
  }

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  std::vector<expression::AbstractExpression *> runtime_keys;

  if(state.index_scan){
    /////////////////////////////////////////////////////////
    // PERFORM bwtree index scan
    /////////////////////////////////////////////////////////
    // Create and set up index scan executor
//    std::vector<oid_t> key_column_ids_s;
//    std::vector<ExpressionType> expr_types_s;
//    std::vector<oid_t> column_ids_s;
//
//    oid_t column_count_s = state.projectivity * state.column_count;
//    for (oid_t col_itr = 0; col_itr < column_count_s; col_itr++) {
//      column_ids_s.push_back(col_itr);
//    }
//
//    // set up parameter values
//    std::vector<type::Value> values_s = {};
//    std::vector<oid_t> tuple_key_attrs = {0};
//    std::vector<oid_t> index_key_attrs = {0};
//
//    auto predicate = nullptr;
//
//    auto ycsb_pkey_index =
//        user_table->GetIndexWithOid(user_table_pkey_index_oid);

//    // Create index scan predicate
//    CreateSimpleIndexScanPredicate(index_key_attrs, key_column_ids_s, expr_types_s,
//                                   values_s);
//
//    planner::IndexScanPlan::IndexScanDesc index_scan_desc(
//        ycsb_pkey_index, key_column_ids_s, expr_types_s, values_s,
//        runtime_keys);
//
//    planner::IndexScanPlan index_scan_node(user_table, predicate,
//                                           column_ids_s, index_scan_desc);
//
//
//    executor::IndexScanExecutor index_scan_executor(&index_scan_node,
//                                                    context.get());
//
////    int tuple_count = state.scale_factor * state.tuples_per_tilegroup;
////      int selected_tuple_count = state.selectivity * tuple_count;
//    int selected_tuple_count = 100;
//    index_scan_node.SetLimit(true);
//    index_scan_node.SetLimitNumber(selected_tuple_count);
//    index_scan_node.SetLimitOffset(-1);
////    LOG_DEBUG("selected_tuple_count: %d", selected_tuple_count);
//
//    ExecuteScan(&index_scan_executor);
//
//    if (txn->GetResult() != ResultType::SUCCESS) {
//      txn_manager.AbortTransaction(txn);
//      return false;
//    }
    std::vector<oid_t> column_ids;
    oid_t column_count = state.column_count + 1;

    // read all the attributes in a tuple.
    for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
      column_ids.push_back(col_itr);
    }

    // set up parameter values
    std::vector<expression::AbstractExpression *> runtime_keys;
    // Create and set up index scan executor
    std::vector<oid_t> key_column_ids_s;
    std::vector<ExpressionType> expr_types_s;

    std::vector<type::Value> values_s = {};
    std::vector<oid_t> tuple_key_attrs = {0};
    std::vector<oid_t> index_key_attrs = {0};

    auto predicate = nullptr;

    auto ycsb_pkey_index =
        user_table->GetIndexWithOid(user_table_pkey_index_oid);


    std::vector<oid_t> key_column_ids;
    std::vector<ExpressionType> expr_types;
    key_column_ids.push_back(0);
    expr_types.push_back(ExpressionType::COMPARE_GREATERTHANOREQUALTO);
    auto lookup_key = zipf.GetNextNumber();
    std::vector<type::Value> values;
    if (state.key_string_mode){
      auto lookup_key_s = eml_keys[lookup_key];
      values.push_back(type::ValueFactory::GetVarcharValue(lookup_key_s).Copy());
    }else{
//        values.push_back(type::ValueFactory::GetIntegerValue(lookup_key).Copy());
      values.push_back(type::ValueFactory::GetVarcharValue(std::to_string(lookup_key)).Copy());
    }
    planner::IndexScanPlan::IndexScanDesc index_scan_desc(
        ycsb_pkey_index, key_column_ids, expr_types, values, runtime_keys);

    planner::IndexScanPlan index_scan_node(user_table, predicate, column_ids,
                                           index_scan_desc);
    int selected_tuple_count = 1000;
    index_scan_node.SetLimit(true);
    index_scan_node.SetLimitNumber(selected_tuple_count);
    index_scan_node.SetLimitOffset(-1);
    // Run the executor
    executor::IndexScanExecutor index_scan_executor(&index_scan_node,
                                                    context.get());

//    LOG_DEBUG("selected_tuple_count: %d", selected_tuple_count);

    auto tuples = ExecuteScan(&index_scan_executor);

    for (int j = 0; j < tuples.size(); ++j) {
      auto tuple_values = tuples[j];
      char *data = new char[100];
      for (int k = 1; k < tuple_values.size(); ++k) {
        type::Value cl = tuple_values[0];
        auto cl_ = cl.ToString().c_str();
        memcpy(data, cl_, 100);
      }
    }

    if (txn->GetResult() != ResultType::SUCCESS) {
      txn_manager.AbortTransaction(txn);
      return false;
    }
  }else {
    /////////////////////////////////////////////////////////
    // PERFORM sequential scan
    /////////////////////////////////////////////////////////
    // Create and set up index scan executor
    std::vector<oid_t> key_column_ids_s;
    std::vector<ExpressionType> expr_types_s;
    std::vector<oid_t> column_ids_s;

    oid_t column_count_s = state.projectivity * state.column_count;
    for (oid_t col_itr = 0; col_itr < column_count_s; col_itr++) {
      column_ids_s.push_back(col_itr);
    }

    // set up parameter values
    std::vector<type::Value> values_s = {};
    std::vector<oid_t> tuple_key_attrs = {0};
    std::vector<oid_t> index_key_attrs = {0};

    auto predicate = CreateScanPredicate(tuple_key_attrs);
    auto hybrid_scan_type = HybridScanType::SEQUENTIAL;
    planner::IndexScanPlan::IndexScanDesc index_scan_desc_s;

    std::shared_ptr<planner::HybridScanPlan> hybrid_scan_node(
        new planner::HybridScanPlan(user_table, predicate, column_ids_s,
                                    index_scan_desc_s, hybrid_scan_type));

    // Run the executor
    executor::HybridScanExecutor hybrid_scan_executor(hybrid_scan_node.get(),
                                                      context.get());



    ExecuteRead(&hybrid_scan_executor);
    if (txn->GetResult() != ResultType::SUCCESS) {
      txn_manager.AbortTransaction(txn);
      return false;
    }
  }


  // transaction passed execution.
  PL_ASSERT(txn->GetResult() == ResultType::SUCCESS);

  auto result = txn_manager.CommitTransaction(txn);

  if (result == ResultType::SUCCESS) {
    return true;

  } else {
    // transaction failed commitment.
    PL_ASSERT(result == ResultType::ABORTED || result == ResultType::FAILURE);
    return false;
  }
}
}//namespace ycsb
}//namespace benchmark
}//namespace peloton