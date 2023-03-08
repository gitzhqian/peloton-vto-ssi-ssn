//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_new_order.cpp
//
// Identification: src/main/tpcc/tpcc_new_order.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//



#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>
#include <cassert>
#include <thread>
#include <algorithm>
#include <random>
#include <cstddef>
#include <limits>


#include "benchmark/tpcc/tpcc_workload.h"
#include "benchmark/tpcc/tpcc_configuration.h"
#include "benchmark/tpcc/tpcc_loader.h"

#include "catalog/manager.h"
#include "catalog/schema.h"

#include "type/types.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "common/logger.h"
#include "common/timer.h"
#include "common/generator.h"

#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"

#include "executor/executor_context.h"
#include "executor/abstract_executor.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "executor/materialization_executor.h"
#include "executor/update_executor.h"
#include "executor/index_scan_executor.h"
#include "executor/insert_executor.h"

#include "expression/abstract_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/tuple_value_expression.h"
#include "expression/comparison_expression.h"
#include "expression/expression_util.h"
#include "common/container_tuple.h"

#include "index/index_factory.h"

#include "logging/log_manager.h"

#include "planner/abstract_plan.h"
#include "planner/materialization_plan.h"
#include "planner/insert_plan.h"
#include "planner/update_plan.h"
#include "planner/index_scan_plan.h"

#include "storage/data_table.h"
#include "storage/table_factory.h"



namespace peloton {
namespace benchmark {
namespace tpcc {

bool RunNewOrder(const size_t &thread_id){
  /*
     "NEW_ORDER": {
     "getWarehouseTaxRate": "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?", # w_id
     "getDistrict": "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?", # d_id, w_id
     "getCustomer": "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
     "incrementNextOrderId": "UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?", # d_next_o_id, d_id, w_id
     "createOrder": "INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", # d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local
     "createNewOrder": "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)", # o_id, d_id, w_id
     "getItemInfo": "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?", # ol_i_id
     "getStockInfo": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?", # d_id, ol_i_id, ol_supply_w_id
     "updateStock": "UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?", # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
     "createOrderLine": "INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", # o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info
     }
   */

  LOG_TRACE("-------------------------------------");

  /////////////////////////////////////////////////////////
  // PREPARE ARGUMENTS
  /////////////////////////////////////////////////////////
  int warehouse_id = GenerateWarehouseId(thread_id);
  int district_id = GetRandomInteger(0, state.districts_per_warehouse - 1);
  int customer_id = GetRandomInteger(0, state.customers_per_district - 1);
  int o_ol_cnt = GetRandomInteger(orders_min_ol_cnt, orders_max_ol_cnt);

  std::vector<int> i_ids, ol_w_ids, ol_qtys;
  bool o_all_local = true;

  for (auto ol_itr = 0; ol_itr < o_ol_cnt; ol_itr++) {
    // in the original TPC-C benchmark, it is possible to read an item that does not exist.
    // for simplicity, we ignore this case.
    // this essentially makes the processing of NewOrder transaction more time-consuming.
    i_ids.push_back(GetRandomInteger(0, state.item_count - 1));
    bool remote = GetRandomBoolean(new_order_remote_txns);
    ol_w_ids.push_back(warehouse_id);

    if(remote == true) {
      ol_w_ids[ol_itr] = GetRandomIntegerExcluding(0, state.warehouse_count - 1, warehouse_id);
      o_all_local = false;
    }

    ol_qtys.push_back(GetRandomInteger(0, order_line_max_ol_quantity));
  }

  std::vector<expression::AbstractExpression *> runtime_keys;

  /////////////////////////////////////////////////////////
  // BEGIN TRANSACTION
  /////////////////////////////////////////////////////////

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction(thread_id);

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  std::vector<oid_t> item_key_column_ids;
  std::vector<ExpressionType> item_expr_types;
  item_key_column_ids.push_back(0); // I_ID
  item_expr_types.push_back(
    ExpressionType::COMPARE_EQUAL);
  
  auto item_pkey_index = item_table->GetIndexWithOid(
    item_table_pkey_index_oid);
  
  std::vector<oid_t> item_column_ids = {2, 3, 4}; // I_NAME, I_PRICE, I_DATA
  
  for (auto item_id : i_ids) {

    LOG_TRACE("getItemInfo: SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = %d", item_id);
    
    std::vector<type::Value > item_key_values;
    
//    item_key_values.push_back(type::ValueFactory::GetIntegerValue(item_id).Copy());
    item_key_values.push_back(type::ValueFactory::GetVarcharValue(std::to_string(item_id)));

    planner::IndexScanPlan::IndexScanDesc item_index_scan_desc(
      item_pkey_index, item_key_column_ids, item_expr_types,
      item_key_values, runtime_keys);


    planner::IndexScanPlan item_index_scan_node(item_table, nullptr,
     item_column_ids,
     item_index_scan_desc);

    executor::IndexScanExecutor item_index_scan_executor(&item_index_scan_node, context.get());

    auto gii_lists_values = ExecuteRead(&item_index_scan_executor);

    if (txn->GetResult() != ResultType::SUCCESS) {
      LOG_TRACE("abort transaction");
      txn_manager.AbortTransaction(txn);
      return false;
    }

    if (gii_lists_values.size() != 1) {
      LOG_ERROR("getItemInfo return size incorrect : %lu", gii_lists_values.size());
      PL_ASSERT(false);
    }

  }


  LOG_TRACE("getWarehouseTaxRate: SELECT W_TAX FROM WAREHOUSE WHERE W_ID = %d", warehouse_id);
  
  std::vector<oid_t> warehouse_key_column_ids;
  std::vector<ExpressionType> warehouse_expr_types;
  warehouse_key_column_ids.push_back(0); // W_ID
  warehouse_expr_types.push_back(ExpressionType::COMPARE_EQUAL);

  std::vector<type::Value > warehouse_key_values;

//  warehouse_key_values.push_back(type::ValueFactory::GetIntegerValue(warehouse_id).Copy());
  warehouse_key_values.push_back(type::ValueFactory::GetVarcharValue(std::to_string(warehouse_id)));

  auto warehouse_pkey_index = warehouse_table->GetIndexWithOid(
      warehouse_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc warehouse_index_scan_desc(
      warehouse_pkey_index, warehouse_key_column_ids, warehouse_expr_types,
      warehouse_key_values, runtime_keys);

  std::vector<oid_t> warehouse_column_ids = {7}; // W_TAX

  planner::IndexScanPlan warehouse_index_scan_node(warehouse_table, nullptr,
                                                   warehouse_column_ids,
                                                   warehouse_index_scan_desc);

  executor::IndexScanExecutor warehouse_index_scan_executor(&warehouse_index_scan_node, context.get());

  auto gwtr_lists_values = ExecuteRead(&warehouse_index_scan_executor);

  if (txn->GetResult() != ResultType::SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction(txn);
    return false;
  }

  if (gwtr_lists_values.size() != 1) {
    LOG_ERROR("getWarehouseTaxRate return size incorrect : %lu", gwtr_lists_values.size());
    PL_ASSERT(false);
  }

  UNUSED_ATTRIBUTE auto w_tax = gwtr_lists_values[0][0];

  LOG_TRACE("w_tax: %s", w_tax.GetInfo().c_str());


  LOG_TRACE("getDistrict: SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = %d AND D_W_ID = %d", district_id, warehouse_id);

  std::vector<oid_t> district_key_column_ids;
  std::vector<ExpressionType> district_expr_types;
  
  district_key_column_ids.push_back(0); // D_ID
  district_key_column_ids.push_back(1); // D_W_ID
  district_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
  district_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
  
  auto district_pkey_index = district_table->GetIndexWithOid(
      district_table_pkey_index_oid);

  std::vector<type::Value > district_key_values;
//  district_key_values.push_back(type::ValueFactory::GetIntegerValue(district_id).Copy());
//  district_key_values.push_back(type::ValueFactory::GetIntegerValue(warehouse_id).Copy());
  district_key_values.push_back(type::ValueFactory::GetVarcharValue(std::to_string(district_id)));
  district_key_values.push_back(type::ValueFactory::GetVarcharValue(std::to_string(warehouse_id)));

  planner::IndexScanPlan::IndexScanDesc district_index_scan_desc(
      district_pkey_index, district_key_column_ids, district_expr_types,
      district_key_values, runtime_keys);

  std::vector<oid_t> district_column_ids = {8, 10}; // D_TAX, D_NEXT_O_ID

  // Create plan node.
  planner::IndexScanPlan district_index_scan_node(district_table, nullptr,
                                                  district_column_ids,
                                                  district_index_scan_desc);

  executor::IndexScanExecutor district_index_scan_executor(&district_index_scan_node, context.get());

  auto gd_lists_values = ExecuteRead(&district_index_scan_executor);

  if (txn->GetResult() != ResultType::SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction(txn);
    return false;
  }

  if (gd_lists_values.size() != 1) {
    LOG_ERROR("getDistrict return size incorrect : %lu", gd_lists_values.size());
    PL_ASSERT(false);
  }

  UNUSED_ATTRIBUTE auto d_tax = gd_lists_values[0][0];
  UNUSED_ATTRIBUTE auto d_next_o_id = gd_lists_values[0][1];

  LOG_TRACE("d_tax: %s, d_next_o_id: %s", d_tax.GetInfo().c_str(), d_next_o_id.GetInfo().c_str());


  LOG_TRACE("getCustomer: SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d", warehouse_id, district_id, customer_id);


  std::vector<oid_t> customer_key_column_ids;
  std::vector<ExpressionType> customer_expr_types;

  customer_key_column_ids.push_back(0); // C_ID
  customer_key_column_ids.push_back(1); // C_D_ID
  customer_key_column_ids.push_back(2); // C_W_ID
  customer_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
  customer_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
  customer_expr_types.push_back(ExpressionType::COMPARE_EQUAL);

  std::vector<type::Value > customer_key_values;
//  customer_key_values.push_back(type::ValueFactory::GetIntegerValue(customer_id).Copy());
//  customer_key_values.push_back(type::ValueFactory::GetIntegerValue(district_id).Copy());
//  customer_key_values.push_back(type::ValueFactory::GetIntegerValue(warehouse_id).Copy());
  customer_key_values.push_back(type::ValueFactory::GetVarcharValue(std::to_string(customer_id)));
  customer_key_values.push_back(type::ValueFactory::GetVarcharValue(std::to_string(district_id)));
  customer_key_values.push_back(type::ValueFactory::GetVarcharValue(std::to_string(warehouse_id)));
  
  auto customer_pkey_index = customer_table->GetIndexWithOid(
      customer_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc customer_index_scan_desc(
      customer_pkey_index, customer_key_column_ids, customer_expr_types,
      customer_key_values, runtime_keys);

  std::vector<oid_t> customer_column_ids = {5, 13, 15}; // C_LAST, C_CREDIT, C_DISCOUNT

  // Create plan node.
  planner::IndexScanPlan customer_index_scan_node(customer_table, nullptr,
                                                  customer_column_ids,
                                                  customer_index_scan_desc);

  executor::IndexScanExecutor customer_index_scan_executor(&customer_index_scan_node, context.get());

  auto gc_lists_values = ExecuteRead(&customer_index_scan_executor);

  if (txn->GetResult() != ResultType::SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction(txn);
    return false;
  }

  if (gc_lists_values.size() != 1) {
    LOG_ERROR("getCustomer return size incorrect : %lu", gc_lists_values.size());
    PL_ASSERT(false);
  }

  UNUSED_ATTRIBUTE auto c_last = gc_lists_values[0][0];
  UNUSED_ATTRIBUTE auto c_credit = gc_lists_values[0][1];
  UNUSED_ATTRIBUTE auto c_discount = gc_lists_values[0][2];

  LOG_TRACE("c_last: %s, c_credit: %s, c_discount: %s", c_last.GetInfo().c_str(), c_credit.GetInfo().c_str(), c_discount.GetInfo().c_str());


  int district_update_value = type::ValuePeeker::PeekInteger(d_next_o_id) + 1;
  LOG_TRACE("district update value = %d", district_update_value);

  LOG_TRACE("incrementNextOrderId: UPDATE DISTRICT SET D_NEXT_O_ID = %d WHERE D_ID = %d AND D_W_ID = %d", district_update_value, district_id, warehouse_id);

  std::vector<oid_t> district_update_column_ids = {10}; // D_NEXT_O_ID

  std::vector<type::Value > district_update_key_values;
//  district_update_key_values.push_back(type::ValueFactory::GetIntegerValue(district_id).Copy());
//  district_update_key_values.push_back(type::ValueFactory::GetIntegerValue(warehouse_id).Copy());
  district_update_key_values.push_back(type::ValueFactory::GetVarcharValue(std::to_string(district_id)));
  district_update_key_values.push_back(type::ValueFactory::GetVarcharValue(std::to_string(warehouse_id)));

  planner::IndexScanPlan::IndexScanDesc district_update_index_scan_desc(
      district_pkey_index, district_key_column_ids, district_expr_types,
      district_update_key_values, runtime_keys);

  // Create plan node.
  planner::IndexScanPlan district_update_index_scan_node(district_table, nullptr,
                                                  district_update_column_ids,
                                                  district_update_index_scan_desc);

  executor::IndexScanExecutor district_update_index_scan_executor(&district_update_index_scan_node, context.get());

  TargetList district_target_list;
  DirectMapList district_direct_map_list;

  // Update the last attribute
  for (oid_t col_itr = 0; col_itr < 10; col_itr++) {
      district_direct_map_list.emplace_back(col_itr,
                                   std::pair<oid_t, oid_t>(0, col_itr));
  }
  type::Value  district_update_val = type::ValueFactory::GetIntegerValue(district_update_value).Copy();
  
  district_target_list.emplace_back(
      10, expression::ExpressionUtil::ConstantValueFactory(district_update_val));

  std::unique_ptr<const planner::ProjectInfo> district_project_info(
      new planner::ProjectInfo(std::move(district_target_list),
                               std::move(district_direct_map_list)));
  planner::UpdatePlan district_update_node(district_table, std::move(district_project_info));

  executor::UpdateExecutor district_update_executor(&district_update_node, context.get());

  district_update_executor.AddChild(&district_update_index_scan_executor);

  ExecuteUpdate(&district_update_executor);

  if (txn->GetResult() != ResultType::SUCCESS) {
    LOG_TRACE("abort transaction");
    txn_manager.AbortTransaction(txn);
    return false;
  }

  LOG_TRACE("createOrder: INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL)");


  std::unique_ptr<storage::Tuple> orders_tuple(new storage::Tuple(orders_table->GetSchema(), true));

  // O_ID
//  orders_tuple->SetValue(0, type::ValueFactory::GetIntegerValue(type::ValuePeeker::PeekInteger(d_next_o_id)), nullptr);
  orders_tuple->SetValue(0, type::ValueFactory::GetVarcharValue(std::to_string(type::ValuePeeker::PeekInteger(d_next_o_id))), nullptr);
  // O_C_ID
//  orders_tuple->SetValue(1, type::ValueFactory::GetIntegerValue(customer_id), nullptr);
  orders_tuple->SetValue(1, type::ValueFactory::GetVarcharValue(std::to_string(customer_id)), nullptr);
  // O_D_ID
//  orders_tuple->SetValue(2, type::ValueFactory::GetIntegerValue(district_id), nullptr);
  orders_tuple->SetValue(2, type::ValueFactory::GetVarcharValue(std::to_string(district_id)), nullptr);
  // O_W_ID
//  orders_tuple->SetValue(3, type::ValueFactory::GetIntegerValue(warehouse_id), nullptr);
  orders_tuple->SetValue(3, type::ValueFactory::GetVarcharValue(std::to_string(warehouse_id)), nullptr);
  // O_ENTRY_D
  //auto o_entry_d = GetTimeStamp();
  orders_tuple->SetValue(4, type::ValueFactory::GetTimestampValue(1) , nullptr);
  // O_CARRIER_ID
  orders_tuple->SetValue(5, type::ValueFactory::GetIntegerValue(0), nullptr);
  // O_OL_CNT
  orders_tuple->SetValue(6, type::ValueFactory::GetIntegerValue(o_ol_cnt), nullptr);
  // O_ALL_LOCAL
  orders_tuple->SetValue(7, type::ValueFactory::GetIntegerValue(o_all_local), nullptr);

  planner::InsertPlan orders_node(orders_table, std::move(orders_tuple));
  executor::InsertExecutor orders_executor(&orders_node, context.get());
  orders_executor.Execute();

  if (txn->GetResult() != ResultType::SUCCESS) {
    LOG_TRACE("abort transaction when inserting order table, thread_id = %d, d_id = %d, next_o_id = %d", (int)thread_id, (int)district_id, (int)type::ValuePeeker::PeekInteger(d_next_o_id));
    txn_manager.AbortTransaction(txn);
    return false;
  } else {
    LOG_TRACE("successfully insert order table, thread_id = %d, d_id = %d, next_o_id = %d", (int)thread_id, (int)district_id, (int)type::ValuePeeker::PeekInteger(d_next_o_id));
  }

  LOG_TRACE("createNewOrder: INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)");
  std::unique_ptr<storage::Tuple> new_order_tuple(new storage::Tuple(new_order_table->GetSchema(), true));

  // NO_O_ID
//  new_order_tuple->SetValue(0, type::ValueFactory::GetIntegerValue(type::ValuePeeker::PeekInteger(d_next_o_id)), nullptr);
  new_order_tuple->SetValue(0, type::ValueFactory::GetVarcharValue(std::to_string(type::ValuePeeker::PeekInteger(d_next_o_id))),
                            nullptr);
  // NO_D_ID
//  new_order_tuple->SetValue(1, type::ValueFactory::GetIntegerValue(district_id), nullptr);
  new_order_tuple->SetValue(1, type::ValueFactory::GetVarcharValue(std::to_string(district_id)), nullptr);
  // NO_W_ID
//  new_order_tuple->SetValue(2, type::ValueFactory::GetIntegerValue(warehouse_id), nullptr);
  new_order_tuple->SetValue(2, type::ValueFactory::GetVarcharValue(std::to_string(warehouse_id)), nullptr);

  planner::InsertPlan new_order_node(new_order_table, std::move(new_order_tuple));
  executor::InsertExecutor new_order_executor(&new_order_node, context.get());
  new_order_executor.Execute();

  if (txn->GetResult() != ResultType::SUCCESS) {
    LOG_TRACE("abort transaction when inserting new order table");
    txn_manager.AbortTransaction(txn);
    return false;
  }



  std::vector<oid_t> stock_key_column_ids;
  std::vector<ExpressionType> stock_expr_types;

  stock_key_column_ids.push_back(0); // S_I_ID
  stock_key_column_ids.push_back(1); // S_W_ID
  stock_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
  stock_expr_types.push_back(ExpressionType::COMPARE_EQUAL);


  auto stock_pkey_index = stock_table->GetIndexWithOid(
      stock_table_pkey_index_oid);
  
  // S_QUANTITY, S_DIST_%02d, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA
  std::vector<oid_t> stock_column_ids = {2, oid_t(3 + district_id), 13, 14, 15, 16}; 

  std::vector<oid_t> stock_update_column_ids = {2, 13, 14, 15}; // S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT

  for (size_t i = 0; i < i_ids.size(); ++i) {
    int item_id = i_ids.at(i);
    int ol_w_id = ol_w_ids.at(i);
    int ol_qty = ol_qtys.at(i);

    LOG_TRACE("getStockInfo: SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_? FROM STOCK WHERE S_I_ID = %d AND S_W_ID = %d", item_id, ol_w_id);
    
    std::vector<type::Value > stock_key_values;
    
//    stock_key_values.push_back(type::ValueFactory::GetIntegerValue(item_id).Copy());
//    stock_key_values.push_back(type::ValueFactory::GetIntegerValue(ol_w_id).Copy());
    stock_key_values.push_back(type::ValueFactory::GetVarcharValue(std::to_string(item_id)));
    stock_key_values.push_back(type::ValueFactory::GetVarcharValue(std::to_string(ol_w_id)));

    planner::IndexScanPlan::IndexScanDesc stock_index_scan_desc(
        stock_pkey_index, stock_key_column_ids, stock_expr_types,
        stock_key_values, runtime_keys);

    std::vector<type::Value > stock_update_key_values;
    
//    stock_update_key_values.push_back(type::ValueFactory::GetIntegerValue(item_id).Copy());
//    stock_update_key_values.push_back(type::ValueFactory::GetIntegerValue(ol_w_id).Copy());
    stock_update_key_values.push_back(type::ValueFactory::GetVarcharValue(std::to_string(item_id)));
    stock_update_key_values.push_back(type::ValueFactory::GetVarcharValue(std::to_string(ol_w_id)));

    planner::IndexScanPlan::IndexScanDesc stock_update_index_scan_desc(
        stock_pkey_index, stock_key_column_ids, stock_expr_types,
        stock_update_key_values, runtime_keys);


    // Create plan node.
    planner::IndexScanPlan stock_index_scan_node(stock_table, nullptr,
                                                 stock_column_ids,
                                                 stock_index_scan_desc);

    executor::IndexScanExecutor stock_index_scan_executor(&stock_index_scan_node, context.get());

    auto gsi_lists_values = ExecuteRead(&stock_index_scan_executor);

    if (txn->GetResult() != ResultType::SUCCESS) {
      LOG_TRACE("abort transaction");
      txn_manager.AbortTransaction(txn);
      return false;
    }

    if (gsi_lists_values.size() != 1) {
      LOG_ERROR("getStockInfo return size incorrect : %lu", gsi_lists_values.size());
      PL_ASSERT(false);
    }

    int s_quantity = type::ValuePeeker::PeekInteger(gsi_lists_values[0][0]);

    if (s_quantity >= ol_qty + 10) {
      s_quantity = s_quantity - ol_qty;
    } else {
      s_quantity = s_quantity + 91 - ol_qty;
    }

    type::Value  s_data = gsi_lists_values[0][1];

    int s_ytd = type::ValuePeeker::PeekInteger(gsi_lists_values[0][2]) + ol_qty;

    int s_order_cnt = type::ValuePeeker::PeekInteger(gsi_lists_values[0][3]) + 1;

    int s_remote_cnt = type::ValuePeeker::PeekInteger(gsi_lists_values[0][4]);

    if (ol_w_id != warehouse_id) {
      s_remote_cnt += 1;
    }

    LOG_TRACE("updateStock: UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?");

    // Create plan node.
    planner::IndexScanPlan stock_update_index_scan_node(stock_table, nullptr,
                                                        stock_update_column_ids,
                                                        stock_update_index_scan_desc);
    
    executor::IndexScanExecutor stock_update_index_scan_executor(&stock_update_index_scan_node, context.get());

    TargetList stock_target_list;
    DirectMapList stock_direct_map_list;

    // Update the last attribute
    for (oid_t col_itr = 0; col_itr < 17; col_itr++) {
      if (col_itr != 2 && col_itr != 13 && col_itr != 14 && col_itr != 15) {
        stock_direct_map_list.emplace_back(col_itr,
                                     std::pair<oid_t, oid_t>(0, col_itr));
      }
    }
    stock_target_list.emplace_back(
        2, expression::ExpressionUtil::ConstantValueFactory(type::ValueFactory::GetIntegerValue(s_quantity)));
    stock_target_list.emplace_back(
        13, expression::ExpressionUtil::ConstantValueFactory(type::ValueFactory::GetIntegerValue(s_ytd)));
    stock_target_list.emplace_back(
        14, expression::ExpressionUtil::ConstantValueFactory(type::ValueFactory::GetIntegerValue(s_order_cnt)));
    stock_target_list.emplace_back(
        15, expression::ExpressionUtil::ConstantValueFactory(type::ValueFactory::GetIntegerValue(s_remote_cnt)));

    std::unique_ptr<const planner::ProjectInfo> stock_project_info(
        new planner::ProjectInfo(std::move(stock_target_list),
                                 std::move(stock_direct_map_list)));
    planner::UpdatePlan stock_update_node(stock_table, std::move(stock_project_info));

    executor::UpdateExecutor stock_update_executor(&stock_update_node, context.get());

    stock_update_executor.AddChild(&stock_update_index_scan_executor);

    ExecuteUpdate(&stock_update_executor);

    if (txn->GetResult() != ResultType::SUCCESS) {
      LOG_TRACE("abort transaction");
      txn_manager.AbortTransaction(txn);
      return false;
    }

    // the original benchmark requires check constraints.
    // however, we ignored here.
    // it does not influence the performance.
    // if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
    // brand_generic = 'B'
    // else:
    // brand_generic = 'G'

    LOG_TRACE("createOrderLine: INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
    std::unique_ptr<storage::Tuple> order_line_tuple(new storage::Tuple(order_line_table->GetSchema(), true));

    // OL_O_ID
//    order_line_tuple->SetValue(0, type::ValueFactory::GetIntegerValue(type::ValuePeeker::PeekInteger(d_next_o_id)), nullptr);
    order_line_tuple->SetValue(0, type::ValueFactory::GetVarcharValue(std::to_string(type::ValuePeeker::PeekInteger(d_next_o_id))),
                               nullptr);
    // OL_D_ID
//    order_line_tuple->SetValue(1, type::ValueFactory::GetIntegerValue(district_id), nullptr);
    order_line_tuple->SetValue(1, type::ValueFactory::GetVarcharValue(std::to_string(district_id)), nullptr);
    // OL_W_ID
//    order_line_tuple->SetValue(2, type::ValueFactory::GetIntegerValue(warehouse_id), nullptr);
    order_line_tuple->SetValue(2, type::ValueFactory::GetVarcharValue(std::to_string(warehouse_id)), nullptr);
    // OL_NUMBER
//    order_line_tuple->SetValue(3, type::ValueFactory::GetIntegerValue(i), nullptr);
    order_line_tuple->SetValue(3, type::ValueFactory::GetVarcharValue(std::to_string(i)), nullptr);
    // OL_I_ID
    order_line_tuple->SetValue(4, type::ValueFactory::GetIntegerValue(item_id), nullptr);
    // OL_SUPPLY_W_ID
    order_line_tuple->SetValue(5, type::ValueFactory::GetIntegerValue(ol_w_id), nullptr);
    // OL_DELIVERY_D
    order_line_tuple->SetValue(6, type::ValueFactory::GetTimestampValue(1) , nullptr);
    // OL_QUANTITY
    order_line_tuple->SetValue(7, type::ValueFactory::GetIntegerValue(ol_qty), nullptr);
    // OL_AMOUNT
    // TODO: workaround!!! I don't know how to get float from Value.
    order_line_tuple->SetValue(8, type::ValueFactory::GetDecimalValue(0), nullptr);
    // OL_DIST_INFO
    order_line_tuple->SetValue(9, s_data, nullptr);

    planner::InsertPlan order_line_node(order_line_table, std::move(order_line_tuple));
    executor::InsertExecutor order_line_executor(&order_line_node, context.get());
    order_line_executor.Execute();

    if (txn->GetResult() != ResultType::SUCCESS) {
      LOG_TRACE("abort transaction when inserting order line table");
      txn_manager.AbortTransaction(txn);
      return false;
    }
  }

  // transaction passed execution.
  PL_ASSERT(txn->GetResult() == ResultType::SUCCESS);

  auto result = txn_manager.CommitTransaction(txn);

  if (result == ResultType::SUCCESS) {
    // transaction passed commitment.
    LOG_TRACE("commit txn, thread_id = %d, d_id = %d, next_o_id = %d", (int)thread_id, (int)district_id,
              (int)type::ValuePeeker::PeekInteger(d_next_o_id));
    return true;
    
  } else {
    // transaction failed commitment.
    PL_ASSERT(result == ResultType::ABORTED ||
           result == ResultType::FAILURE);
    LOG_TRACE("abort txn, thread_id = %d, d_id = %d, next_o_id = %d", (int)thread_id, (int)district_id,
              (int)type::ValuePeeker::PeekInteger(d_next_o_id));
    return false;
  }
}


bool RunQuery2(const size_t &thread_id,
               const std::vector<std::vector<std::pair<int32_t, int32_t>>> &supp_stock_map) {
  /*
     "query_2": {
                 "SELECT su_suppkey, "
                  + "su_name, "
                  + "n_name, "
                  + "i_id, "
                  + "i_name, "
                  + "su_address, "
                  + "su_phone, "
                  + "su_comment "
                  + "FROM item, supplier, stock, nation, region, "
                  + "(SELECT s_i_id AS m_i_id, MIN(s_quantity) AS m_s_quantity "
                  + "FROM stock, "
                  + "supplier, "
                  + "nation, "
                  + "region "
                  + "WHERE MOD((s_w_id*s_i_id), 10000)=su_suppkey "
                  + "AND su_nationkey=n_nationkey "
                  + "AND n_regionkey=r_regionkey "
                  + "AND r_name LIKE 'Europ%' "
                  + "GROUP BY s_i_id) m "
                  + "WHERE i_id = s_i_id "
                  + "AND MOD((s_w_id * s_i_id), 10000) = su_suppkey "
                  + "AND su_nationkey = n_nationkey "
                  + "AND n_regionkey = r_regionkey "
                  + "AND i_data LIKE '%b' "
                  + "AND r_name LIKE 'Europ%' "
                  + "AND i_id=m_i_id "
                  + "AND s_quantity = m_s_quantity "
                  + "ORDER BY n_name, "
                  + "su_name, "
                  + "i_id"
     }
   */

  std::vector<expression::AbstractExpression *> runtime_keys;
  /////////////////////////////////////////////////////////
  // BEGIN TRANSACTION
  /////////////////////////////////////////////////////////
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction(thread_id);
  std::unique_ptr<executor::ExecutorContext> context(new executor::ExecutorContext(txn));

//  std::vector<std::vector<type::Value>> regions_vec;
//  std::vector<std::vector<type::Value>> nations_vec;
  std::vector<type::Value> regions_vec;
  std::vector<std::string> regions_names_vec;
  std::vector<std::pair<type::Value,type::Value>> nations_vec;
  std::vector<std::string> nations_names_vec;
  // Prepare random data
  //ScanRegion
  {
    std::vector<oid_t> region_key_column_ids;
    std::vector<ExpressionType> region_expr_types;
    region_key_column_ids.push_back(0); // I_ID
    region_expr_types.push_back(ExpressionType::COMPARE_GREATERTHANOREQUALTO);

    auto region_pkey_index = region_table->GetIndexWithOid(region_table_pkey_index_oid);

    std::vector<oid_t> region_column_ids = {0, 1, 2}; // r_key, r_name

    LOG_TRACE("getItemInfo: SELECT R_REGIONKEY, R_NAME, R_COMMENT FROM REGION WHERE R_REGIONKEY = %d", reg_id);

    std::vector<type::Value > region_key_values;
//      region_key_values.push_back(type::ValueFactory::GetIntegerValue(reg_id).Copy());
    region_key_values.push_back(type::ValueFactory::GetVarcharValue(std::to_string(0)));

    planner::IndexScanPlan::IndexScanDesc region_index_scan_desc(
        region_pkey_index, region_key_column_ids, region_expr_types,
        region_key_values, runtime_keys);


    planner::IndexScanPlan region_index_scan_node(region_table, nullptr,
                                                region_column_ids,
                                                  region_index_scan_desc);

    executor::IndexScanExecutor region_index_scan_executor(&region_index_scan_node, context.get());

    auto reg_lists_values = ExecuteRead(&region_index_scan_executor);

    if (txn->GetResult() != ResultType::SUCCESS) {
      LOG_DEBUG("abort transaction");
      txn_manager.AbortTransaction(txn);
      return false;
    }

    assert(reg_lists_values.size() == 5);

    for (uint reg_id = 0; reg_id < 5; reg_id++) {
      auto reg_key = reg_lists_values[reg_id][0];
      auto reg_name = reg_lists_values[reg_id][1];
      regions_vec.emplace_back(reg_key);
      regions_names_vec.emplace_back(reg_name.ToString());
    }

  }

  //ScanNation
  {
    std::vector<oid_t> nation_key_column_ids;
    std::vector<ExpressionType> nation_expr_types;
    nation_key_column_ids.push_back(0); // I_ID
    nation_expr_types.push_back(ExpressionType::COMPARE_GREATERTHANOREQUALTO);

    auto nation_pkey_index = nation_table->GetIndexWithOid(nation_table_pkey_index_oid);

    std::vector<oid_t> nation_column_ids = {0, 1, 2, 3}; // nationkey, regionkey, name

//      LOG_DEBUG("getNationInfo: SELECT N_NATIONKEY, N_REGIONKEY, N_NAME, N_COMMENT"
//                                            " FROM NATION WHERE N_NATIONKEY = %d", nat_id);
    std::vector<type::Value > nation_key_values;
//      nation_key_values.push_back(type::ValueFactory::GetIntegerValue(nat_id).Copy());
    nation_key_values.push_back(type::ValueFactory::GetVarcharValue(std::to_string(0)));

    planner::IndexScanPlan::IndexScanDesc nation_index_scan_desc(
        nation_pkey_index, nation_key_column_ids, nation_expr_types,
        nation_key_values, runtime_keys);


    planner::IndexScanPlan nation_index_scan_node(nation_table, nullptr,
                                                  nation_column_ids,
                                                  nation_index_scan_desc);

    executor::IndexScanExecutor nation_index_scan_executor(&nation_index_scan_node, context.get());

    auto nat_lists_values = ExecuteRead(&nation_index_scan_executor);

    if (txn->GetResult() != ResultType::SUCCESS) {
      LOG_DEBUG("abort transaction");
      txn_manager.AbortTransaction(txn);
      return false;
    }
    assert(nat_lists_values.size() == 62);
    for (uint nat_id = 0; nat_id < 62; nat_id++) {
        auto nat_key = nat_lists_values[nat_id][0];
        auto reg_key = nat_lists_values[nat_id][1];
        auto nat_name = nat_lists_values[nat_id][2];
        //      oid_t nat_key_ = type::ValuePeeker::PeekInteger(nat_key);
        //      oid_t reg_key_ = type::ValuePeeker::PeekInteger(reg_key);
        nations_vec.emplace_back(nat_key, reg_key);
        nations_names_vec.emplace_back(nat_name.ToString());
      }
  }

  {
    // Pick a target region
//    auto target_region = GetRandomInteger(0, 4);
    auto target_region = 3;
    assert(0 <= target_region and target_region <= 4);

    //Scan region
    for(int reg_itr= 0; reg_itr < regions_vec.size(); ++reg_itr){
      // filtering region
      std::string reg_name =  regions_names_vec[reg_itr];
//      LOG_DEBUG("reg name %s,",reg_name.c_str());
      std::string tar_reg_name =  std::string(regions[target_region]);
      if (reg_name != tar_reg_name) continue;

      // Scan nation
      for (int nat_itr= 0; nat_itr < nations_vec.size(); ++nat_itr) {
        // filtering nation
        auto n_regionkey = nations_vec[nat_itr].second;
        auto reg_regionkey = regions_vec[reg_itr];
        // n_regionkey != reg_regionkey
        if (n_regionkey.CompareEquals(reg_regionkey) == type::CmpBool::CMP_FALSE) continue;

        std::vector<oid_t> supp_key_column_ids;
        std::vector<ExpressionType> supp_expr_types;
        supp_key_column_ids.push_back(0); // I_ID
        supp_expr_types.push_back(ExpressionType::COMPARE_GREATERTHANOREQUALTO);
        auto supp_pkey_index = supplier_table->GetIndexWithOid(supplier_table_pkey_index_oid);
        // suppkey, nationkey, name, address, phone, comment
        std::vector<oid_t> supp_column_ids = {0, 1, 2, 3, 4, 5, 6};

        std::vector<type::Value > supp_key_values;
//        supp_key_values.push_back(type::ValueFactory::GetIntegerValue(0).Copy());
        //start_key
        supp_key_values.push_back(type::ValueFactory::GetVarcharValue(std::to_string(0)));

        planner::IndexScanPlan::IndexScanDesc supp_index_scan_desc(
            supp_pkey_index, supp_key_column_ids, supp_expr_types,
            supp_key_values, runtime_keys);


        planner::IndexScanPlan supp_index_scan_node(supplier_table, nullptr,
                                                    supp_column_ids,
                                                    supp_index_scan_desc);

        executor::IndexScanExecutor supp_index_scan_executor(&supp_index_scan_node, context.get());

        auto supp_lists_values = ExecuteRead(&supp_index_scan_executor);

        if (txn->GetResult() != ResultType::SUCCESS) {
          LOG_DEBUG("abort transaction");
          txn_manager.AbortTransaction(txn);
          return false;
        }
        assert(supp_lists_values.size() == 10000);

        // Scan suppliers
        for (auto supp_id = 0; supp_id< 10000; supp_id++) {
          auto supplier_ = supp_lists_values[supp_id];

          // Filtering suppliers
          auto n_nationkey = nations_vec[nat_itr].first;
          type::Value supp_nationkey = supplier_[1];
          auto supp_nationkey_ = supp_nationkey;
          //n_nationkey != supp_nationkey_
          if (n_nationkey.CompareEquals(supp_nationkey_) == type::CmpBool::CMP_FALSE) continue;

          // aggregate - finding a stock tuple having min. stock level
          //s_w_id/s_i_id
          type::Value v_zero = type::ValueFactory::GetIntegerValue(0);
          std::vector<type::Value> stock_0={v_zero, v_zero};
          //s_quantity/s_ytd/s_order_cnt/s_remote_cnt
          std::vector<type::Value> stock_1={v_zero, v_zero, v_zero, v_zero};

          type::Value supp_key = supplier_[0];
//          int supp_key_ = type::ValuePeeker::PeekInteger(supp_key);
          int supp_key_ = type::ValuePeeker::PeekInteger(supp_key.CastAs(type::Type::INTEGER));
          int16_t min_qty = std::numeric_limits<int16_t>::max();
          for (auto &it : supp_stock_map[supp_key_])  // already know
            // "mod((s_w_id*s_i_id),10000)=su_suppkey"
            // items
          {
            type::Value item_id = type::ValueFactory::GetVarcharValue(std::to_string(it.second));
            type::Value ol_w_id = type::ValueFactory::GetVarcharValue(std::to_string(it.first));

            std::vector<oid_t> stock_key_column_ids;
            std::vector<ExpressionType> stock_expr_types;
            stock_key_column_ids.push_back(0); // S_I_ID
            stock_key_column_ids.push_back(1); // S_W_ID
            stock_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
            stock_expr_types.push_back(ExpressionType::COMPARE_EQUAL);

            // S_I_ID, S_W_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT
            LOG_TRACE("getStockInfo: SELECT S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, "
                "? FROM STOCK WHERE S_I_ID = %d AND S_W_ID = %d", item_id, ol_w_id);

            std::vector<type::Value> stock_key_values;
            stock_key_values.push_back(item_id);
            stock_key_values.push_back(ol_w_id);
            std::vector<oid_t> stock_column_ids = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
            auto stock_pkey_index = stock_table->GetIndexWithOid(
                stock_table_pkey_index_oid);

            planner::IndexScanPlan::IndexScanDesc stock_index_scan_desc(
                stock_pkey_index, stock_key_column_ids, stock_expr_types,
                stock_key_values, runtime_keys);

            // Create plan node.
            planner::IndexScanPlan stock_index_scan_node(stock_table, nullptr,
                                                         stock_column_ids,
                                                         stock_index_scan_desc);

            executor::IndexScanExecutor stock_index_scan_executor(&stock_index_scan_node, context.get());

            auto stk_lists_values = ExecuteRead(&stock_index_scan_executor);

            if (txn->GetResult() != ResultType::SUCCESS) {
//              LOG_DEBUG("abort transaction");
              txn_manager.AbortTransaction(txn);
              return false;
            }

            if (stk_lists_values.size() != 1) {
              LOG_ERROR("getStockInfo return size incorrect : %lu", stk_lists_values.size());
              PL_ASSERT(false);
            }

            auto stock_ = stk_lists_values[0];

            type::Value s_s_i_id = stock_[0];
            type::Value s_s_w_id = stock_[1];
            type::Value s_s_quantity = stock_[2];
            type::Value s_s_ytd = stock_[3];
            type::Value s_s_order_cnt = stock_[4];
            type::Value s_s_remote_cnt = stock_[5];
            int s_s_w_id_ = type::ValuePeeker::PeekInteger(s_s_w_id.CastAs(type::Type::INTEGER));
            int s_s_i_id_ = type::ValuePeeker::PeekInteger(s_s_i_id.CastAs(type::Type::INTEGER));
            assert(((s_s_w_id_+1) * (s_s_i_id_+1) % 10000) == supp_key_);

            int s_s_quantity_ = type::ValuePeeker::PeekInteger(s_s_quantity);
            if (min_qty > s_s_quantity_) {
              stock_0[0] = s_s_w_id ;
              stock_0[1] = s_s_i_id ;
              stock_1[0] = s_s_quantity;
              stock_1[1] = s_s_ytd;
              stock_1[2] = s_s_order_cnt;
              stock_1[3] = s_s_remote_cnt;
            }
          }

          // fetch the (lowest stock level) item info
          type::Value lowest_item_id = stock_0[1];

          LOG_TRACE("getItemInfo: SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = %d", item_id);
          std::vector<type::Value> item_key_values;
          item_key_values.push_back(lowest_item_id);
          std::vector<oid_t> item_key_column_ids;
          std::vector<ExpressionType> item_expr_types;
          item_key_column_ids.push_back(0); // I_ID
          item_expr_types.push_back(ExpressionType::COMPARE_EQUAL);

          auto item_pkey_index = item_table->GetIndexWithOid(
              item_table_pkey_index_oid);
          std::vector<oid_t> item_column_ids = {0, 1, 2, 3, 4}; // I_NAME, I_PRICE, I_DATA
          planner::IndexScanPlan::IndexScanDesc item_index_scan_desc(
              item_pkey_index, item_key_column_ids, item_expr_types,
              item_key_values, runtime_keys);

          planner::IndexScanPlan item_index_scan_node(item_table, nullptr,
                                                      item_column_ids,
                                                      item_index_scan_desc);
          executor::IndexScanExecutor item_index_scan_executor(&item_index_scan_node, context.get());

          auto item_lists_values = ExecuteRead(&item_index_scan_executor);

          if (txn->GetResult() != ResultType::SUCCESS) {
            LOG_DEBUG("abort transaction");
            txn_manager.AbortTransaction(txn);
            return false;
          }

          if (item_lists_values.size() != 1) {
            LOG_ERROR("getItemInfo return size incorrect : %lu", item_lists_values.size());
            PL_ASSERT(false);
          }

          auto item_ = item_lists_values[0];

          type::Value i_data = item_[2];
          //  filtering item (i_data like '%b')
          std::string itm_idata = i_data.ToString();
          auto found = itm_idata.find('b');
          if (found != std::string::npos) continue;

          // XXX. read-mostly txn: update stock or item here
          oid_t s_s_quantity_ = type::ValuePeeker::PeekInteger(stock_1[0]);
          if (s_s_quantity_< 10) {
            type::Value s_quantity = stock_1[0].Add(type::ValueFactory::GetIntegerValue(50));
            type::Value s_ytd = stock_1[1] ;
            type::Value s_order_cnt = stock_1[2] ;
            type::Value s_remote_cnt = stock_1[3] ;

            type::Value ol_w_id = stock_0[0];
            type::Value item_id = stock_0[1];

            LOG_TRACE("updateStock: UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?");
            auto stock_pkey_index = stock_table->GetIndexWithOid(
                stock_table_pkey_index_oid);
            std::vector<oid_t> stock_key_column_ids;
            std::vector<ExpressionType> stock_expr_types;
            stock_key_column_ids.push_back(0); // S_I_ID
            stock_key_column_ids.push_back(1); // S_W_ID
            stock_expr_types.push_back( ExpressionType::COMPARE_EQUAL);
            stock_expr_types.push_back( ExpressionType::COMPARE_EQUAL);

            std::vector<type::Value > stock_update_key_values;
            stock_update_key_values.push_back(item_id);
            stock_update_key_values.push_back(ol_w_id);

            // S_QUANTITY, S_DIST_%02d, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA
//            std::vector<oid_t> stock_column_ids = {2, oid_t(3 + district_id), 13, 14, 15, 16};

            std::vector<oid_t> stock_update_column_ids = {2, 13, 14, 15}; // S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT
            planner::IndexScanPlan::IndexScanDesc stock_update_index_scan_desc(
                stock_pkey_index, stock_key_column_ids, stock_expr_types,
                stock_update_key_values, runtime_keys);

            // Create plan node.
            planner::IndexScanPlan stock_update_index_scan_node(stock_table, nullptr,
                                                                stock_update_column_ids,
                                                                stock_update_index_scan_desc);

            executor::IndexScanExecutor stock_update_index_scan_executor(&stock_update_index_scan_node, context.get());

            TargetList stock_target_list;
            DirectMapList stock_direct_map_list;

            // Update the last attribute
            for (oid_t col_itr = 0; col_itr < 17; col_itr++) {
              if (col_itr != 2 && col_itr != 13 && col_itr != 14 && col_itr != 15) {
                stock_direct_map_list.emplace_back(col_itr,
                                                   std::pair<oid_t, oid_t>(0, col_itr));
              }
            }
            stock_target_list.emplace_back(
                2, expression::ExpressionUtil::ConstantValueFactory(s_quantity));
            stock_target_list.emplace_back(
                13, expression::ExpressionUtil::ConstantValueFactory(s_ytd));
            stock_target_list.emplace_back(
                14, expression::ExpressionUtil::ConstantValueFactory(s_order_cnt));
            stock_target_list.emplace_back(
                15, expression::ExpressionUtil::ConstantValueFactory(s_remote_cnt));

            std::unique_ptr<const planner::ProjectInfo> stock_project_info(
                new planner::ProjectInfo(std::move(stock_target_list),
                                         std::move(stock_direct_map_list)));
            planner::UpdatePlan stock_update_node(stock_table, std::move(stock_project_info));

            executor::UpdateExecutor stock_update_executor(&stock_update_node, context.get());

            stock_update_executor.AddChild(&stock_update_index_scan_executor);

            ExecuteUpdate(&stock_update_executor);

            if (txn->GetResult() != ResultType::SUCCESS) {
              LOG_TRACE("abort transaction");
              txn_manager.AbortTransaction(txn);
              return false;
            }
          }


//
//
//          std::cout <<  "supplier: " <<    supp_key   << ","
//                    << "supplier_name: " <<  supplier_.SU_NAME               << ","
//                    << "nation_name:" << nat.N_NAME                  << ","
//                    << "item_id:" << item_.I_ID                    << ","
//                    << "item_name:" << item_.I_NAME                << std::endl;
        }
      }
    }
  }




  assert(txn->GetResult() == ResultType::SUCCESS);

  auto result = txn_manager.CommitTransaction(txn);

  if (result == ResultType::SUCCESS) {
//    delete txn;
//    txn = nullptr;
//    LOG_DEBUG("success committed.");
    return true;
  } else {
//    delete txn;
//    txn = nullptr;

    return false;
  }

}


}
}
}
