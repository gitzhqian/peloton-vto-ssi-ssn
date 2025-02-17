//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_workload.cpp
//
// Identification: src/main/ycsb/ycsb_workload.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "benchmark/ycsb/ycsb_workload.h"

#include <include/executor/hybrid_scan_executor.h>
#include <include/planner/hybrid_scan_plan.h>
#include <papi.h>
#include <sys/utsname.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <ctime>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <regex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "benchmark/ycsb/ycsb_configuration.h"
#include "benchmark/ycsb/ycsb_loader.h"
#include "catalog/manager.h"
#include "catalog/schema.h"
#include "common/generator.h"
#include "common/logger.h"
#include "common/platform.h"
#include "common/timer.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/abstract_executor.h"
#include "executor/executor_context.h"
#include "executor/index_scan_executor.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "executor/materialization_executor.h"
#include "executor/update_executor.h"
#include "expression/abstract_expression.h"
#include "expression/comparison_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/expression_util.h"
#include "expression/tuple_value_expression.h"
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


/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////

volatile bool is_running = true;

PadInt *abort_counts;
PadInt *commit_counts;
//static constexpr int PAPI_EVENT_COUNT = 3;
#define NUM_EVENTS 7
static constexpr int PAPI_CACHE_EVENT_COUNT = 3;
static constexpr int PAPI_INST_EVENT_COUNT = 4;
thread_local size_t num_rw_ops = 0;


void PinToCore(size_t core) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core, &cpuset);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}

void InitPAPI() {
  int retval = PAPI_OK;

  retval = PAPI_library_init(PAPI_VER_CURRENT);
  if(retval != PAPI_VER_CURRENT && retval > 0) {
    fprintf(stderr,"PAPI library version mismatch!\n");
    exit(1);
  }

  if (retval < 0) {
    fprintf(stderr, "PAPI failed to start (1): %s\n", PAPI_strerror(retval));
    exit(1);
  }

  retval = PAPI_is_initialized();


  if (retval != PAPI_LOW_LEVEL_INITED) {
    fprintf(stderr, "PAPI failed to start (2): %s\n", PAPI_strerror(retval));
    exit(1);
  }

  return;
}

void print_environment()
{
  std::time_t now = std::time(nullptr);
  uint64_t num_cpus = 0;
  std::string cpu_type;
  std::string cache_size;

  std::ifstream cpuinfo("/proc/cpuinfo", std::ifstream::in);

  if(!cpuinfo.good())
  {
    num_cpus = 0;
    cpu_type = "Could not open /proc/cpuinfo";
    cache_size = "Could not open /proc/cpuinfo";
  }
  else
  {
    std::string line;
    while(!getline(cpuinfo, line).eof())
    {
      auto sep_pos = line.find(':');
      if(sep_pos == std::string::npos)
      {
        continue;
      }
      else
      {
        std::string key = std::regex_replace(std::string(line, 0, sep_pos), std::regex("\\t+$"), "");
        std::string val = sep_pos == line.size()-1 ? "" : std::string(line, sep_pos+2, line.size());
        if(key.compare("model name") == 0)
        {
          ++num_cpus;
          cpu_type = val;
        }
        else if(key.compare("cache size") == 0)
        {
          cache_size = val;
        }
      }
    }
  }
  cpuinfo.close();

  std::string kernel_version;
  struct utsname uname_buf;
  if(uname(&uname_buf) == -1)
  {
    kernel_version = "Unknown";
  }
  else
  {
    kernel_version = std::string(uname_buf.sysname) + " " + std::string(uname_buf.release);
  }

  std::cout << "Environment:" << "\n"
            << "\tTime: " << std::asctime(std::localtime(&now))
            << "\tCPU: " << num_cpus << " * " << cpu_type << "\n"
            << "\tCPU Cache: " << cache_size << "\n"
            << "\tKernel: " << kernel_version << std::endl;
}

void RunBackend(const size_t thread_id) {

  PinToCore(thread_id);

  PadInt &execution_count_ref = abort_counts[thread_id];
  PadInt &transaction_count_ref = commit_counts[thread_id];

  ZipfDistribution zipf((state.scale_factor * 1000) - 1,
                        state.zipf_theta);

  FastRandom rng(rand());

  // backoff
  uint32_t backoff_shifts = 0;

  while (true) {
    if (is_running == false) {
      break;
    }
    size_t num_rw_ops_snap = num_rw_ops;

    while (RunMixed(thread_id, zipf, rng) == false) {
      if (is_running == false) {
        break;
      }
      num_rw_ops_snap = num_rw_ops;
      execution_count_ref.data++;
      // backoff
      if (state.exp_backoff) {
        if (backoff_shifts < 13) {
          ++backoff_shifts;
        }
        uint64_t sleep_duration = 1UL << backoff_shifts;
        sleep_duration *= 100;
        std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
      }
    }
    backoff_shifts >>= 1;
//    transaction_count_ref.data++;
    transaction_count_ref.data += num_rw_ops - num_rw_ops_snap;
  }
}
void RunWarmupBackend(const size_t thread_id) {

  PinToCore(thread_id);

  PadInt &execution_count_ref = abort_counts[thread_id];
  PadInt &transaction_count_ref = commit_counts[thread_id];

  ZipfDistribution zipf((state.scale_factor * 1000) - 1,
                        state.zipf_theta);

  // backoff
  uint32_t backoff_shifts = 0;

  if (state.scan_only){
    LOG_DEBUG("scan_only.");
    while (true) {
      if (is_running == false) {
        break;
      }

      while (RunScanSimpleMixed(thread_id,zipf) == false) {
        if (is_running == false) {
          break;
        }
        // backoff
        if (state.exp_backoff) {
          if (backoff_shifts < 13) {
            ++backoff_shifts;
          }
          uint64_t sleep_duration = 1UL << backoff_shifts;
          sleep_duration *= 100;
          std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
        }
      }

      backoff_shifts >>= 1;
    }
  }else{
    ZipfDistribution zipf((state.scale_factor * 1000) - 1,
                          state.zipf_theta);

    FastRandom rng(rand());

    while (true) {
      if (is_running == false) {
        break;
      }

      while (RunMixed(thread_id, zipf, rng) == false) {
        if (is_running == false) {
          break;
        }
        // backoff
        if (state.exp_backoff) {
          if (backoff_shifts < 13) {
            ++backoff_shifts;
          }
          uint64_t sleep_duration = 1UL << backoff_shifts;
          sleep_duration *= 100;
          std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
        }
      }

      backoff_shifts >>= 1;
    }
  }
}

void RunScanBackend(const size_t thread_id) {

  PinToCore(thread_id);

  PadInt &execution_count_ref = abort_counts[thread_id];
  PadInt &transaction_count_ref = commit_counts[thread_id];

  ZipfDistribution zipf((state.scale_factor * 1000) - 1,
                        state.zipf_theta);

  // backoff
  uint32_t backoff_shifts = 0;

  if (state.scan_only){
    LOG_DEBUG("scan_only.");
    while (true) {
      if (is_running == false) {
        break;
      }

      while (RunScanSimpleMixed(thread_id, zipf) == false) {
        if (is_running == false) {
          break;
        }
        execution_count_ref.data++;
        // backoff
        if (state.exp_backoff) {
          if (backoff_shifts < 13) {
            ++backoff_shifts;
          }
          uint64_t sleep_duration = 1UL << backoff_shifts;
          sleep_duration *= 100;
          std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
        }
      }
      backoff_shifts >>= 1;
      transaction_count_ref.data ++;
    }
  }else{
    while (true) {
      if (is_running == false) {
        break;
      }
      while (RunScanMixed(thread_id) == false) {
        if (is_running == false) {
          break;
        }
        execution_count_ref.data++;
        // backoff
        if (state.exp_backoff) {
          if (backoff_shifts < 13) {
            ++backoff_shifts;
          }
          uint64_t sleep_duration = 1UL << backoff_shifts;
          sleep_duration *= 100;
          std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
        }
      }
      backoff_shifts >>= 1;
      transaction_count_ref.data++;
    }
  }

}
void RunWarmupWorkload() {
  std::vector<std::thread> thread_group;
  size_t num_threads = state.backend_count;

  int Events[PAPI_INST_EVENT_COUNT] = {PAPI_L1_DCM, PAPI_L2_TCM, PAPI_L3_TCM, PAPI_BR_INS};
  //int Events[PAPI_EVENT_COUNT] = {PAPI_TOT_CYC, PAPI_TOT_INS, PAPI_BR_MSP};
  int EventSet = PAPI_NULL;
  long long values[PAPI_INST_EVENT_COUNT];
  int retval;
  if(enable_papi){
    InitPAPI();
/* Allocate space for the new eventset and do setup */
    retval = PAPI_create_eventset(&EventSet);
/* Add Flops and total cycles to the eventset */
    retval = PAPI_add_events(EventSet,Events,PAPI_INST_EVENT_COUNT);
/* Start the counters */
    retval = PAPI_start(EventSet);
    assert(retval == PAPI_OK);
  }

  // Launch a group of threads
  size_t profile_round = (size_t)(state.duration / state.profile_duration);
  for (size_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::move(std::thread(RunScanBackend, thread_itr)));
  }


  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
    std::this_thread::sleep_for(std::chrono::milliseconds(int(1000)));

  }

  /*Stop counters and store results in values */
  retval = PAPI_stop(EventSet,values);
  assert(retval == PAPI_OK);
  std::cout << "L1 miss = " << values[0] << "\n";
  std::cout << "L2 miss = " << values[1] << "\n";
  std::cout << "L3 miss = " << values[2] << "\n";
  std::cout << "Total branch = " << values[3] << "\n";
  //std::cout << "Total cycle = " << values[0] << "\n";
  //std::cout << "Total instruction = " << values[1] << "\n";
  //std::cout << "Total branch misprediction = " << values[2] << "\n";
  PAPI_shutdown();


  is_running = false;

  // Join the threads with the main thread
  for (size_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }

  //Restore the states
  is_running = true;
  LOG_INFO("Warmed up");
}
void RunWorkload() {

  // Execute the workload to build the log
  std::vector<std::thread> thread_group;
  size_t num_threads = state.backend_count;

  abort_counts = new PadInt[num_threads];
  PL_MEMSET(abort_counts, 0, sizeof(PadInt) * num_threads);

  commit_counts = new PadInt[num_threads];
  PL_MEMSET(commit_counts, 0, sizeof(PadInt) * num_threads);

  size_t profile_round = (size_t)(state.duration / state.profile_duration);

  PadInt **abort_counts_profiles = new PadInt *[profile_round];
  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
    abort_counts_profiles[round_id] = new PadInt[num_threads];
  }

  PadInt **commit_counts_profiles = new PadInt *[profile_round];
  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
    commit_counts_profiles[round_id] = new PadInt[num_threads];
  }

  // Launch a group of threads
//  size_t start_thread = num_threads * state.scan_rate;
  size_t start_thread = 0;
//  for (size_t thread_itr = start_thread; thread_itr < num_threads; ++thread_itr) {
//    thread_group.push_back(std::move(std::thread(RunScanBackend, thread_itr)));
//  }
  for (size_t thread_itr = start_thread; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::move(std::thread(RunBackend, thread_itr)));
  }

  //////////////////////////////start ///////////////////////////////////////
  oid_t last_tile_group_id = 0;
  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(int(state.profile_duration * 1000)));
    PL_MEMCPY(abort_counts_profiles[round_id], abort_counts,
              sizeof(PadInt) * num_threads);
    PL_MEMCPY(commit_counts_profiles[round_id], commit_counts,
              sizeof(PadInt) * num_threads);

    auto& manager = catalog::Manager::GetInstance();
    oid_t current_tile_group_id = manager.GetCurrentTileGroupId();
    if (round_id != 0) {
      state.profile_memory.push_back(current_tile_group_id - last_tile_group_id);
    }
    last_tile_group_id = current_tile_group_id;

  }

  state.profile_memory.push_back(state.profile_memory.at(state.profile_memory.size() - 1));

  is_running = false;

  // Join the threads with the main thread
  for (size_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }

  //calculate the throughput and abort rate for the first round.
  uint64_t total_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_commit_count += commit_counts_profiles[0][i].data;
  }

  uint64_t total_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_abort_count += abort_counts_profiles[0][i].data;
  }

  uint64_t scan_commit_count = 0;
  for (size_t i = 0; i < start_thread; ++i) {
    scan_commit_count += commit_counts_profiles[0][i].data;
  }

  state.profile_throughput.push_back(total_commit_count * 1.0 /
                                     state.profile_duration);
  state.profile_abort_rate.push_back(total_abort_count * 1.0 /
                                     total_commit_count);

  // calculate the throughput and abort rate for the remaining rounds.
  for (size_t round_id = 0; round_id < profile_round - 1; ++round_id) {
    scan_commit_count = 0;
    for (size_t i = 0; i < start_thread; ++i) {
      scan_commit_count += commit_counts_profiles[round_id + 1][i].data -
                           commit_counts_profiles[round_id][i].data;
    }

    total_commit_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
      total_commit_count += commit_counts_profiles[round_id + 1][i].data -
                            commit_counts_profiles[round_id][i].data;
    }

    total_abort_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
      total_abort_count += abort_counts_profiles[round_id + 1][i].data -
                           abort_counts_profiles[round_id][i].data;
    }

    state.profile_throughput.push_back(total_commit_count * 1.0 /
                                       state.profile_duration);
    state.profile_abort_rate.push_back(total_abort_count * 1.0 /
                                       total_commit_count);
  }

  //////////////////////////////////////////////////
  // calculate the aggregated throughput and abort rate.
  total_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_commit_count += commit_counts_profiles[profile_round - 1][i].data;
  }

  total_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_abort_count += abort_counts_profiles[profile_round - 1][i].data;
  }

  scan_commit_count = 0;
  for (size_t i = 0; i < start_thread; ++i) {
    scan_commit_count += commit_counts_profiles[profile_round - 1][i].data;
  }

  state.throughput = total_commit_count * 1.0 / state.duration;
  state.abort_rate = total_abort_count * 1.0 / total_commit_count;
  state.scan_latency = state.duration / (scan_commit_count * 1.0);
  //////////////////////////////////////////////////

  // cleanup everything.
  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
    delete[] abort_counts_profiles[round_id];
    abort_counts_profiles[round_id] = nullptr;
  }

  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
    delete[] commit_counts_profiles[round_id];
    commit_counts_profiles[round_id] = nullptr;
  }

  delete[] abort_counts_profiles;
  abort_counts_profiles = nullptr;
  delete[] commit_counts_profiles;
  commit_counts_profiles = nullptr;

  delete[] abort_counts;
  abort_counts = nullptr;
  delete[] commit_counts;
  commit_counts = nullptr;

}

/**
 * paper tests
 * workload1: scan->insert
 */
void RunWorkload1() {
  std::cout << "Workload1 started." << std::endl;
  stopwatch_t sw;
  sw.start();

  PinToCore(0);

  std::cout << "Workload1 scan started." << std::endl;
  //1. scan from YCSB, projectivity , selectivity , loop 1000
  // Column ids to be added to logical tile after scan. projectivity
  int end_column_idx = state.column_count * state.projectivity;
  std::cout << "YCSB column_count." << state.column_count << std::endl;
  std::cout << "YCSB projectivity." << state.projectivity << std::endl;
  std::vector<oid_t> column_ids;
  column_ids.clear();
  for (int j = 0; j < end_column_idx; ++j) {
    column_ids.push_back(j);
  }

  int tuple_total_count = user_table->GetTupleCount();
  oid_t pred_constant = tuple_total_count * state.selectivity;
  std::cout << "YCSB total tuple count." << tuple_total_count << std::endl;
  std::cout << "YCSB selectivity." << state.selectivity << std::endl;

  for (int i = 0; i < 1000; ++i) {
    auto &txn_manager_scan = concurrency::TransactionManagerFactory::GetInstance();
    concurrency::Transaction *txn_scan =  txn_manager_scan.BeginTransaction(0);
    txn_scan->SetDeclaredReadOnly();
    std::unique_ptr<executor::ExecutorContext> context_scan(new executor::ExecutorContext(txn_scan));
    // Set of tuple_ids that will satisfy the predicate in our test cases.
    // left 0, right 1
    oid_t pred_tuple_idx = 0;
    oid_t pred_column_idx = 0;

    //left, column 0
    auto tup_val_exp =  new expression::TupleValueExpression(type::Type::INTEGER,
                                                             pred_tuple_idx, pred_column_idx);
    auto const_val_exp = new expression::ConstantValueExpression(
        type::ValueFactory::GetIntegerValue(pred_constant));

    auto predicate = new expression::ComparisonExpression(
        ExpressionType::COMPARE_LESSTHAN, tup_val_exp, const_val_exp);

    // Create plan node.
    planner::SeqScanPlan node(user_table, predicate, column_ids);
    // Create executor node.
    executor::SeqScanExecutor executor(&node, context_scan.get());
//    int expected_num_tiles = user_table->GetTileGroupCount();

    std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;
    executor.Init();
    while(executor.Execute()) {
      std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
      if(result_tile == nullptr) {
        break;
      }
      result_tiles.emplace_back(result_tile.release());
    }
//    int result_tile_count = result_tiles.size();

    // Check correctness of result tiles.
//    for (int i = 0; i < result_tile_count; ++i) {
//      assert(column_ids.size() == result_tiles[i]->GetColumnCount());
//
//      // Verify values.
//      for (oid_t new_tuple_id : *(result_tiles[i])) {
//        // We divide by 10 because we know how PopulatedValue() computes.
//        // Bad style. Being a bit lazy here...
//
//        type::Value value1 = (result_tiles[i]->GetValue(new_tuple_id, 0));
//        type::Value value2 = type::ValueFactory::GetIntegerValue(pred_constant);
//
//        assert(value1.CompareLessThan(value2) == type::CMP_TRUE);
//      }
//    }

    txn_manager_scan.CommitTransaction(txn_scan);
    result_tiles.clear();
  }

  std::cout << "Workload1 insert1 started." << std::endl;
  //2. insert into YCSB, 10million tuples
  auto table_schema = user_table->GetSchema();
  std::unique_ptr<type::AbstractPool> pool(new type::EphemeralPool());
  const bool allocate = true;
  oid_t column_count = state.column_count + 1;

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  int interval = 1000*1000;
  int insert_total = 10*1000*1000;
  int insert_count = insert_total/interval;
  for (int j = 0; j < insert_count; ++j) {
    int from_tuple_id = user_table->GetTupleCount();
    int end_tuple_id = from_tuple_id + interval;

    concurrency::Transaction *txn =  txn_manager.BeginTransaction(0);
    std::unique_ptr<executor::ExecutorContext> context(new executor::ExecutorContext(txn));
    for (int rowid = from_tuple_id; rowid < end_tuple_id; ++rowid) {
      std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(table_schema, allocate));
      auto primary_key_value = type::ValueFactory::GetIntegerValue(rowid);
      tuple->SetValue(0, primary_key_value, pool.get());

      auto key_value = type::ValueFactory::GetIntegerValue(rowid);
      for (oid_t col_itr = 1; col_itr < column_count; col_itr++) {
        tuple->SetValue(col_itr, key_value, nullptr);
      }

      planner::InsertPlan node(user_table, std::move(tuple));
      executor::InsertExecutor executor(&node, context.get());
      executor.Execute();
    }
    txn_manager.CommitTransaction(txn);

    int ycsb_tuple_count = user_table->GetTupleCount();
    std::cout << "YCSB table total tuple counts: " << ycsb_tuple_count << std::endl;
  }

  auto elapsed = sw.elapsed<std::chrono::seconds>();
  std::cout << "Workload1 finished in " << elapsed << " seconds" << std::endl;

  int ycsb_tuple_count = user_table->GetTupleCount();
  std::cout << "YCSB table total tuple counts: " << ycsb_tuple_count << std::endl;
}
/**
 * paper tests
 * workload2: scan
 * laod 1million
 * run 1000 scan
 */
void RunWorkload2Index() {
  std::cout << "RunWorkload2Index started." << std::endl;
  assert(state.index_scan);

  //1. insert into YCSB, 1million tuples
  PinToCore(0);

  ZipfDistribution zipf((state.scale_factor * 1000) - 1,
                        state.zipf_theta);

  //2. scan from YCSB, projectivity , selectivity , loop 1000
  // Column ids to be added to logical tile after scan. projectivity
  int end_column_idx = state.column_count * state.projectivity;
  std::cout << "YCSB column_count." << state.column_count << std::endl;
  std::cout << "YCSB projectivity." << state.projectivity << std::endl;
  std::vector<oid_t> column_ids;
  std::vector<oid_t> full_column_ids;
  column_ids.clear();
  for (int j = 0; j < end_column_idx; ++j) {
    column_ids.push_back(j);
  }
  for (oid_t col_itr = 0; col_itr < state.column_count; col_itr++) {
    full_column_ids.push_back(col_itr);
  }

  stopwatch_t sw;
  sw.start();

  int tuple_total_count = user_table->GetTupleCount();
//  oid_t pred_constant = tuple_total_count * state.selectivity;
  std::cout << "YCSB selectivity." << state.selectivity << std::endl;
  std::cout << "YCSB total tuple count." << tuple_total_count << std::endl;

  for (int i = 0; i < 1000; ++i) {
    auto &txn_manager_scan = concurrency::TransactionManagerFactory::GetInstance();
    concurrency::Transaction *txn_scan =  txn_manager_scan.BeginTransaction(0);
    txn_scan->SetDeclaredReadOnly();
    std::unique_ptr<executor::ExecutorContext> context_scan(new executor::ExecutorContext(txn_scan));

    //if index scan
    //index
    std::vector<expression::AbstractExpression *> runtime_keys;
    std::vector<oid_t> key_column_ids;
    std::vector<ExpressionType> expr_types;
    key_column_ids.push_back(0);
    expr_types.push_back(ExpressionType::COMPARE_LESSTHAN);
    std::vector<type::Value> values;

    auto lookup_key = 0;
    lookup_key = zipf.GetNextNumber();
    values.push_back(type::ValueFactory::GetIntegerValue(lookup_key).Copy());

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
                                                    context_scan.get());

    std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;
    std::vector<std::vector<type::Value >> logical_tile_values;

    //if layout row or column
//    if (state.layout_mode != LAYOUT_TYPE_HYBRID){
      index_scan_executor.Init();
      while(index_scan_executor.Execute()) {
        std::unique_ptr<executor::LogicalTile> result_tile(index_scan_executor.GetOutput());
        if(result_tile == nullptr) {
          break;
        }
        result_tiles.emplace_back(result_tile.release());
      }
      size_t sum = 0;
      for (auto &result_tile : result_tiles) {
        if (result_tile != nullptr){
            auto column_count = result_tile->GetColumnCount();
            for (oid_t tuple_id : *result_tile) {
              expression::ContainerTuple<executor::LogicalTile> cur_tuple(result_tile.get(),
                                                                          tuple_id);
              std::vector<type::Value > tuple_values;
              for (oid_t column_itr = 0; column_itr < column_count; column_itr++){
                auto value = cur_tuple.GetValue(column_itr);
                tuple_values.push_back(value);
              }
              // Move the tuple list
              logical_tile_values.push_back(std::move(tuple_values));
            }

          sum += result_tile->GetTupleCount();
        }
      }
      LOG_INFO("result tiles have %d tuples", (int)sum);
      int result_tile_count = result_tiles.size();

//    }else {
//      //if layout row+column
//      /////////////////////////////////////////////////////////
//      // MATERIALIZE
//      /////////////////////////////////////////////////////////
//
//      // Create and set up materialization executor
//      std::vector<catalog::Column> output_columns;
//      std::unordered_map<oid_t, oid_t> old_to_new_cols;
//      oid_t col_itr = 0;
//      for (auto column_id : column_ids) {
//        auto column = catalog::Column(type::Type::INTEGER,
//                                      type::Type::GetTypeSize(type::Type::INTEGER),
//                                      std::to_string(column_id), true);
//        output_columns.push_back(column);
//
//        old_to_new_cols[col_itr] = col_itr;
//
//        col_itr++;
//      }
//
//      std::shared_ptr<const catalog::Schema> output_schema(
//          new catalog::Schema(output_columns));
//      bool physify_flag = true;  // is going to create a physical tile
//      planner::MaterializationPlan mat_node(old_to_new_cols, output_schema,
//                                            physify_flag);
//
//      executor::MaterializationExecutor mat_executor(&mat_node, nullptr);
//
//      mat_executor.AddChild(&index_scan_executor);
//      /////////////////////////////////////////////////////////
//      // EXECUTE
//      /////////////////////////////////////////////////////////
//      bool status = false;
//      std::vector<executor::AbstractExecutor *> executors;
//      executors.push_back(&mat_executor);
//
//      // Run all the executors
//      for (auto executor : executors) {
//        status = executor->Init();
//        if (status == false) {
//          throw Exception("Init failed");
//        }
//
//        while (executor->Execute() == true) {
//          std::unique_ptr<executor::LogicalTile> result_tile(executor->GetOutput());
//          result_tiles.emplace_back(result_tile.release());
//        }
//
//        size_t sum = 0;
//        for (auto &result_tile : result_tiles) {
//          if (result_tile != nullptr) {
//              auto column_count = result_tile->GetColumnCount();
//              for (oid_t tuple_id : *result_tile) {
//                expression::ContainerTuple<executor::LogicalTile> cur_tuple(result_tile.get(),
//                                                                            tuple_id);
//                std::vector<type::Value > tuple_values;
//                for (oid_t column_itr = 0; column_itr < column_count; column_itr++){
//                  auto value = cur_tuple.GetValue(column_itr);
//                  tuple_values.push_back(value);
//                }
//                // Move the tuple list
//                logical_tile_values.push_back(std::move(tuple_values));
//              }
//
//            sum += result_tile->GetTupleCount();
//          }
//        }
//
//        LOG_INFO("result tiles have %d tuples", (int)sum);
//
//        // Execute stuff
//        executor->Execute();
//      }
//    }

    txn_manager_scan.CommitTransaction(txn_scan);
    result_tiles.clear();
    logical_tile_values.clear();
  }

  auto elapsed = sw.elapsed<std::chrono::seconds>();
  std::cout << "RunWorkload2Index finished in " << elapsed << " seconds" << std::endl;

  int ycsb_tuple_count = user_table->GetTupleCount();
  std::cout << "YCSB table total tuple counts: " << ycsb_tuple_count << std::endl;
}
/**
 * paper tests
 * workload2: scan
 * laod 1million
 * run 1000 scan
 */
void RunWorkload2() {
  if (state.index_scan){
    RunWorkload2Index();
  }else{
    std::cout << "Workload2 started." << std::endl;

    //1. insert into YCSB, 1million tuples
    PinToCore(0);

    ZipfDistribution zipf((state.scale_factor * 1000) - 1,
                          state.zipf_theta);

    //2. scan from YCSB, projectivity , selectivity , loop 1000
    // Column ids to be added to logical tile after scan. projectivity
    int end_column_idx = state.column_count * state.projectivity;
    std::cout << "YCSB column_count." << state.column_count << std::endl;
    std::cout << "YCSB projectivity." << state.projectivity << std::endl;
    std::vector<oid_t> column_ids;
    column_ids.clear();
    for (int j = 0; j < end_column_idx; ++j) {
      column_ids.push_back(j);
    }

    stopwatch_t sw;
    sw.start();

    int tuple_total_count = user_table->GetTupleCount();
    //oid_t pred_constant = tuple_total_count * state.selectivity;
    std::cout << "YCSB selectivity." << state.selectivity << std::endl;
    std::cout << "YCSB total tuple count." << tuple_total_count << std::endl;

    for (int i = 0; i < 1000; ++i) {
      auto &txn_manager_scan = concurrency::TransactionManagerFactory::GetInstance();
      concurrency::Transaction *txn_scan =  txn_manager_scan.BeginTransaction(0);
      txn_scan->SetDeclaredReadOnly();
      std::unique_ptr<executor::ExecutorContext> context_scan(new executor::ExecutorContext(txn_scan));

      auto lookup_key = 0;
      lookup_key = zipf.GetNextNumber();

      //if sequentical scan
      // Set of tuple_ids that will satisfy the predicate in our test cases.
      // left 0, right 1
      oid_t pred_tuple_idx = 0;
      oid_t pred_column_idx = 0;
      //left, column 0
      auto tup_val_exp =  new expression::TupleValueExpression(type::Type::INTEGER,
                                                               pred_tuple_idx, pred_column_idx);
      auto const_val_exp = new expression::ConstantValueExpression(
          type::ValueFactory::GetIntegerValue(lookup_key));
      auto predicate = new expression::ComparisonExpression(
          ExpressionType::COMPARE_LESSTHAN, tup_val_exp, const_val_exp);
      // Create plan node.
      planner::SeqScanPlan node(user_table, predicate, column_ids);
      // Create executor node.
      executor::SeqScanExecutor seq_scan_executor(&node, context_scan.get());

      std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;
      std::vector<std::vector<type::Value >> logical_tile_values;

      //if layout row or column
//      if (state.layout_mode != LAYOUT_TYPE_HYBRID){
        seq_scan_executor.Init();
        while(seq_scan_executor.Execute()) {
          std::unique_ptr<executor::LogicalTile> result_tile(seq_scan_executor.GetOutput());
          if(result_tile == nullptr) {
            break;
          }
          result_tiles.emplace_back(result_tile.release());
        }
        size_t sum = 0;
        for (auto &result_tile : result_tiles) {
          if (result_tile != nullptr) {
              auto column_count = result_tile->GetColumnCount();
              for (oid_t tuple_id : *result_tile) {
                expression::ContainerTuple<executor::LogicalTile> cur_tuple(result_tile.get(),
                                                                            tuple_id);
                std::vector<type::Value > tuple_values;
                for (oid_t column_itr = 0; column_itr < column_count; column_itr++){
                  auto value = cur_tuple.GetValue(column_itr);
                  tuple_values.push_back(value);
                }
                // Move the tuple list
                logical_tile_values.push_back(std::move(tuple_values));
              }

            sum += result_tile->GetTupleCount();
          }
        }
        LOG_INFO("result tiles have %d tuples", (int)sum);
        int result_tile_count = result_tiles.size();

//      }else {
//        //if layout row+column
//        /////////////////////////////////////////////////////////
//        // MATERIALIZE
//        /////////////////////////////////////////////////////////
//
//        // Create and set up materialization executor
//        std::vector<catalog::Column> output_columns;
//        std::unordered_map<oid_t, oid_t> old_to_new_cols;
//        oid_t col_itr = 0;
//        for (auto column_id : column_ids) {
//          auto column = catalog::Column(type::Type::INTEGER,
//                                        type::Type::GetTypeSize(type::Type::INTEGER),
//                                        std::to_string(column_id), true);
//          output_columns.push_back(column);
//
//          old_to_new_cols[col_itr] = col_itr;
//
//          col_itr++;
//        }
//
//        std::shared_ptr<const catalog::Schema> output_schema(
//            new catalog::Schema(output_columns));
//        bool physify_flag = true;  // is going to create a physical tile
//        planner::MaterializationPlan mat_node(old_to_new_cols, output_schema,
//                                              physify_flag);
//
//        executor::MaterializationExecutor mat_executor(&mat_node, nullptr);
//
//        mat_executor.AddChild(&seq_scan_executor);
//        /////////////////////////////////////////////////////////
//        // EXECUTE
//        /////////////////////////////////////////////////////////
//        bool status = false;
//        std::vector<executor::AbstractExecutor *> executors;
//        executors.push_back(&mat_executor);
//
//        // Run all the executors
//        for (auto executor : executors) {
//          status = executor->Init();
//
//          if (status == false) {
//            throw Exception("Init failed");
//          }
//
//          while (executor->Execute() == true) {
//            std::unique_ptr<executor::LogicalTile> result_tile(executor->GetOutput());
//            result_tiles.emplace_back(result_tile.release());
//          }
//
//          size_t sum = 0;
//          for (auto &result_tile : result_tiles) {
//            if (result_tile != nullptr) {
//              auto column_count = result_tile->GetColumnCount();
//              for (oid_t tuple_id : *result_tile) {
//                expression::ContainerTuple<executor::LogicalTile> cur_tuple(result_tile.get(),
//                                                                            tuple_id);
//                std::vector<type::Value > tuple_values;
//                for (oid_t column_itr = 0; column_itr < column_count; column_itr++){
//                  auto value = cur_tuple.GetValue(column_itr);
//                  tuple_values.push_back(value);
//                }
//                // Move the tuple list
//                logical_tile_values.push_back(std::move(tuple_values));
//              }
//
//              sum += result_tile->GetTupleCount();
//            }
//          }
//
//          LOG_INFO("result tiles have %d tuples", (int)sum);
//
//          // Execute stuff
//          executor->Execute();
//        }
//      }

      txn_manager_scan.CommitTransaction(txn_scan);
      result_tiles.clear();
      logical_tile_values.clear();
    }

    auto elapsed = sw.elapsed<std::chrono::seconds>();
    std::cout << "Workload2 finished in " << elapsed << " seconds" << std::endl;

    int ycsb_tuple_count = user_table->GetTupleCount();
    std::cout << "YCSB table total tuple counts: " << ycsb_tuple_count << std::endl;
  }
}

/**
 * paper tests
 * workload3: read
 * load 1 million
 * run 10*1000*1000 reads
 */
void RunWorkload3() {
    std::cout << "Workload3 started." << std::endl;
    assert(state.index_scan);

    ZipfDistribution zipf((state.scale_factor * 1000) - 1,
                          state.zipf_theta);

    PinToCore(0);
    stopwatch_t sw;
    sw.start();

    // Column ids to be added to logical tile.
    int end_column_idx = state.column_count * state.projectivity;
    std::cout << "YCSB column_count." << state.column_count << std::endl;
    std::cout << "YCSB projectivity." << state.projectivity << std::endl;
    std::vector<oid_t> column_ids;
    column_ids.clear();
    for (int j = 0; j < end_column_idx; ++j) {
      column_ids.push_back(j);
    }

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//    // read all the attributes in a tuple.
//    for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
//      column_ids.push_back(col_itr);
//    }

    for (int i = 0; i < 10*1000*1000; ++i) {
        concurrency::Transaction *read_txn =txn_manager.BeginTransaction(0);
        if (state.read_only) {
          read_txn->SetDeclaredReadOnly();
        }

        std::unique_ptr<executor::ExecutorContext> context( new executor::ExecutorContext(read_txn));

        std::vector<oid_t> key_column_ids;
        std::vector<ExpressionType> expr_types;
        key_column_ids.push_back(0);
        expr_types.push_back(ExpressionType::COMPARE_EQUAL);
        std::vector<expression::AbstractExpression *> runtime_keys;
        /////////////////////////////////////////////////////////
        // PERFORM READ
        /////////////////////////////////////////////////////////
        // set up parameter values
        std::vector<type::Value> values;
        auto lookup_key = 0;
        lookup_key = zipf.GetNextNumber();
        values.push_back(type::ValueFactory::GetIntegerValue(lookup_key).Copy());

        // index
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
        //if layout row or column
//        if (state.layout_mode != LAYOUT_TYPE_HYBRID){

          ExecuteRead(&index_scan_executor);

//        }else{
//          std::vector<std::unique_ptr<executor::LogicalTile>> result_tiles;
//          /////////////////////////////////////////////////////////
//          // MATERIALIZE
//          /////////////////////////////////////////////////////////
//          // Create and set up materialization executor
//          std::vector<catalog::Column> output_columns;
//          std::unordered_map<oid_t, oid_t> old_to_new_cols;
//          oid_t col_itr = 0;
//          for (auto column_id : column_ids) {
//            auto column = catalog::Column(type::Type::INTEGER,
//                                          type::Type::GetTypeSize(type::Type::INTEGER),
//                                          std::to_string(column_id), true);
//            output_columns.push_back(column);
//
//            old_to_new_cols[col_itr] = col_itr;
//
//            col_itr++;
//          }
//
//          std::shared_ptr<const catalog::Schema> output_schema(
//              new catalog::Schema(output_columns));
//          bool physify_flag = true;  // is going to create a physical tile
//          planner::MaterializationPlan mat_node(old_to_new_cols, output_schema,
//                                                physify_flag);
//
//          executor::MaterializationExecutor mat_executor(&mat_node, nullptr);
//
//          mat_executor.AddChild(&index_scan_executor);
//          /////////////////////////////////////////////////////////
//          // EXECUTE
//          /////////////////////////////////////////////////////////
//          bool status = false;
//          std::vector<executor::AbstractExecutor *> executors;
//          executors.push_back(&mat_executor);
//
//          // Run all the executors
//          for (auto executor : executors) {
//            status = executor->Init();
//            if (status == false) {
//              throw Exception("Init failed");
//            }
//
//            while (executor->Execute() == true) {
//              std::unique_ptr<executor::LogicalTile> result_tile(executor->GetOutput());
//              result_tiles.emplace_back(result_tile.release());
//            }
//
//            size_t sum = 0;
//            for (auto &result_tile : result_tiles) {
//              if (result_tile != nullptr) {
//                sum += result_tile->GetTupleCount();
//                auto column_count = result_tile->GetColumnCount();
//                for (oid_t tuple_id : *result_tile) {
//                  expression::ContainerTuple<executor::LogicalTile> cur_tuple(result_tile.get(),
//                                                                              tuple_id);
//                  std::vector<type::Value > tuple_values;
//                  for (oid_t column_itr = 0; column_itr < column_count; column_itr++){
//                    auto value = cur_tuple.GetValue(column_itr);
//                    tuple_values.push_back(value);
//                  }
//                }
//              }
//            }
//
//            //LOG_INFO("result tiles have %d tuples", (int)sum);
//
//            // Execute stuff
//            executor->Execute();
//          }
//        }

        txn_manager.CommitTransaction(read_txn);
      }

    auto elapsed = sw.elapsed<std::chrono::seconds>();
    std::cout << "Workload3 finished in " << elapsed << " seconds" << std::endl;

    int ycsb_tuple_count = user_table->GetTupleCount();
    std::cout << "YCSB table total tuple counts: " << ycsb_tuple_count << std::endl;
}
/**
 * paper tests
 * workload4: update
 * load 1 million
 * run 10*1000*1000 updates
 */
void RunWorkload4() {
  std::cout << "Workload4 started." << std::endl;
  assert(state.index_scan);

  ZipfDistribution zipf((state.scale_factor * 1000) - 1,state.zipf_theta);

  // Column ids to be added to logical tile.
  std::cout << "YCSB column_count." << state.column_count << std::endl;
  std::cout << "YCSB projectivity." << state.projectivity << std::endl;
  oid_t column_count = state.column_count + 1;
  int end_column_idx = state.column_count * state.projectivity;
  std::vector<oid_t> column_ids;
  // read all the attributes in a tuple.
  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(col_itr);
  }

  PinToCore(0);
  stopwatch_t sw;
  sw.start();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  for (int i = 0; i < 10*1000*1000; ++i) {
    concurrency::Transaction *read_txn =txn_manager.BeginTransaction(0);

    std::unique_ptr<executor::ExecutorContext> context( new executor::ExecutorContext(read_txn));

    std::vector<oid_t> key_column_ids;
    std::vector<ExpressionType> expr_types;
    key_column_ids.push_back(0);
    expr_types.push_back(ExpressionType::COMPARE_EQUAL);
    std::vector<expression::AbstractExpression *> runtime_keys;
    /////////////////////////////////////////////////////////
    // PERFORM UPDATE
    /////////////////////////////////////////////////////////
    // Create and set up index scan executor
    // set up parameter values
    std::vector<type::Value> values;
    auto lookup_key = 0;
    lookup_key = zipf.GetNextNumber();
    values.push_back(type::ValueFactory::GetIntegerValue(lookup_key).Copy());

    //index
    auto ycsb_pkey_index = user_table->GetIndexWithOid(user_table_pkey_index_oid);

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
      if (col_itr>0 && col_itr < end_column_idx) {
        int update_raw_value = 1;
        type::Value update_val =
            type::ValueFactory::GetIntegerValue(update_raw_value).Copy();
        target_list.emplace_back(
            col_itr,
            expression::ExpressionUtil::ConstantValueFactory(update_val));
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


    txn_manager.CommitTransaction(read_txn);
  }

  auto elapsed = sw.elapsed<std::chrono::seconds>();
  std::cout << "Workload4 finished in " << elapsed << " seconds" << std::endl;

  int ycsb_tuple_count = user_table->GetTupleCount();
  std::cout << "YCSB table total tuple counts: " << ycsb_tuple_count << std::endl;
}
/**
 * paper tests
 * workload5: 50% read & 50% update
 * load 1 million
 * run 10*1000*1000 reads/updates
 */
void RunWorkload5() {
  std::cout << "Workload5 started." << std::endl;
  assert(state.index_scan);
  PinToCore(0);

  ZipfDistribution zipf((state.scale_factor * 1000) - 1,state.zipf_theta);
  FastRandom rng(rand());

  // Column ids to be added to logical tile.
  std::cout << "YCSB column_count." << state.column_count << std::endl;
  std::cout << "YCSB projectivity." << state.projectivity << std::endl;
  oid_t column_count = state.column_count + 1;
  int end_column_idx = state.column_count * state.projectivity;
  std::vector<oid_t> column_ids;
  // read all the attributes in a tuple.
  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(col_itr);
  }

  stopwatch_t sw;
  sw.start();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  concurrency::Transaction *read_txn =txn_manager.BeginTransaction(0);
  std::unique_ptr<executor::ExecutorContext> read_context( new executor::ExecutorContext(read_txn));

  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  key_column_ids.push_back(0);
  expr_types.push_back(ExpressionType::COMPARE_EQUAL);
  std::vector<expression::AbstractExpression *> runtime_keys;

  for (int i = 0; i < 10*1000; ++i) {
    concurrency::Transaction *update_txn = txn_manager.BeginTransaction(0);
    std::unique_ptr<executor::ExecutorContext> update_context(
        new executor::ExecutorContext(update_txn));

    //index lookup
    // set up parameter values
    std::vector<type::Value> values;
    auto lookup_key = 0;
    lookup_key = zipf.GetNextNumber();
    values.push_back(type::ValueFactory::GetIntegerValue(lookup_key).Copy());
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
                                                    update_context.get());

    /////////////////////////////////////////////////////////
    // PERFORM UPDATE
    /////////////////////////////////////////////////////////
    // Create and set up index scan executor
    TargetList target_list;
    DirectMapList direct_map_list;
    // update multiple attributes
    for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
      if (col_itr > 0 && col_itr < end_column_idx) {
        int update_raw_value = 1;
        type::Value update_val =
            type::ValueFactory::GetIntegerValue(update_raw_value).Copy();
        target_list.emplace_back(
            col_itr,
            expression::ExpressionUtil::ConstantValueFactory(update_val));
      } else {
        direct_map_list.emplace_back(col_itr,
                                     std::pair<oid_t, oid_t>(0, col_itr));
      }
    }

    std::unique_ptr<const planner::ProjectInfo> project_info(
        new planner::ProjectInfo(std::move(target_list),
                                 std::move(direct_map_list)));
    planner::UpdatePlan update_node(user_table, std::move(project_info));

    executor::UpdateExecutor update_executor(&update_node,
                                             update_context.get());

    update_executor.AddChild(&index_scan_executor);

    ExecuteUpdate(&update_executor);

    txn_manager.CommitTransaction(update_txn);
  }

  for (int i = 0; i < 1000*10; ++i) {
      /////////////////////////////////////////////////////////
      // PERFORM READ
      /////////////////////////////////////////////////////////
      // set up parameter values
      std::vector<type::Value> values;
      auto lookup_key = 0;
      lookup_key = zipf.GetNextNumber();
      values.push_back(type::ValueFactory::GetIntegerValue(lookup_key).Copy());
      // index
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
                                                      read_context.get());
      //if layout row or column
      ExecuteRead(&index_scan_executor);

      txn_manager.CommitTransaction(read_txn);
  }


  auto elapsed = sw.elapsed<std::chrono::seconds>();
  std::cout << "Workload5 finished in " << elapsed << " seconds" << std::endl;

  int ycsb_tuple_count = user_table->GetTupleCount();
  std::cout << "YCSB table total tuple counts: " << ycsb_tuple_count << std::endl;
}


/////////////////////////////////////////////////////////
// HARNESS
/////////////////////////////////////////////////////////
std::vector<std::vector<type::Value >> ExecuteRead(executor::AbstractExecutor* executor) {
  executor->Init();

  std::vector<std::vector<type::Value >> logical_tile_values;

  // Execute stuff
  size_t sum = 0;
  while (executor->Execute() == true) {
    std::unique_ptr<executor::LogicalTile> result_tile(executor->GetOutput());

    if(result_tile == nullptr) {
      break;
    }

    auto column_count = result_tile->GetColumnCount();
//    LOG_DEBUG("result column count = %d\n", (int)column_count);

    for (oid_t tuple_id : *result_tile) {
      expression::ContainerTuple<executor::LogicalTile> cur_tuple(result_tile.get(),
                                                                  tuple_id);
      std::vector<type::Value > tuple_values;
      for (oid_t column_itr = 0; column_itr < column_count; column_itr++){
        auto value = cur_tuple.GetValue(column_itr);
        tuple_values.push_back(value);
//        LOG_DEBUG("column value string = %s", value.ToString().c_str());
      }

      // Move the tuple list
      logical_tile_values.push_back(std::move(tuple_values));
    }
    sum = sum + result_tile->GetTupleCount();

  }
//  LOG_DEBUG("result row count = %zu", sum);

  return std::move(logical_tile_values);
}

void ExecuteUpdate(executor::AbstractExecutor* executor) {
  executor->Init();
  // Execute stuff
  while (executor->Execute() == true);
}
std::vector<std::vector<type::Value >> ExecuteScan(executor::AbstractExecutor* executor) {
  executor->Init();

  std::vector<std::vector<type::Value >> logical_tile_values;

  // Execute stuff
  size_t sum = 0;
  while (executor->Execute() == true){
    std::unique_ptr<executor::LogicalTile> result_tile(executor->GetOutput());
//    LOG_DEBUG("result tuple count = %zu", result_tile->GetTupleCount());
    if(result_tile == nullptr) {
      break;
    }

    auto column_count = result_tile->GetColumnCount();
//    LOG_DEBUG("result column count = %d\n", (int)column_count);

    for (oid_t tuple_id : *result_tile) {
      expression::ContainerTuple<executor::LogicalTile> cur_tuple(result_tile.get(),
                                                                  tuple_id);
      std::vector<type::Value > tuple_values;
      for (oid_t column_itr = 0; column_itr < column_count; column_itr++){
        auto value = cur_tuple.GetValue(column_itr);
        tuple_values.push_back(value);
//        LOG_DEBUG("column value string = %s", value.ToString().c_str());
      }

      // Move the tuple list
      logical_tile_values.push_back(std::move(tuple_values));
    }
    sum = sum + result_tile->GetTupleCount();
  }

//  LOG_DEBUG("logical_tile_values size = %zu", logical_tile_values.size());

  return std::move(logical_tile_values);
}


}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
