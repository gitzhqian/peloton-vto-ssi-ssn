//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_workload.cpp
//
// Identification: src/main/tpcc/tpcc_workload.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "benchmark/tpcc/tpcc_workload.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <ctime>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <regex>
#include <string>
#include <thread>
#include <unordered_map>
#include <sys/utsname.h>
#include <vector>
#include <papi.h>

#include "benchmark/benchmark_common.h"
#include "benchmark/tpcc/tpcc_configuration.h"
#include "benchmark/tpcc/tpcc_loader.h"
#include "catalog/manager.h"
#include "catalog/schema.h"
#include "common/container_tuple.h"
#include "common/generator.h"
#include "common/logger.h"
#include "common/timer.h"
#include "concurrency/epoch_manager_factory.h"
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
#include "planner/aggregate_plan.h"
#include "planner/index_scan_plan.h"
#include "planner/insert_plan.h"
#include "planner/limit_plan.h"
#include "planner/materialization_plan.h"
#include "planner/nested_loop_join_plan.h"
#include "planner/order_by_plan.h"
#include "planner/update_plan.h"
#include "storage/data_table.h"
#include "storage/table_factory.h"
#include "type/types.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace peloton {
namespace benchmark {
namespace tpcc {

/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////

#define STOCK_LEVEL_RATIO     0.1
#define ORDER_STATUS_RATIO    0.0
#define PAYMENT_RATIO         0.0
#define NEW_ORDER_RATIO       0.1
#define QUERY2_RATIO          0.8

volatile bool is_running = true;

PadInt *abort_counts;
PadInt *commit_counts;
PadInt *q2_abort_counts;
PadInt *q2_commit_counts;
PadInt *new_abort_counts;
PadInt *new_commit_counts;
PadInt *stock_abort_counts;
PadInt *stock_commit_counts;

static constexpr int PAPI_EVENT_COUNT = 7;

size_t GenerateWarehouseId(const size_t &thread_id) {
  if (state.affinity) {
    if (state.warehouse_count <= state.backend_count) {
      return thread_id % state.warehouse_count;
    } else {
      int warehouse_per_partition = state.warehouse_count / state.backend_count;
      int start_warehouse = warehouse_per_partition * thread_id;
      int end_warehouse = ((int)thread_id != (state.backend_count - 1)) ?
                          start_warehouse + warehouse_per_partition - 1 : state.warehouse_count - 1;
      return GetRandomInteger(start_warehouse, end_warehouse);
    }
  } else {
    return GetRandomInteger(0, state.warehouse_count - 1);
  }
}


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

void RunScanBackend(const size_t thread_id,
                    const std::vector<std::vector<std::pair<int32_t, int32_t>>> &supp_stock_map) {
  PinToCore(thread_id);

  PadInt &execution_count_ref = abort_counts[thread_id];
  PadInt &transaction_count_ref = commit_counts[thread_id];
//  PadInt &q2_execution_count_ref = q2_abort_counts[thread_id];
//  PadInt &q2_transaction_count_ref = q2_commit_counts[thread_id];
//  PadInt &new_execution_count_ref = new_abort_counts[thread_id];
//  PadInt &new_transaction_count_ref = new_commit_counts[thread_id];
//  PadInt &stock_execution_count_ref = stock_abort_counts[thread_id];
//  PadInt &stock_transaction_count_ref = stock_commit_counts[thread_id];

  double STOCK_LEVEL_RATIO_ = state.stock_level_rate;
  double NEW_ORDER_RATIO_= state.new_order_rate;
  double QUERY2_RATIO_ = state.scan_rate;
  LOG_DEBUG("%f, %f, %f",STOCK_LEVEL_RATIO_, NEW_ORDER_RATIO_, QUERY2_RATIO_);

  // backoff
  uint32_t backoff_shifts = 0;
  FastRandom rng(rand());

  while (true) {
    if (is_running == false) {
      break;
    }

    while (RunQuery2(thread_id, supp_stock_map) == false) {
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

//    auto rng_val = rng.NextUniform();
//    if (rng_val <= STOCK_LEVEL_RATIO_) {
//      while (RunStockLevel(thread_id) == false) {
//        if (is_running == false) {
//          break;
//        }
//        execution_count_ref.data++;
//        stock_execution_count_ref.data++;
//        // backoff
//        if (state.exp_backoff) {
//          if (backoff_shifts < 13) {
//            ++backoff_shifts;
//          }
//          uint64_t sleep_duration = 1UL << backoff_shifts;
//          sleep_duration *= 100;
//          std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
//        }
//      }
//      stock_transaction_count_ref.data++;
//  //    LOG_DEBUG("stocklevel");
//  //    LOG_DEBUG("%f, %f, %f",STOCK_LEVEL_RATIO_, NEW_ORDER_RATIO_, QUERY2_RATIO_);
//    }else if (rng_val <= STOCK_LEVEL_RATIO_ + NEW_ORDER_RATIO_) {
//      while (RunNewOrder(thread_id) == false) {
//        if (is_running == false) {
//          break;
//        }
//        execution_count_ref.data++;
//        new_execution_count_ref.data++;
//        // backoff
//        if (state.exp_backoff) {
//          if (backoff_shifts < 13) {
//            ++backoff_shifts;
//          }
//          uint64_t sleep_duration = 1UL << backoff_shifts;
//          sleep_duration *= 100;
//          std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
//        }
//      }
//      new_transaction_count_ref.data ++;
//
//    }else {
//
//       while (RunQuery2(thread_id, supp_stock_map) == false) {
//        if (is_running == false) {
//          break;
//        }
//        execution_count_ref.data++;
//        q2_execution_count_ref.data++;
//        // backoff
//        if (state.exp_backoff) {
//          if (backoff_shifts < 13) {
//            ++backoff_shifts;
//          }
//          uint64_t sleep_duration = 1UL << backoff_shifts;
//          sleep_duration *= 100;
//          std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
//        }
//      }
//      q2_transaction_count_ref.data ++;
//  //    LOG_DEBUG("neworder");
//    }

    backoff_shifts >>= 1;
    transaction_count_ref.data ++;
  }
}

void RunScanBackendWarm(const size_t thread_id,
                        const std::vector<std::vector<std::pair<int32_t, int32_t>>> &supp_stock_map) {
  PinToCore(thread_id);

//  PadInt &execution_count_ref = abort_counts[thread_id];
//  PadInt &transaction_count_ref = commit_counts[thread_id];
//  PadInt &q2_execution_count_ref = q2_abort_counts[thread_id];
//  PadInt &q2_transaction_count_ref = q2_commit_counts[thread_id];
//  PadInt &new_execution_count_ref = new_abort_counts[thread_id];
//  PadInt &new_transaction_count_ref = new_commit_counts[thread_id];
//  PadInt &stock_execution_count_ref = stock_abort_counts[thread_id];
//  PadInt &stock_transaction_count_ref = stock_commit_counts[thread_id];

  double STOCK_LEVEL_RATIO_ = state.stock_level_rate;
  double NEW_ORDER_RATIO_= state.new_order_rate;
  double QUERY2_RATIO_ = state.scan_rate;
  LOG_DEBUG("%f, %f, %f",STOCK_LEVEL_RATIO_, NEW_ORDER_RATIO_, QUERY2_RATIO_);

  // backoff
  uint32_t backoff_shifts = 0;
  FastRandom rng(rand());

  while (true) {
    if (is_running == false) {
      break;
    }

    auto rng_val = rng.NextUniform();

    if (rng_val <= STOCK_LEVEL_RATIO_) {
      while (RunStockLevel(thread_id) == false) {
        if (is_running == false) {
          break;
        }
//        execution_count_ref.data++;
//        stock_execution_count_ref.data++;
        // backoff
//        if (state.exp_backoff) {
//          if (backoff_shifts < 13) {
//            ++backoff_shifts;
//          }
//          uint64_t sleep_duration = 1UL << backoff_shifts;
//          sleep_duration *= 100;
//          std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
//        }
      }
//      stock_transaction_count_ref.data++;
//      LOG_DEBUG("stocklevel");
//      LOG_DEBUG("%f, %f, %f",STOCK_LEVEL_RATIO_, NEW_ORDER_RATIO_, QUERY2_RATIO_);
    }else if (rng_val <= STOCK_LEVEL_RATIO_ + QUERY2_RATIO_) {
      while (RunQuery2(thread_id, supp_stock_map) == false) {
        if (is_running == false) {
          break;
        }
//        execution_count_ref.data++;
//        q2_execution_count_ref.data++;
        // backoff
//        if (state.exp_backoff) {
//          if (backoff_shifts < 13) {
//            ++backoff_shifts;
//          }
//          uint64_t sleep_duration = 1UL << backoff_shifts;
//          sleep_duration *= 100;
//          std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
//        }
      }
//      q2_transaction_count_ref.data ++;

    }else {

      while (RunNewOrder(thread_id) == false) {
        if (is_running == false) {
          break;
        }
//        execution_count_ref.data++;
//        new_execution_count_ref.data++;
        // backoff
//        if (state.exp_backoff) {
//          if (backoff_shifts < 13) {
//            ++backoff_shifts;
//          }
//          uint64_t sleep_duration = 1UL << backoff_shifts;
//          sleep_duration *= 100;
//          std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
//        }
      }
//      new_transaction_count_ref.data ++;
//      LOG_DEBUG("neworder");
    }

//    backoff_shifts >>= 1;
//    transaction_count_ref.data ++;
  }
}
void RunBackend(const size_t thread_id) {

  PinToCore(thread_id);

  if (concurrency::EpochManagerFactory::GetEpochType() == EpochType::DECENTRALIZED_EPOCH) {
    // register thread to epoch manager
    auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
    epoch_manager.RegisterThread(thread_id);
  }

  PadInt &execution_count_ref = abort_counts[thread_id];
  PadInt &transaction_count_ref = commit_counts[thread_id];
//  PadInt &new_execution_count_ref = new_abort_counts[thread_id];
//  PadInt &new_transaction_count_ref = new_commit_counts[thread_id];

  // backoff
  uint32_t backoff_shifts = 0;
  FastRandom rng(rand());

  while (true) {

    if (is_running == false) {
      break;
    }

    //auto rng_val = rng.NextUniform();

    while (RunNewOrder(thread_id) == false) {
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
    //if (rng_val <= STOCK_LEVEL_RATIO) {
    //  while (RunStockLevel(thread_id) == false) {
    //    if (is_running == false) {
    //      break;
    //    }
    //    execution_count_ref.data++;
    //    // backoff
    //    if (state.exp_backoff) {
    //      if (backoff_shifts < 13) {
    //        ++backoff_shifts;
    //      }
    //      uint64_t sleep_duration = 1UL << backoff_shifts;
    //      sleep_duration *= 100;
    //      std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
    //    }
    //  }
    //}else if (rng_val <= STOCK_LEVEL_RATIO + NEW_ORDER_RATIO) {
    //  while (RunNewOrder(thread_id) == false) {
    //    if (is_running == false) {
    //      break;
    //    }
    //    execution_count_ref.data++;
    //    // backoff
    //    if (state.exp_backoff) {
    //      if (backoff_shifts < 13) {
    //        ++backoff_shifts;
    //      }
    //      uint64_t sleep_duration = 1UL << backoff_shifts;
    //      sleep_duration *= 100;
    //      std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
    //    }
    //  }
    //}
//    else if (rng_val <= ORDER_STATUS_RATIO + STOCK_LEVEL_RATIO) {
//      while (RunOrderStatus(thread_id) == false) {
//        if (is_running == false) {
//          break;
//        }
//        execution_count_ref.data++;
//        // backoff
//        if (state.exp_backoff) {
//          if (backoff_shifts < 13) {
//            ++backoff_shifts;
//          }
//          uint64_t sleep_duration = 1UL << backoff_shifts;
//          sleep_duration *= 100;
//          std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
//        }
//      }
//    } else if (rng_val <= PAYMENT_RATIO + ORDER_STATUS_RATIO + STOCK_LEVEL_RATIO) {
//      while (RunPayment(thread_id) == false) {
//        if (is_running == false) {
//          break;
//        }
//        execution_count_ref.data++;
//        // backoff
//        if (state.exp_backoff) {
//          if (backoff_shifts < 13) {
//            ++backoff_shifts;
//          }
//          uint64_t sleep_duration = 1UL << backoff_shifts;
//          sleep_duration *= 100;
//          std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
//        }
//      }
//    } else if (rng_val <= PAYMENT_RATIO + ORDER_STATUS_RATIO + STOCK_LEVEL_RATIO + NEW_ORDER_RATIO) {
//      while (RunNewOrder(thread_id) == false) {
//        if (is_running == false) {
//          break;
//        }
//        execution_count_ref.data++;
//        // backoff
//        if (state.exp_backoff) {
//          if (backoff_shifts < 13) {
//            ++backoff_shifts;
//          }
//          uint64_t sleep_duration = 1UL << backoff_shifts;
//          sleep_duration *= 100;
//          std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
//        }
//      }
//    } else {
//      while (RunDelivery(thread_id) == false) {
//        if (is_running == false) {
//          break;
//        }
//        execution_count_ref.data++;
//        // backoff
//        if (state.exp_backoff) {
//          if (backoff_shifts < 13) {
//            ++backoff_shifts;
//          }
//          uint64_t sleep_duration = 1UL << backoff_shifts;
//          sleep_duration *= 100;
//          std::this_thread::sleep_for(std::chrono::microseconds(sleep_duration));
//        }
//      }
//    }

    backoff_shifts >>= 1;
    transaction_count_ref.data++;

  }
}

void RunWarmupWorkload() {
  // value ranges 0 ~ 9999 ( modulo by 10k )
  std::vector<std::vector<std::pair<int32_t, int32_t>>> supp_stock_map(10000);
  // pre-build supp-stock mapping table to boost tpc-ch queries
  for (int w = 1; w <= state.warehouse_count; w++)
    for (int i = 1; i <= state.item_count; i++)
      supp_stock_map[w * i % 10000].push_back(std::make_pair((w-1), (i-1)));

  std::vector<std::thread> thread_group;
  // Execute the workload to build the log
  size_t num_threads = state.backend_count;;

  abort_counts = new PadInt[num_threads];
  memset(abort_counts, 0, sizeof(PadInt) * num_threads);

  commit_counts = new PadInt[num_threads];
  memset(commit_counts, 0, sizeof(PadInt) * num_threads);

  size_t profile_round = (size_t) state.warmup_duration;

  for (size_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::move(std::thread(RunScanBackendWarm, thread_itr, supp_stock_map)));
  }

  //int Events[PAPI_EVENT_COUNT] = {PAPI_L1_DCM, PAPI_L2_TCM, PAPI_L3_TCM, PAPI_BR_INS};
  int Events[PAPI_EVENT_COUNT] = {PAPI_TOT_CYC, PAPI_TOT_INS, PAPI_BR_MSP};
  int EventSet = PAPI_NULL;
  long long values[PAPI_EVENT_COUNT];
  int retval = PAPI_OK;
  if(enable_papi){
    InitPAPI();
/* Allocate space for the new eventset and do setup */
    retval = PAPI_create_eventset(&EventSet);
/* Add Flops and total cycles to the eventset */
    retval = PAPI_add_events(EventSet,Events,PAPI_EVENT_COUNT);
/* Start the counters */
    retval = PAPI_start(EventSet);
    assert(retval == PAPI_OK);
  }
  //////////////////////////////start ///////////////////////////////////////
  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(int(1000)));
  }
  //////////////////////////////end ///////////////////////////////////////
  if(enable_papi){
    print_environment();
    /*Stop counters and store results in values */
    retval = PAPI_stop(EventSet,values);
    assert(retval == PAPI_OK);
    //std::cout << "L1 miss = " << values[0] << "\n";
    //std::cout << "L2 miss = " << values[1] << "\n";
    //std::cout << "L3 miss = " << values[2] << "\n";
    //std::cout << "Total branch = " << values[3] << "\n";
    std::cout << "Total cycle = " << values[0] << "\n";
    std::cout << "Total instruction = " << values[1] << "\n";
    std::cout << "Total branch misprediction = " << values[2] << "\n";
    PAPI_shutdown();
  }

  is_running = false;

  // Join the threads with the main thread
  for (size_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }

  LOG_INFO("Warmed up\n");
  is_running = true;


  delete[] abort_counts;
  abort_counts = nullptr;
  delete[] commit_counts;
  commit_counts = nullptr;
}


void RunWorkload() {
  // value ranges 0 ~ 9999 ( modulo by 10k )
  std::vector<std::vector<std::pair<int32_t, int32_t>>> supp_stock_map(10000);
  // pre-build supp-stock mapping table to boost tpc-ch queries
  for (int w = 1; w <= state.warehouse_count; w++)
    for (int i = 1; i <= state.item_count; i++)
      supp_stock_map[w * i % 10000].push_back(std::make_pair((w-1), (i-1)));

  // Execute the workload to build the log
  std::vector<std::thread> thread_group;
  size_t num_threads = state.backend_count;
  size_t scan_thread = num_threads*state.scan_rate;

  abort_counts = new PadInt[num_threads];
  PL_MEMSET(abort_counts, 0, sizeof(PadInt) * num_threads);

  commit_counts = new PadInt[num_threads];
  PL_MEMSET(commit_counts, 0, sizeof(PadInt) * num_threads);

//  q2_abort_counts = new PadInt[num_threads];
//  PL_MEMSET(q2_abort_counts, 0, sizeof(PadInt) * num_threads);
//
//  q2_commit_counts = new PadInt[num_threads];
//  PL_MEMSET(q2_commit_counts, 0, sizeof(PadInt) * num_threads);
//
//  stock_abort_counts = new PadInt[num_threads];
//  PL_MEMSET(stock_abort_counts, 0, sizeof(PadInt) * num_threads);
//
//  stock_commit_counts = new PadInt[num_threads];
//  PL_MEMSET(stock_commit_counts, 0, sizeof(PadInt) * num_threads);
//
//  new_abort_counts = new PadInt[num_threads];
//  PL_MEMSET(new_abort_counts, 0, sizeof(PadInt) * num_threads);
//
//  new_commit_counts = new PadInt[num_threads];
//  PL_MEMSET(new_commit_counts, 0, sizeof(PadInt) * num_threads);

  size_t profile_round = (size_t)(state.duration / state.profile_duration);

  PadInt **abort_counts_profiles = new PadInt *[profile_round];
  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
    abort_counts_profiles[round_id] = new PadInt[num_threads];
  }

  PadInt **commit_counts_profiles = new PadInt *[profile_round];
  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
    commit_counts_profiles[round_id] = new PadInt[num_threads];
  }

//  PadInt **q2_abort_counts_profiles = new PadInt *[profile_round];
//  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
//    q2_abort_counts_profiles[round_id] = new PadInt[num_threads];
//  }
//
//  PadInt **q2_commit_counts_profiles = new PadInt *[profile_round];
//  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
//    q2_commit_counts_profiles[round_id] = new PadInt[num_threads];
//  }
//
//  PadInt **new_abort_counts_profiles = new PadInt *[profile_round];
//  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
//    new_abort_counts_profiles[round_id] = new PadInt[(num_threads)];
//  }
//
//  PadInt **new_commit_counts_profiles = new PadInt *[profile_round];
//  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
//    new_commit_counts_profiles[round_id] = new PadInt[(num_threads)];
//  }
//
//  PadInt **stock_abort_counts_profiles = new PadInt *[profile_round];
//  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
//    stock_abort_counts_profiles[round_id] = new PadInt[num_threads];
//  }
//
//  PadInt **stock_commit_counts_profiles = new PadInt *[profile_round];
//  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
//    stock_commit_counts_profiles[round_id] = new PadInt[num_threads];
//  }


  for (size_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    if (thread_itr < scan_thread){
      thread_group.push_back(std::move(std::thread(RunScanBackend, thread_itr, supp_stock_map)));
    }else{
      thread_group.push_back(std::move(std::thread(RunBackend, thread_itr)));
    }
  }
//  for (size_t thread_itr = start_thread; thread_itr < num_threads; ++thread_itr) {
//    thread_group.push_back(std::move(std::thread(RunBackend, thread_itr)));
//  }

  //////////////////////////////////////
  oid_t last_tile_group_id = 0;
  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(int(state.profile_duration * 1000)));
    PL_MEMCPY(abort_counts_profiles[round_id], abort_counts,
              sizeof(PadInt) * num_threads);
    PL_MEMCPY(commit_counts_profiles[round_id], commit_counts,
              sizeof(PadInt) * num_threads);
//    PL_MEMCPY(q2_abort_counts_profiles[round_id], q2_abort_counts,
//              sizeof(PadInt) * num_threads);
//    PL_MEMCPY(q2_commit_counts_profiles[round_id], q2_commit_counts,
//              sizeof(PadInt) * num_threads);
//    PL_MEMCPY(new_abort_counts_profiles[round_id], new_abort_counts,
//              sizeof(PadInt) * num_threads);
//    PL_MEMCPY(new_commit_counts_profiles[round_id], new_commit_counts,
//              sizeof(PadInt) * num_threads);
//    PL_MEMCPY(stock_abort_counts_profiles[round_id], stock_abort_counts,
//              sizeof(PadInt) * num_threads);
//    PL_MEMCPY(stock_commit_counts_profiles[round_id], stock_commit_counts,
//              sizeof(PadInt) * num_threads);

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

  // calculate the throughput and abort rate for the first round.
  uint64_t total_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_commit_count += commit_counts_profiles[0][i].data;
  }

  uint64_t total_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_abort_count += abort_counts_profiles[0][i].data;
  }

//  uint64_t q2_total_commit_count = 0;
//  for (size_t i = 0; i < num_threads; ++i) {
//    q2_total_commit_count += q2_commit_counts_profiles[0][i].data;
//  }
//  uint64_t q2_total_abort_count = 0;
//  for (size_t i = 0; i < num_threads; ++i) {
//    q2_total_abort_count += q2_abort_counts_profiles[0][i].data;
//  }
//  uint64_t new_total_commit_count = 0;
//  for (size_t i = 0; i < num_threads; ++i) {
//    new_total_commit_count += new_commit_counts_profiles[0][i].data;
//  }
//  uint64_t new_total_abort_count = 0;
//  for (size_t i = 0; i < num_threads; ++i) {
//    new_total_abort_count += new_abort_counts_profiles[0][i].data;
//  }

  uint64_t q2_total_commit_count = 0;
  for (size_t i = 0; i < scan_thread; ++i) {
    q2_total_commit_count += commit_counts_profiles[0][i].data;
  }
  uint64_t q2_total_abort_count = 0;
  for (size_t i = 0; i < scan_thread; ++i) {
    q2_total_abort_count += abort_counts_profiles[0][i].data;
  }
  uint64_t new_total_commit_count = 0;
  for (size_t i = scan_thread; i < num_threads; ++i) {
    new_total_commit_count += commit_counts_profiles[0][i].data;
  }
  uint64_t new_total_abort_count = 0;
  for (size_t i = scan_thread; i < num_threads; ++i) {
    new_total_abort_count += abort_counts_profiles[0][i].data;
  }

  uint64_t stock_total_commit_count = 0;
//  for (size_t i = 0; i < num_threads; ++i) {
//    stock_total_commit_count += stock_commit_counts_profiles[0][i].data;
//  }
//
  uint64_t stock_total_abort_count = 0;
//  for (size_t i = 0; i < num_threads; ++i) {
//    stock_total_abort_count += stock_abort_counts_profiles[0][i].data;
//  }

  state.profile_throughput.push_back(total_commit_count * 1.0 / state.profile_duration);
  state.profile_abort_rate.push_back(total_abort_count * 1.0 / (total_commit_count + total_abort_count));
//  state.profile_q2_throughput.push_back(q2_total_commit_count * 1.0 / state.profile_duration);
//  state.profile_q2_abort_rate.push_back(q2_total_abort_count * 1.0 / q2_total_commit_count);

  // calculate the throughput and abort rate for the remaining rounds.
  for (size_t round_id = 0; round_id < profile_round - 1; ++round_id) {
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
    q2_total_commit_count = 0;
    for (size_t i = 0; i < scan_thread; ++i) {
      q2_total_commit_count += commit_counts_profiles[round_id + 1][i].data -
                               commit_counts_profiles[round_id][i].data;
    }
    q2_total_abort_count = 0;
    for (size_t i = 0; i < scan_thread; ++i) {
      q2_total_abort_count += abort_counts_profiles[round_id + 1][i].data -
                              abort_counts_profiles[round_id][i].data;
    }

    new_total_commit_count = 0;
    for (size_t i = scan_thread; i < num_threads; ++i) {
      new_total_commit_count += commit_counts_profiles[round_id + 1][i].data -
                                commit_counts_profiles[round_id][i].data;
    }
    new_total_abort_count = 0;
    for (size_t i = scan_thread; i < num_threads; ++i) {
      new_total_abort_count += abort_counts_profiles[round_id + 1][i].data -
                                abort_counts_profiles[round_id][i].data;
    }


//    q2_total_commit_count = 0;
//    for (size_t i = 0; i < num_threads; ++i) {
//      q2_total_commit_count += q2_commit_counts_profiles[round_id + 1][i].data -
//                               q2_commit_counts_profiles[round_id][i].data;
//    }
//    q2_total_abort_count = 0;
//    for (size_t i = 0; i < num_threads; ++i) {
//      q2_total_abort_count += q2_abort_counts_profiles[round_id + 1][i].data -
//                              q2_abort_counts_profiles[round_id][i].data;
//    }
//
//    new_total_commit_count = 0;
//    for (size_t i = 0; i < num_threads; ++i) {
//      new_total_commit_count += new_commit_counts_profiles[round_id + 1][i].data -
//                                new_commit_counts_profiles[round_id][i].data;
//    }
//    new_total_abort_count = 0;
//    for (size_t i = 0; i < num_threads; ++i) {
//      new_total_abort_count += new_abort_counts_profiles[round_id + 1][i].data -
//                               new_abort_counts_profiles[round_id][i].data;
//    }
//
//    stock_total_commit_count = 0;
//    for (size_t i = 0; i < num_threads; ++i) {
//      stock_total_commit_count += stock_commit_counts_profiles[round_id + 1][i].data -
//                                  stock_commit_counts_profiles[round_id][i].data;
//    }
//    stock_total_abort_count = 0;
//    for (size_t i = 0; i < num_threads; ++i) {
//      stock_total_abort_count += stock_abort_counts_profiles[round_id + 1][i].data -
//                                 stock_abort_counts_profiles[round_id][i].data;
//    }
    state.profile_throughput.push_back(total_commit_count * 1.0 / state.profile_duration);
    state.profile_abort_rate.push_back(total_abort_count * 1.0 / (total_commit_count + total_abort_count));
//    state.profile_q2_throughput.push_back(q2_total_commit_count * 1.0 / state.profile_duration);
//    state.profile_q2_abort_rate.push_back(q2_total_abort_count * 1.0 / q2_total_commit_count);
  }

  // calculate the aggregated throughput and abort rate.
  total_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_commit_count += commit_counts_profiles[profile_round - 1][i].data;
  }

  total_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_abort_count += abort_counts_profiles[profile_round - 1][i].data;
  }

  q2_total_commit_count = 0;
  for (size_t i = 0; i < scan_thread; ++i) {
    q2_total_commit_count += commit_counts_profiles[profile_round - 1][i].data;
  }
  q2_total_abort_count = 0;
  for (size_t i = 0; i < scan_thread; ++i) {
    q2_total_abort_count += abort_counts_profiles[profile_round - 1][i].data;
  }

  new_total_commit_count = 0;
  for (size_t i = scan_thread; i < num_threads; ++i) {
    new_total_commit_count += commit_counts_profiles[profile_round - 1][i].data;
  }
  new_total_abort_count = 0;
  for (size_t i = scan_thread; i < num_threads; ++i) {
    new_total_abort_count += abort_counts_profiles[profile_round - 1][i].data;
  }

//  q2_total_commit_count = 0;
//  for (size_t i = 0; i < num_threads; ++i) {
//    q2_total_commit_count += q2_commit_counts_profiles[profile_round - 1][i].data;
//  }
//  q2_total_abort_count = 0;
//  for (size_t i = 0; i < num_threads; ++i) {
//    q2_total_abort_count += q2_abort_counts_profiles[profile_round - 1][i].data;
//  }
//
//  new_total_commit_count = 0;
//  for (size_t i = 0; i < num_threads; ++i) {
//    new_total_commit_count += new_commit_counts_profiles[profile_round - 1][i].data;
//  }
//  new_total_abort_count = 0;
//  for (size_t i = 0; i < num_threads; ++i) {
//    new_total_abort_count += new_abort_counts_profiles[profile_round - 1][i].data;
//  }
//
//  stock_total_commit_count = 0;
//  for (size_t i = 0; i < num_threads; ++i) {
//    stock_total_commit_count += stock_commit_counts_profiles[profile_round - 1][i].data;
//  }
//  stock_total_abort_count = 0;
//  for (size_t i = 0; i < num_threads; ++i) {
//    stock_total_abort_count += stock_abort_counts_profiles[profile_round - 1][i].data;
//  }

//  LOG_INFO("scan commit count %d", (int)scan_commit_count);
  LOG_INFO("total, total commit , total abort, "
           "q2 total commit , q2 total abort,"
           "new total commit , new total abort,"
           "stock total commit , stock total abort : "
           "%d %d %d %d %d %d %d %d %d",
           (int)(total_commit_count + total_abort_count),
           (int)total_commit_count, (int)total_abort_count,
           (int)q2_total_commit_count, (int)q2_total_abort_count,
           (int)new_total_commit_count, (int)new_total_abort_count,
           (int)stock_total_commit_count, (int)stock_total_abort_count);

  state.throughput = total_commit_count * 1.0 / state.duration;
  state.abort_rate = total_abort_count * 1.0 / (total_commit_count+total_abort_count);
//  state.q2_throughput = q2_total_commit_count * 1.0 / state.duration;
//  state.q2_abort_rate = q2_total_abort_count * 1.0 / q2_total_commit_count;
  //when q2 is 100%
  state.scan_latency = state.duration / ((q2_total_commit_count + q2_total_abort_count) * 1.0);

  // cleanup everything.
  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
    delete[] abort_counts_profiles[round_id];
    abort_counts_profiles[round_id] = nullptr;
  }
  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
    delete[] commit_counts_profiles[round_id];
    commit_counts_profiles[round_id] = nullptr;
  }

//  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
//    delete[] q2_abort_counts_profiles[round_id];
//    q2_abort_counts_profiles[round_id] = nullptr;
//  }
//  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
//    delete[] q2_commit_counts_profiles[round_id];
//    q2_commit_counts_profiles[round_id] = nullptr;
//  }
//
//  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
//    delete[] new_abort_counts_profiles[round_id];
//    new_abort_counts_profiles[round_id] = nullptr;
//  }
//  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
//    delete[] new_commit_counts_profiles[round_id];
//    new_commit_counts_profiles[round_id] = nullptr;
//  }
//
//  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
//    delete[] stock_abort_counts_profiles[round_id];
//    stock_abort_counts_profiles[round_id] = nullptr;
//  }
//  for (size_t round_id = 0; round_id < profile_round; ++round_id) {
//    delete[] stock_commit_counts_profiles[round_id];
//    stock_commit_counts_profiles[round_id] = nullptr;
//  }

  delete[] abort_counts_profiles;
  abort_counts_profiles = nullptr;
  delete[] commit_counts_profiles;
  commit_counts_profiles = nullptr;
//  delete[] q2_abort_counts_profiles;
//  q2_abort_counts_profiles = nullptr;
//  delete[] q2_commit_counts_profiles;
//  q2_commit_counts_profiles = nullptr;
//  delete[] new_abort_counts_profiles;
//  new_abort_counts_profiles = nullptr;
//  delete[] new_commit_counts_profiles;
//  new_commit_counts_profiles = nullptr;
//  delete[] stock_abort_counts_profiles;
//  stock_abort_counts_profiles = nullptr;
//  delete[] stock_commit_counts_profiles;
//  stock_commit_counts_profiles = nullptr;

  delete[] abort_counts;
  abort_counts = nullptr;
  delete[] commit_counts;
  commit_counts = nullptr;
  delete[] q2_abort_counts;
  q2_abort_counts = nullptr;
  delete[] q2_commit_counts;
  q2_commit_counts = nullptr;
  delete[] new_abort_counts;
  new_abort_counts = nullptr;
  delete[] new_commit_counts;
  new_commit_counts = nullptr;
  delete[] stock_abort_counts;
  stock_abort_counts = nullptr;
  delete[] stock_commit_counts;
  stock_commit_counts = nullptr;
}

/////////////////////////////////////////////////////////
// HARNESS
/////////////////////////////////////////////////////////

std::vector<std::vector<type::Value >> ExecuteRead(executor::AbstractExecutor* executor) {
  executor->Init();

  std::vector<std::vector<type::Value >> logical_tile_values;

  // Execute stuff
  while (executor->Execute() == true) {
    std::unique_ptr<executor::LogicalTile> result_tile(executor->GetOutput());

    if(result_tile == nullptr) {
      break;
    }

    auto column_count = result_tile->GetColumnCount();
    LOG_TRACE("result column count = %d\n", (int)column_count);

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
  }

  return std::move(logical_tile_values);
}

void ExecuteUpdate(executor::AbstractExecutor* executor) {
  executor->Init();
  // Execute stuff
  while (executor->Execute() == true);
}


void ExecuteDelete(executor::AbstractExecutor* executor) {
  executor->Init();
  // Execute stuff
  while (executor->Execute() == true);
}


}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
