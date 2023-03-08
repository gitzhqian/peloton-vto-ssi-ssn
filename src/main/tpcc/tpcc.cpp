//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc.cpp
//
// Identification: src/main/tpcc/tpcc.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <include/concurrency/transaction_manager_factory.h>
#include <fstream>
#include <iomanip>
#include <iostream>

#include "benchmark/tpcc/tpcc_configuration.h"
#include "benchmark/tpcc/tpcc_loader.h"
#include "benchmark/tpcc/tpcc_workload.h"
#include "common/logger.h"
#include "concurrency/epoch_manager_factory.h"
#include "gc/gc_manager_factory.h"

namespace peloton {
namespace benchmark {
namespace tpcc {

configuration state;

// Main Entry Point
void RunBenchmark() {

  if (state.gc_mode == false) {
    gc::GCManagerFactory::Configure(0);
  } else {
    gc::GCManagerFactory::Configure(state.gc_backend_count);
  }
  
  concurrency::EpochManagerFactory::Configure(state.epoch);

  std::unique_ptr<std::thread> epoch_thread;
  std::vector<std::unique_ptr<std::thread>> gc_threads;

  concurrency::EpochManager &epoch_manager = concurrency::EpochManagerFactory::GetInstance();

  if (concurrency::EpochManagerFactory::GetEpochType() == EpochType::DECENTRALIZED_EPOCH) {
    for (size_t i = 0; i < (size_t) state.backend_count; ++i) {
      // register thread to epoch manager
      epoch_manager.RegisterThread(i);
    }
  }

  if (state.concurrency_type == 0){
    concurrency::TransactionManagerFactory::Configure(ConcurrencyType::TIMESTAMP_ORDERING);
  }else if(state.concurrency_type == 1){
    concurrency::TransactionManagerFactory::Configure(ConcurrencyType::CONCURRENCY_TYPE_SSI);
  }else if(state.concurrency_type == 2){
    concurrency::TransactionManagerFactory::Configure(ConcurrencyType::CONCURRENCY_TYPE_SI_SSN);
  }


  // start epoch.
  epoch_manager.StartEpoch(epoch_thread);

  gc::GCManager &gc_manager = gc::GCManagerFactory::GetInstance();
 
  // start GC.
  gc_manager.StartGC(gc_threads);

  // Create the database
  CreateTPCCDatabase();

  // Load the database
  LoadTPCCDatabase();


  if (state.warmup_duration != 0) {
    RunWarmupWorkload();
  }

  // Run the workload
  RunWorkload();

  // stop GC.
  gc_manager.StopGC();

  // stop epoch.
  epoch_manager.StopEpoch();

  // join all gc threads
  for (auto &gc_thread : gc_threads) {
    PL_ASSERT(gc_thread != nullptr);
    gc_thread->join();
  }

  // join epoch thread
  PL_ASSERT(epoch_thread != nullptr);
  epoch_thread->join();


  // Emit throughput
  WriteOutput();
}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton

int main(int argc, char **argv) {
  peloton::benchmark::tpcc::ParseArguments(argc, argv,
                                           peloton::benchmark::tpcc::state);

  peloton::benchmark::tpcc::RunBenchmark();

  return 0;
}
