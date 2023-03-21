//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_workload.h
//
// Identification: src/include/benchmark/ycsb/ycsb_workload.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "benchmark/benchmark_common.h"
#include "benchmark/ycsb/ycsb_configuration.h"
#include "storage/data_table.h"
#include "executor/abstract_executor.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace benchmark {
namespace ycsb {

extern configuration state;

extern storage::DataTable* user_table;
extern thread_local size_t num_rw_ops;

void RunWorkload();
void RunWarmupWorkload();

void RunWorkload1();
void RunWorkload2();
void RunWorkload3();
void RunWorkload4();
void RunWorkload5();

bool RunMixed(const size_t thread_id, ZipfDistribution &zipf, FastRandom &rng);
bool  RunScanMixed(const size_t thread_id);
bool  RunScanSimpleMixed(const size_t thread_id, ZipfDistribution &zipf);
/////////////////////////////////////////////////////////

std::vector<std::vector<type::Value>> ExecuteRead(executor::AbstractExecutor* executor);

void ExecuteUpdate(executor::AbstractExecutor* executor);
std::vector<std::vector<type::Value >> ExecuteScan(executor::AbstractExecutor* executor);

void PinToCore(size_t core);

template <typename T>
struct is_duration : std::false_type
{
};

template <class Rep, std::intmax_t Num, std::intmax_t Denom>
struct is_duration<std::chrono::duration<Rep, std::ratio<Num, Denom>>> : std::true_type
{
};

/**
 * @brief Wrapper that implements stopwatch functionality.
 *
 */
class stopwatch_t
{
 public:
  stopwatch_t() noexcept {}
  ~stopwatch_t() noexcept {}

  /**
   * @brief Start time counter.
   *
   */
  void start() noexcept
  {
    start_ = std::chrono::high_resolution_clock::now();
  }

  /**
   * @brief Clear time counter.
   *
   */
  void clear() noexcept
  {
    start_ = {};
  }

  /**
   * @brief Returns amount of elapsed time.
   *
   * @tparam T unit used to return.
   * @return float
   */
  template <typename T>
  float elapsed() noexcept
  {
    //static_assert(is_duration<T>::value);

    auto stop = std::chrono::high_resolution_clock::now();
    std::chrono::duration<float, typename T::period> e = stop - start_;
    return e.count();
  }

  /**
   * @brief Returns true if given duration is elapsed since last call.
   *
   * @tparam T unit used by duration.
   * @param d duration to be checked.
   * @return true
   * @return false
   */
  template <typename T>
  bool is_elapsed(const T& d) noexcept
  {
    //static_assert(is_duration<T>::value);

    static auto prev = start_;

    auto stop = std::chrono::high_resolution_clock::now();
    std::chrono::duration<float, typename T::period> e = stop - prev;

    if (e >= d)
    {
      prev = stop;
      return true;
    }
    else
      return false;
  }

 private:
  /// Time point when the stopwatched started.
  std::chrono::high_resolution_clock::time_point start_;
};

}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
