//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_loader.h
//
// Identification: src/include/benchmark/tpcc/tpcc_loader.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <memory>

#include "benchmark/tpcc/tpcc_configuration.h"

namespace peloton {
namespace storage {
class Database;
class DataTable;
class Tuple;
}

class VarlenPool;

namespace benchmark {
namespace tpcc {

extern configuration state;

void CreateTPCCDatabase();

void LoadTPCCDatabase();

/////////////////////////////////////////////////////////
// Tables
/////////////////////////////////////////////////////////

extern storage::Database* tpcc_database;

extern storage::DataTable* warehouse_table;
extern storage::DataTable* district_table;
extern storage::DataTable* item_table;
extern storage::DataTable* customer_table;
extern storage::DataTable* history_table;
extern storage::DataTable* stock_table;
extern storage::DataTable* orders_table;
extern storage::DataTable* new_order_table;
extern storage::DataTable* order_line_table;
extern storage::DataTable* region_table;
extern storage::DataTable* nation_table;
extern storage::DataTable* supplier_table;

/////////////////////////////////////////////////////////
// Constants
/////////////////////////////////////////////////////////

extern const size_t name_length;
extern const size_t middle_name_length;
extern const size_t data_length;
extern const size_t state_length;
extern const size_t zip_length;
extern const size_t street_length;
extern const size_t city_length;
extern const size_t credit_length;
extern const size_t phone_length;
extern const size_t dist_length;

extern double item_min_price;
extern double item_max_price;

extern double warehouse_name_length;
extern double warehouse_min_tax;
extern double warehouse_max_tax;
extern double warehouse_initial_ytd;

extern double district_name_length;
extern double district_min_tax;
extern double district_max_tax;
extern double district_initial_ytd;

extern std::string customers_good_credit;
extern std::string customers_bad_credit;
extern double customers_bad_credit_ratio;
extern double customers_init_credit_lim;
extern double customers_min_discount;
extern double customers_max_discount;
extern double customers_init_balance;
extern double customers_init_ytd;
extern int customers_init_payment_cnt;
extern int customers_init_delivery_cnt;

extern double history_init_amount;
extern size_t history_data_length;

extern int orders_min_ol_cnt;
extern int orders_max_ol_cnt;
extern int orders_init_all_local;
extern int orders_null_carrier_id;
extern int orders_min_carrier_id;
extern int orders_max_carrier_id;

extern int new_orders_per_district;

extern int order_line_init_quantity;
extern int order_line_max_ol_quantity;
extern double order_line_min_amount;
extern size_t order_line_dist_info_length;

extern double stock_original_ratio;
extern int stock_min_quantity;
extern int stock_max_quantity;
extern int stock_dist_count;

extern double payment_min_amount;
extern double payment_max_amount;

extern int stock_min_threshold;
extern int stock_max_threshold;

extern double new_order_remote_txns;

extern const int syllable_count;
extern const char* syllables[];

extern const std::string data_constant;

struct NURandConstant {
  int c_last;
  int c_id;
  int order_line_itme_id;

  NURandConstant();
};

struct Nation {
  int64_t N_NATIONKEY;
  int64_t N_REGIONKEY;
  char N_NAME[25];
  char N_COMMENT[152];

  int64_t Key() const {
    return N_NATIONKEY;
  }

};


const Nation nations[] = {{48, 0, "ALGERIA"},
                          {49, 1, "ARGENTINA"},
                          {50, 1, "BRAZIL"},
                          {51, 1, "CANADA"},
                          {52, 4, "EGYPT" },
                          {53, 0, "ETHIOPIA"},
                          {54, 3, "FRANCE"  },
                          {55, 3, "GERMANY" },
                          {56, 2, "INDIA"   },
                          {57, 2, "INDONESIA"},
                          {65, 4, "IRAN"     },
                          {66, 4, "IRAQ"     },
                          {67, 2, "JAPAN"    },
                          {68, 4, "JORDAN"   },
                          {69, 0, "KENYA"    },
                          {70, 0, "MOROCCO"  },
                          {71, 0, "MOZAMBIQUE"},
                          {72, 1, "PERU"      },
                          {73, 2, "CHINA"     },
                          {74, 3, "ROMANIA"   },
                          {75, 4, "SAUDI ARABIA"                                },
                          {76, 2, "VIETNAM"                                     },
                          {77, 3, "RUSSIA"                                      },
                          {78, 3, "UNITED KINGDOM"                              },
                          {79, 1, "UNITED STATES"                               },
                          {80, 2, "CHINA"                                       },
                          {81, 2, "PAKISTAN"                                    },
                          {82, 2, "BANGLADESH"                                  },
                          {83, 1, "MEXICO"                                      },
                          {84, 2, "PHILIPPINES"                                 },
                          {85, 2, "THAILAND"                                    },
                          {86, 3, "ITALY"                                       },
                          {87, 0, "SOUTH AFRICA"                                },
                          {88, 2, "SOUTH KOREA"                                 },
                          {89, 1, "COLOMBIA"                                    },
                          {90, 3, "SPAIN"                                       },
                          {97, 3, "UKRAINE"                                     },
                          {98, 3, "POLAND"                                      },
                          {99, 0, "SUDAN"                                       },
                          {100, 2, "UZBEKISTAN"                                 },
                          {101, 2, "MALAYSIA"                                   },
                          {102, 1, "VENEZUELA"                                  },
                          {103, 2, "NEPAL"                                      },
                          {104, 2, "AFGHANISTAN"                                },
                          {105, 2, "NORTH KOREA"                                },
                          {106, 2, "TAIWAN"                                     },
                          {107, 0, "GHANA"                                      },
                          {108, 0, "IVORY COAST"                                },
                          {109, 4, "SYRIA"                                      },
                          {110, 0, "MADAGASCAR"                                 },
                          {111, 0, "CAMEROON"                                   },
                          {112, 2, "SRI LANKA"                                  },
                          {113, 3, "ROMANIA"                                    },
                          {114, 3, "NETHERLANDS"                                },
                          {115, 2, "CAMBODIA"                                   },
                          {116, 3, "BELGIUM"                                    },
                          {117, 3, "GREECE"                                     },
                          {118, 3, "PORTUGAL"                                   },
                          {119, 4, "ISRAEL"                                     },
                          {120, 3, "FINLAND"                                    },
                          {121, 2, "SINGAPORE"                                  },
                          {122, 3, "NORWAY"                                     }};

static const char *regions[] = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};

extern NURandConstant nu_rand_const;

/////////////////////////////////////////////////////////
// Tuple Constructors
/////////////////////////////////////////////////////////

std::unique_ptr<storage::Tuple> BuildItemTuple(
    const int item_id, const std::unique_ptr<VarlenPool>& pool);

std::unique_ptr<storage::Tuple> BuildWarehouseTuple(
    const int warehouse_id, const std::unique_ptr<VarlenPool>& pool);

std::unique_ptr<storage::Tuple> BuildDistrictTuple(
    const int district_id, const int warehouse_id,
    const std::unique_ptr<VarlenPool>& pool);

std::unique_ptr<storage::Tuple> BuildCustomerTuple(
    const int customer_id, const int district_id, const int warehouse_id,
    const std::unique_ptr<VarlenPool>& pool);

std::unique_ptr<storage::Tuple> BuildHistoryTuple(
    const int customer_id, const int district_id, const int warehouse_id,
    const int history_district_id, const int history_warehouse_id,
    const std::unique_ptr<VarlenPool>& pool);

std::unique_ptr<storage::Tuple> BuildOrdersTuple(const int orders_id,
                                                 const int district_id,
                                                 const int warehouse_id,
                                                 const bool new_order,
                                                 const int o_ol_cnt);

std::unique_ptr<storage::Tuple> BuildNewOrderTuple(const int orders_id,
                                                   const int district_id,
                                                   const int warehouse_id);

std::unique_ptr<storage::Tuple> BuildOrderLineTuple(
    const int orders_id, const int district_id, const int warehouse_id,
    const int order_line_id, const int ol_supply_w_id, const bool new_order,
    const std::unique_ptr<VarlenPool>& pool);

std::unique_ptr<storage::Tuple> BuildStockTuple(
    const int stock_id, const int s_w_id,
    const std::unique_ptr<VarlenPool>& pool);
std::unique_ptr<storage::Tuple> BuildRegionTuple(
    const int region_id,
    const std::unique_ptr<type::AbstractPool> &pool);
std::unique_ptr<storage::Tuple> BuildNationTuple(
    const int nation_id,
    const std::unique_ptr<type::AbstractPool> &pool);
std::unique_ptr<storage::Tuple> BuildSupplierTuple(
    const int supp_id,
    const std::unique_ptr<type::AbstractPool> &pool);
/////////////////////////////////////////////////////////
// Utils
/////////////////////////////////////////////////////////

std::string GetRandomAlphaNumericString(const size_t string_length);

int GetNURand(int a, int x, int y);

std::string GetLastName(int number);

std::string GetRandomLastName(int max_cid);

bool GetRandomBoolean(double ratio);

int GetRandomInteger(const int lower_bound, const int upper_bound);

int GetRandomIntegerExcluding(const int lower_bound, const int upper_bound,
                              const int exclude_sample);

double GetRandomDouble(const double lower_bound, const double upper_bound);

double GetRandomFixedPoint(int decimal_places, double minimum, double maximum);

std::string GetStreetName();

std::string GetZipCode();

std::string GetCityName();

std::string GetStateName();

int GetTimeStamp();

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
