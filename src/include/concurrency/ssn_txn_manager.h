//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ssn_txn_manager.h
//
// Identification: src/backend/concurrency/ssn_txn_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "concurrency/transaction_manager.h"
#include "storage/tile_group.h"
#include "catalog/manager.h"
#include "libcuckoo/cuckoohash_map.hh"

#include <map>

namespace peloton {
namespace concurrency {

struct SsnTxnContext {
  SsnTxnContext(Transaction *t)
      : transaction_(t),
        pstamp(0),
        sstamp(MAX_CID),
        cstamp(0),
        is_abort_(false),
        is_finish_(false),
        is_comitting_(false){}
  Transaction *transaction_;

  // is_abort() could run without any locks
  // because if it returns wrong result, it just leads to a false abort
  inline bool is_abort() {
    return is_abort_;
  }
  inline bool is_finish() {
    return is_finish_;
  }
//  inline cid_t txn_pstamp() {
//      return pstamp;
//  }
//  inline cid_t txn_sstamp(){
//      return sstamp;
//  }
//  inline cid_t txn_cstamp() {
//    return cstamp;
//  }
  inline bool is_commiting(){
      return is_comitting_;
  }

  cid_t pstamp;
  cid_t sstamp;
  cid_t cstamp;
  bool is_abort_;
  bool is_finish_;  // is commit finished
  bool is_comitting_;
  Spinlock lock_;
};

extern thread_local SsnTxnContext *current_ssn_txn_ctx;

struct ReadnList {
  SsnTxnContext *txn_ctx;
  ReadnList *next;
  ReadnList() : txn_ctx(nullptr), next(nullptr) {}
  ReadnList(SsnTxnContext *t) : txn_ctx(t), next(nullptr) {}
};

struct SInReadLock {
  SInReadLock() : list(nullptr) {}
  ReadnList *list;
  Spinlock lock;
  void Lock() { lock.Lock(); }
  void Unlock() { lock.Unlock(); }
};

class SsnTxnManager: public TransactionManager {
 public:
//  SsnTxnManager() : stopped(false), cleaned(false){
//    gc_cid = 0;
//  }
  SsnTxnManager(){}

  virtual ~SsnTxnManager() {
    LOG_INFO("Deconstruct SSN manager");
  }

  static SsnTxnManager &GetInstance();

  virtual bool IsOccupied(Transaction *const current_txn, const void *position);

  virtual VisibilityType IsVisible(Transaction *const current_txn,
                                   const storage::TileGroupHeader *const tile_group_header,
                                   const oid_t &tuple_id);

  // This method test whether the current transaction is the owner of a tuple.
  virtual bool IsOwner(Transaction *const current_txn,
                       const storage::TileGroupHeader *const tile_group_header,
                       const oid_t &tuple_id);

  // This method tests whether the current transaction has created this version of the tuple
  virtual bool IsWritten(Transaction *const current_txn,
                         const storage::TileGroupHeader *const tile_group_header,
                         const oid_t &tuple_id);

  // This method tests whether it is possible to obtain the ownership.
  virtual bool IsOwnable(Transaction *const current_txn,
                         const storage::TileGroupHeader *const tile_group_header,
                         const oid_t &tuple_id);

  // This method is used to acquire the ownership of a tuple for a transaction.
  virtual bool AcquireOwnership(Transaction *const current_txn,
                                const storage::TileGroupHeader *const tile_group_header,
                                const oid_t &tuple_id);
  virtual void YieldOwnership(Transaction *const current_txn,
                              const oid_t &tile_group_id,
                              const oid_t &tuple_id);



  virtual void PerformInsert(Transaction *const current_txn,
                             const ItemPointer &location,
                             ItemPointer *index_entry_ptr = nullptr);

  virtual bool PerformRead(Transaction *const current_txn,
                           const ItemPointer &location,
                           bool acquire_ownership = false);

  virtual void PerformUpdate(Transaction *const current_txn,
                             const ItemPointer &old_location,
                             const ItemPointer &new_location);

  virtual void PerformDelete(Transaction *const current_txn,
                             const ItemPointer &old_location,
                             const ItemPointer &new_location);

  virtual void PerformUpdate(Transaction *const current_txn,
                             const ItemPointer &location);

  virtual void PerformDelete(Transaction *const current_txn,
                             const ItemPointer &location);

  void DroppingTileGroup(const oid_t &tile_group_id
  __attribute__((unused))) {
//    CleanUp();
  }

  virtual ResultType CommitTransaction(Transaction *const current_txn);

  virtual ResultType AbortTransaction(Transaction *const current_txn);

  virtual Transaction *BeginTransaction(const size_t thread_id = 0);

  virtual Transaction *BeginReadonlyTransaction(const size_t thread_id = 0);

  virtual void EndTransaction(Transaction *current_txn);

  virtual void EndReadonlyTransaction(Transaction *current_txn);

 private:
  std::atomic<cid_t> next_cid_;
  std::atomic<cid_t> next_txn_id_;
  std::atomic<cid_t> maximum_grant_cid_;
  // Mutex to protect txn_table_ and sireadlocks
  // RWLock txn_manager_mutex_;
  // mutex to avoid re-enter clean up
  std::mutex clean_mutex_;
  // Transaction contexts
  cuckoohash_map<txn_id_t, SsnTxnContext *> txn_table_;

  cuckoohash_map<cid_t, SsnTxnContext *> end_txn_table_;

  // init reserved area of a tuple
  // creator txnid | lock (for read list) | read list head
  // The txn_id could only be the cur_txn's txn id.
  void InitTupleReserved(const txn_id_t t_cstamp, const oid_t tile_group_id,
                         const oid_t tuple_id) {
//    LOG_DEBUG("init reserved txn %ld, group %u tid %u", txn_id, tile_group_id,
//             tuple_id);

    auto tile_group_header = catalog::Manager::GetInstance()
        .GetTileGroup(tile_group_id)->GetHeader();

//    assert(tile_group_header->GetTransactionId(tuple_id) == t_cstamp);

    auto reserved_area = tile_group_header->GetReservedFieldRef(tuple_id);

    *(txn_id_t *)(reserved_area + CREATOR_OFFSET) = t_cstamp;
    new ((reserved_area + LOCK_OFFSET)) Spinlock();
    *(cid_t *)(reserved_area + PSTAMP_OFFSET) = t_cstamp;
    *(ReadnList **)(reserved_area + LIST_OFFSET) = nullptr;
  }

  // Get creator of a tuple
  inline txn_id_t GetVnCstamp(storage::TileGroup *tile_group,
                                  const oid_t &tuple_id) {
    return *(txn_id_t *)(tile_group->GetHeader()->GetReservedFieldRef(
        tuple_id) + CREATOR_OFFSET);
  }

  //Get pstamp of a tuple
  inline txn_id_t GetVnPstamp(const storage::TileGroup *tile_group,
                                  const oid_t &tuple_id) {
    return *(txn_id_t *)(tile_group->GetHeader()->GetReservedFieldRef(
        tuple_id) + PSTAMP_OFFSET);
  }
  inline void SetVnPstamp(const cid_t v_pstamp, const oid_t tile_group_id,
                          const oid_t tuple_id) {
    auto tile_group_header = catalog::Manager::GetInstance()
        .GetTileGroup(tile_group_id)->GetHeader();

    auto reserved_area = tile_group_header->GetReservedFieldRef(tuple_id);
    *(cid_t *)(reserved_area + PSTAMP_OFFSET) = v_pstamp;
  }
  void GetReadSsnLock(const storage::TileGroupHeader *const tile_group_header,
                   const oid_t &tuple_id) {
    auto lock = (Spinlock *)(tile_group_header->GetReservedFieldRef(tuple_id) +
                             LOCK_OFFSET);
    lock->Lock();
  }

  void ReleaseReadSsnLock(const storage::TileGroupHeader *const tile_group_header,
                       const oid_t tuple_id) {
    auto lock = (Spinlock *)(tile_group_header->GetReservedFieldRef(tuple_id) +
                             LOCK_OFFSET);
    lock->Unlock();
  }

  // Add the current txn into the reader list of a tuple
  void AddSsnReader(storage::TileGroup *tile_group, const oid_t &tuple_id) {
    ReadnList *reader = new ReadnList(current_ssn_txn_ctx);

    GetReadSsnLock(tile_group->GetHeader(), tuple_id);
    ReadnList **headp = (ReadnList **)(
        tile_group->GetHeader()->GetReservedFieldRef(tuple_id) + LIST_OFFSET);
    reader->next = *headp;
    *headp = reader;
    ReleaseReadSsnLock(tile_group->GetHeader(), tuple_id);
  }

  // Remove reader from the reader list of a tuple
  void RemoveSsnReader(storage::TileGroupHeader *tile_group_header,
                      const oid_t &tuple_id, txn_id_t txn_id) {
//    LOG_DEBUG("Acquire read lock");
    GetReadSsnLock(tile_group_header, tuple_id);
//    LOG_DEBUG("Acquired");

    char *header_reserved = tile_group_header->GetReservedFieldRef(tuple_id)
                            + LIST_OFFSET;
    ReadnList **headp = (ReadnList **)(header_reserved);

    ReadnList fake_header;
    fake_header.next = *headp;
    auto prev = &fake_header;
    auto next = prev->next;
    bool find = false;

    while (next != nullptr) {
      if (next->txn_ctx->transaction_->GetTransactionId() == txn_id) {
        find = true;
        prev->next = next->next;
        delete next;
        break;
      }
      prev = next;
      next = next->next;
    }

    *headp = fake_header.next;

    ReleaseReadSsnLock(tile_group_header, tuple_id);
    if (find == false) {
      assert(false);
    }
  }

  ReadnList *GetReaderList(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id) {
    return *(ReadnList **)(tile_group_header->GetReservedFieldRef(tuple_id) +
                          LIST_OFFSET);
  }

  inline cid_t GetTxnPstamp(SsnTxnContext *txn_ctx) {
    return txn_ctx->pstamp;
  }

  inline cid_t GetTxnSstamp(SsnTxnContext *txn_ctx) {
    return txn_ctx->sstamp;
  }

  inline void SetTxnPstamp(SsnTxnContext *txn_ctx, cid_t pstamp_) {
    txn_ctx->pstamp = pstamp_;
  }

  inline void SetTxnSstamp(SsnTxnContext *txn_ctx, cid_t sstamp_) {
    txn_ctx->sstamp = sstamp_;
  }
  inline void SetTxnCstamp(SsnTxnContext *txn_ctx, cid_t cstamp_) {
    txn_ctx->cstamp = cstamp_;
  }

  inline cid_t GetTxnCstamp(SsnTxnContext *txn_ctx) {

    return txn_ctx->cstamp;
  }

  void RemoveSsnReader(Transaction *txn);

  //cstamp of the tuple
  static const int CREATOR_OFFSET = 0;
  static const int LOCK_OFFSET = (CREATOR_OFFSET + sizeof(txn_id_t));
  //pstamp of the tuple
  static const int PSTAMP_OFFSET = (LOCK_OFFSET + sizeof(txn_id_t));
  //perform read list
  static const int LIST_OFFSET = (PSTAMP_OFFSET + sizeof(txn_id_t));
};
}
}
