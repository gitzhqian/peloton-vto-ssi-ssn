//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ssi_txn_manager.h
//
// Identification: src/backend/concurrency/ssi_txn_manager.h
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

struct SsiTxnContext {
  SsiTxnContext(Transaction *t)
      : transaction_(t),
        in_conflict_(false),
        out_conflict_(false),
        is_abort_(false),
        is_finish_(false) {}
  Transaction *transaction_;

  // is_abort() could run without any locks
  // because if it returns wrong result, it just leads to a false abort
  inline bool is_abort() {
    return is_abort_ || (in_conflict_ && out_conflict_);
  }

  bool in_conflict_;
  bool out_conflict_;
  bool is_abort_;
  bool is_finish_;  // is commit finished
  Spinlock lock_;
};

extern thread_local SsiTxnContext *current_ssi_txn_ctx;

struct ReadList {
  SsiTxnContext *txn_ctx;
  ReadList *next;
  ReadList() : txn_ctx(nullptr), next(nullptr) {}
  ReadList(SsiTxnContext *t) : txn_ctx(t), next(nullptr) {}
};

struct SIReadLock {
  SIReadLock() : list(nullptr) {}
  ReadList *list;
  Spinlock lock;
  void Lock() { lock.Lock(); }
  void Unlock() { lock.Unlock(); }
};

class SsiTxnManager: public TransactionManager {
 public:
  SsiTxnManager() : stopped(false), cleaned(false){
    gc_cid = 0;
//    vacuum = std::thread(&SsiTxnManager::CleanUpBg, this);
  }

  virtual ~SsiTxnManager() {
    LOG_INFO("Deconstruct SSI manager");
//    if(!stopped) {
//      stopped = true;
//      vacuum.join();
//    }
  }

  static SsiTxnManager &GetInstance();

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

//  Transaction *BeginTransaction() {
//    // txn_manager_mutex_.WriteLock();
//
//    // protect beginTransaction with a global lock
//    // to ensure that:
//    //    txn_id_a > txn_id_b --> begin_cid_a > begin_cid_b
//    txn_id_t txn_id = GetNextTransactionId();
//    cid_t begin_cid = GetNextCommitId();
//    Transaction *txn = new Transaction(txn_id, begin_cid);
//
//    current_ssi_txn_ctx = new SsiTxnContext(txn);
////    current_txn = txn;
//
//    auto eid = EpochManagerFactory::GetInstance().EnterEpoch(begin_cid);
//    txn->SetEpochId(eid);
//
//
//    txn_table_[txn->GetTransactionId()] = current_ssi_txn_ctx;
//    // txn_manager_mutex_.Unlock();
//    LOG_DEBUG("Begin txn %lu", txn->GetTransactionId());
//    return txn;
//  }

  void DroppingTileGroup(const oid_t &tile_group_id
                                 __attribute__((unused))) {
    CleanUp();
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
  cuckoohash_map<txn_id_t, SsiTxnContext *> txn_table_;

  cuckoohash_map<cid_t, SsiTxnContext *> end_txn_table_;

  cid_t gc_cid;
  // SIReadLocks
  typedef std::map<oid_t, std::unique_ptr<SIReadLock>> TupleReadlocks;
  std::map<std::pair<oid_t, oid_t>, std::unique_ptr<SIReadLock>> sireadlocks;
  // Used to make the vacuum thread stop
  bool stopped;
  bool cleaned;
  // Vacuum thread, GC over 20 ms
  std::thread vacuum;

  // init reserved area of a tuple
  // creator txnid | lock (for read list) | read list head
  // The txn_id could only be the cur_txn's txn id.
  void InitTupleReserved(const txn_id_t txn_id, const oid_t tile_group_id,
                         const oid_t tuple_id) {
//    LOG_DEBUG("init reserved txn %ld, group %u tid %u", txn_id, tile_group_id,
//             tuple_id);

    auto tile_group_header = catalog::Manager::GetInstance()
        .GetTileGroup(tile_group_id)->GetHeader();

    assert(tile_group_header->GetTransactionId(tuple_id) == txn_id);
//    assert(current_txn->GetTransactionId() == txn_id);

    auto reserved_area = tile_group_header->GetReservedFieldRef(tuple_id);

    *(txn_id_t *)(reserved_area + CREATOR_OFFSET) = txn_id;
    new ((reserved_area + LOCK_OFFSET)) Spinlock();
    *(ReadList **)(reserved_area + LIST_OFFSET) = nullptr;
  }

  // Get creator of a tuple
  inline txn_id_t GetCreatorTxnId(storage::TileGroup *tile_group,
                                  const oid_t &tuple_id) {
    return *(txn_id_t *)(tile_group->GetHeader()->GetReservedFieldRef(
        tuple_id) + CREATOR_OFFSET);
  }

  void GetReadLock(const storage::TileGroupHeader *const tile_group_header,
                   const oid_t &tuple_id) {
    auto lock = (Spinlock *)(tile_group_header->GetReservedFieldRef(tuple_id) +
                             LOCK_OFFSET);
    lock->Lock();
  }

  void ReleaseReadLock(const storage::TileGroupHeader *const tile_group_header,
                       const oid_t tuple_id) {
    auto lock = (Spinlock *)(tile_group_header->GetReservedFieldRef(tuple_id) +
                             LOCK_OFFSET);
    lock->Unlock();
  }

  // Add the current txn into the reader list of a tuple
  void AddSIReader(storage::TileGroup *tile_group, const oid_t &tuple_id) {
    ReadList *reader = new ReadList(current_ssi_txn_ctx);

    GetReadLock(tile_group->GetHeader(), tuple_id);
    ReadList **headp = (ReadList **)(
        tile_group->GetHeader()->GetReservedFieldRef(tuple_id) + LIST_OFFSET);
    reader->next = *headp;
    *headp = reader;
    ReleaseReadLock(tile_group->GetHeader(), tuple_id);
  }

  // Remove reader from the reader list of a tuple
  void RemoveSIReader(storage::TileGroupHeader *tile_group_header,
                      const oid_t &tuple_id, txn_id_t txn_id) {
//    LOG_DEBUG("Acquire read lock");
    GetReadLock(tile_group_header, tuple_id);
//    LOG_DEBUG("Acquired");

    ReadList **headp = (ReadList **)(
        tile_group_header->GetReservedFieldRef(tuple_id) + LIST_OFFSET);

    ReadList fake_header;
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

    ReleaseReadLock(tile_group_header, tuple_id);
    if (find == false) {
      assert(false);
    }
  }

  ReadList *GetReaderList(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id) {
    return *(ReadList **)(tile_group_header->GetReservedFieldRef(tuple_id) +
                          LIST_OFFSET);
  }

  inline bool GetInConflict(SsiTxnContext *txn_ctx) {
    return txn_ctx->in_conflict_;
  }

  inline bool GetOutConflict(SsiTxnContext *txn_ctx) {
    return txn_ctx->out_conflict_;
  }

  inline void SetInConflict(SsiTxnContext *txn_ctx) {
//    LOG_DEBUG("Set in conflict %lu", txn_ctx->transaction_->GetTransactionId());
    txn_ctx->in_conflict_ = true;
  }

  inline void SetOutConflict(SsiTxnContext *txn_ctx) {
//    LOG_DEBUG("Set out conflict %lu", txn_ctx->transaction_->GetTransactionId());
    txn_ctx->out_conflict_ = true;
  }

  void RemoveReader(Transaction *txn);

  // Free contexts for SSI manager
  void CleanUpBg();
  void CleanUp();

  static const int CREATOR_OFFSET = 0;
  static const int LOCK_OFFSET = (CREATOR_OFFSET + sizeof(txn_id_t));
  static const int LIST_OFFSET = (LOCK_OFFSET + sizeof(txn_id_t));
};
}
}
