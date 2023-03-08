//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ssn_txn_manager.cpp
//
// Identification: src/backend/concurrency/ssn_txn_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/ssn_txn_manager.h"
#include "catalog/manager.h"
#include "common/logger.h"
#include "common/platform.h"
#include "concurrency/transaction.h"
#include "gc/gc_manager_factory.h"
#include "logging/log_manager.h"
#include <set>

namespace peloton {
namespace concurrency {

thread_local SsnTxnContext *current_ssn_txn_ctx;

SsnTxnManager &SsnTxnManager::GetInstance() {
  static SsnTxnManager txn_manager;
  return txn_manager;
}

Transaction *SsnTxnManager::BeginTransaction(const size_t thread_id) {

//  auto &log_manager = logging::LogManager::GetInstance();
//  log_manager.PrepareLogging();

  // protect beginTransaction with a global lock
  // to ensure that:
  //    txn_id_a > txn_id_b --> begin_cid_a > begin_cid_b
  txn_id_t txn_id = GetNextTransactionId();
  cid_t begin_cid = GetNextCommitId();
  Transaction *txn = new Transaction(txn_id, begin_cid, thread_id);

  current_ssn_txn_ctx = new SsnTxnContext(txn);

  auto eid = EpochManagerFactory::GetInstance().EnterEpoch(thread_id);
  txn->SetEpochId(eid);


  txn_table_[txn->GetTransactionId()] = current_ssn_txn_ctx;
  // txn_manager_mutex_.Unlock();
//  LOG_DEBUG("Begin txn %lu", txn->GetTransactionId());


  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()
        ->GetTxnLatencyMetric()
        .StartTimer();
  }

  return txn;
}

Transaction *SsnTxnManager::BeginReadonlyTransaction(  const size_t thread_id) {
  Transaction *txn = nullptr;
  // transaction processing with centralized epoch manager
  txn_id_t txn_id = GetNextTransactionId();
  cid_t begin_cid = GetNextCommitId();
  txn = new Transaction(txn_id, begin_cid, thread_id, true);
  current_ssn_txn_ctx = new SsnTxnContext(txn);

  auto eid = EpochManagerFactory::GetInstance().EnterEpoch(thread_id);
  txn->SetEpochId(eid);

  txn_table_[txn->GetTransactionId()] = current_ssn_txn_ctx;

  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()
        ->GetTxnLatencyMetric()
        .StartTimer();
  }
  return txn;
}

void SsnTxnManager::EndTransaction(Transaction *current_txn) {

  EpochManagerFactory::GetInstance().ExitEpoch(
      current_txn->GetThreadId(),
      current_txn->GetEpochId());


//  auto &log_manager = logging::LogManager::GetInstance();

  if (current_txn->GetResult() == ResultType::SUCCESS) {
    if (current_txn->IsGCSetEmpty() != true) {
      gc::GCManagerFactory::GetInstance().
          RecycleTransaction(current_txn->GetGCSetPtr(), current_txn->GetBeginCommitId());
    }
    // Log the transaction's commit
    // For time stamp ordering, every transaction only has one timestamp
//    log_manager.LogCommitTransaction(current_txn->GetBeginCommitId());
  } else {
    if (current_txn->IsGCSetEmpty() != true) {
      gc::GCManagerFactory::GetInstance().
          RecycleTransaction(current_txn->GetGCSetPtr(), GetNextCommitId());
    }
//    log_manager.DoneLogging();
  }

  RemoveSsnReader(current_txn);

  delete current_txn;
  current_txn = nullptr;

  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()
        ->GetTxnLatencyMetric()
        .RecordLatency();
  }
}

void SsnTxnManager::EndReadonlyTransaction(
      Transaction *current_txn) {
  PL_ASSERT(current_txn->IsDeclaredReadOnly() == true);

  EpochManagerFactory::GetInstance().ExitEpoch(
      current_txn->GetThreadId(),
      current_txn->GetEpochId());

  delete current_txn;
  current_txn = nullptr;

  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()
        ->GetTxnLatencyMetric()
        .RecordLatency();
  }
}
// Visibility check
// read an commited version
VisibilityType SsnTxnManager::IsVisible(Transaction *const current_txn,
                                        const storage::TileGroupHeader *const tile_group_header,
                                        const oid_t &tuple_id) {
  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);
  cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  if (tuple_txn_id == INVALID_TXN_ID) {
    // the tuple is not available.
    return VisibilityType::INVISIBLE;
  }
  bool own = (current_txn->GetTransactionId() == tuple_txn_id);

  // there are exactly two versions that can be owned by a transaction.
  // unless it is an insertion.
  if (own == true) {
    if (tuple_begin_cid == MAX_CID && tuple_end_cid != INVALID_CID) {
      assert(tuple_end_cid == MAX_CID);
      // the only version that is visible is the newly inserted one.
      return VisibilityType::OK;
    }else if (tuple_end_cid == INVALID_CID) {
      // tuple being deleted by current txn
      return VisibilityType::DELETED;
    } else {
      // the older version is not visible.
      return VisibilityType::INVISIBLE;
    }
  } else {
    bool activated = (current_txn->GetBeginCommitId() >= tuple_begin_cid);
    bool invalidated = (current_txn->GetBeginCommitId() >= tuple_end_cid);
    if (tuple_txn_id != INITIAL_TXN_ID) {
      // if the tuple is owned by other transactions.
      if (tuple_begin_cid == MAX_CID) {
        // in this protocol, we do not allow cascading abort. so never read an
        // uncommitted version.
        return VisibilityType::INVISIBLE;
      } else {
        // the older version may be visible.
        if (activated && !invalidated) {
          return VisibilityType::OK;
        } else {
          return VisibilityType::INVISIBLE;
        }
      }
    } else {
      // if the tuple is not owned by any transaction.
      //if the tuple_txn_id == INVALID_TXN_ID
      if (activated && !invalidated) {
        return VisibilityType::OK;
      } else {
        return VisibilityType::INVISIBLE;
      }
    }
  }
}
//check the current transaction is updating the tuple
//this is call by update or delete operation
bool SsnTxnManager::IsOwner(Transaction *const current_txn,
                            const storage::TileGroupHeader *const tile_group_header,
                            const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  return tuple_txn_id == current_txn->GetTransactionId();
}

// if the tuple is not owned by any transaction and is visible to current
// transaction. will only be performed by deletes and updates.
bool SsnTxnManager::IsOwnable(UNUSED_ATTRIBUTE Transaction *const current_txn,
                              const storage::TileGroupHeader *const tile_group_header,
                              const oid_t &tuple_id) {
//  assert(current_txn != nullptr);
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  auto tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  return tuple_txn_id == INITIAL_TXN_ID && tuple_end_cid == MAX_CID;
}

//acquire the lock of the tuple
//and set the transaction id = current transaction id
//going to update or delete the tuple
//get the reader list,traverse the txn list
//  if the txn is running, Tx(w)->Tx(r)
//  if the txn is commited after current txn start, then abort
bool SsnTxnManager::AcquireOwnership(Transaction *const current_txn,
                                     const storage::TileGroupHeader *const tile_group_header,
                                     const oid_t &tuple_id) {
  auto txn_id = current_txn->GetTransactionId();
//  LOG_DEBUG("AcquireOwnership %lu", txn_id);

  // jump to abort directly
  if(current_ssn_txn_ctx->is_abort()){
    assert(current_ssn_txn_ctx->is_abort_ == false);
    LOG_DEBUG("detect conflicts");
    return false;
  }

  //set current txn pstamp(high watermark) with max(t_pstamp,v_pre_pstamp)
  //tuple_id is the old version
  auto pre_tile_group = tile_group_header->GetTileGroup();
  GetReadSsnLock(pre_tile_group->GetHeader(), tuple_id);

  auto t_pstamp = GetTxnPstamp(current_ssn_txn_ctx);
  auto v_pre_pstamp = GetVnPstamp(pre_tile_group, tuple_id);
  SetTxnPstamp(current_ssn_txn_ctx, std::max(t_pstamp, v_pre_pstamp));

  if (GetTxnPstamp(current_ssn_txn_ctx) > GetTxnSstamp(current_ssn_txn_ctx)) {
    return false;
  }

  ReleaseReadSsnLock(pre_tile_group->GetHeader(), tuple_id);

  if (tile_group_header->SetAtomicTransactionId(tuple_id, txn_id) == false) {
//    LOG_DEBUG("Fail to insert new tuple. Set txn failure.");
    return false;
  }

  return true;
}
//index check when insert tuple
bool SsnTxnManager::IsOccupied(Transaction *const current_txn, const void *position_ptr){
  ItemPointer &position = *((ItemPointer *)position_ptr);

  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(position.block)->GetHeader();
  auto tuple_id = position.offset;

  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);
  cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);

  if (tuple_txn_id == INVALID_TXN_ID) {
    // the tuple is not available.
    return false;
  }

  // the tuple has already been owned by the current transaction.
  bool own = (current_txn->GetTransactionId() == tuple_txn_id);
  // the tuple has already been committed.
  bool activated = (current_txn->GetBeginCommitId() >= tuple_begin_cid);
  // the tuple is not visible.
  bool invalidated = (current_txn->GetBeginCommitId() >= tuple_end_cid);

  // there are exactly two versions that can be owned by a transaction.
  // unless it is an insertion/select for update.
  if (own == true) {
    if (tuple_begin_cid == MAX_CID && tuple_end_cid != INVALID_CID) {
      PL_ASSERT(tuple_end_cid == MAX_CID);
      // the only version that is visible is the newly inserted one.
      return true;
    } else if (current_txn->GetRWType(position) == RWType::READ_OWN) {
      // the ownership is from a select-for-update read operation
      return true;
    } else {
      // the older version is not visible.
      return false;
    }
  } else {
    if (tuple_txn_id != INITIAL_TXN_ID) {
      // if the tuple is owned by other transactions.
      if (tuple_begin_cid == MAX_CID) {
        // uncommitted version.
        if (tuple_end_cid == INVALID_CID) {
          // dirty delete is invisible
          return false;
        } else {
          // dirty update or insert is visible
          return true;
        }
      } else {
        // the older version may be visible.
        if (activated && !invalidated) {
          return true;
        } else {
          return false;
        }
      }
    } else {
      // if the tuple is not owned by any transaction.
      if (activated && !invalidated) {
        return true;
      } else {
        return false;
      }
    }
  }
}
bool SsnTxnManager::IsWritten(UNUSED_ATTRIBUTE Transaction *const current_txn,
                              UNUSED_ATTRIBUTE const storage::TileGroupHeader *const tile_group_header,
                              UNUSED_ATTRIBUTE const oid_t &tuple_id){
  return  true;
}
void SsnTxnManager::YieldOwnership(UNUSED_ATTRIBUTE Transaction *const current_txn,
                                   UNUSED_ATTRIBUTE const oid_t &tile_group_id,
                                   UNUSED_ATTRIBUTE const oid_t &tuple_id){

}

bool SsnTxnManager::PerformRead(Transaction *const current_txn,
                                const ItemPointer &location,
                                UNUSED_ATTRIBUTE bool acquire_ownership ){
  auto tile_group_id = location.block;
  auto tuple_id = location.offset;
//   LOG_DEBUG("Perform Read %u %u", tile_group_id, tuple_id);
//  LOG_DEBUG("Perform Read acquire_ownership %d ", acquire_ownership);

  // jump to abort directly
  if(current_ssn_txn_ctx->is_abort()){
    assert(current_ssn_txn_ctx->is_abort_ == false);
    LOG_DEBUG("detect conflicts");
    return false;
  }

  auto tile_group = catalog::Manager::GetInstance().GetTileGroup(tile_group_id);

  auto &rw_set = current_txn->GetReadWriteSet();

  if(rw_set.count(tile_group_id) == 0 ||
     rw_set.at(tile_group_id).count(tuple_id) == 0){
    //    LOG_DEBUG("Not read before");
    // Previously, this tuple hasn't been read, add the txn to the reader list
    // of the tuple
    AddSsnReader(tile_group.get(), tuple_id);
  }

  //if the location is in the writes of the current transaction
  if (rw_set.find(tile_group_id) != rw_set.end() &&
      rw_set.at(tile_group_id).find(tuple_id) != rw_set.at(tile_group_id).end()) {
    if(rw_set.at(tile_group_id).at(tuple_id) == RWType::DELETE ||
        rw_set.at(tile_group_id).at(tuple_id) == RWType::INS_DEL ||
        rw_set.at(tile_group_id).at(tuple_id) == RWType::UPDATE ||
        rw_set.at(tile_group_id).at(tuple_id) == RWType::INSERT){
      return true;
      }
  }

  //set the pstamp(high watermark) with the max(t_ps, v_cs)
  auto v_cstamp = GetVnCstamp(tile_group.get(), tuple_id);
  auto t_pstamp = GetTxnPstamp(current_ssn_txn_ctx);
  SetTxnPstamp(current_ssn_txn_ctx, std::max(t_pstamp, v_cstamp));

  auto tile_group_header = tile_group->GetHeader();
  auto v_sstamp = tile_group_header->GetEndCommitId(tuple_id);
  if(v_sstamp == MAX_CID){
    //the version tuple has not been overriten
    current_txn->RecordRead(location);
  }else{
    auto t_sstamp = GetTxnSstamp(current_ssn_txn_ctx) ;
    //set the sstamp(low watermark) with the min(t_ss, v_ss)
    SetTxnSstamp(current_ssn_txn_ctx, std::min(t_sstamp, v_sstamp));
  }

  //verify and abort
  //if predecessor > successor, then return false
  if(GetTxnPstamp(current_ssn_txn_ctx) > GetTxnSstamp(current_ssn_txn_ctx) ){
    return false;
  }

  return true;
}

void SsnTxnManager::PerformInsert(Transaction *const current_txn,
                                  const ItemPointer &location,
                                  ItemPointer *index_entry_ptr) {
  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

//  LOG_DEBUG("Perform insert %u %u", tile_group_id, tuple_id);

  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  // Set MVCC info
  assert(tile_group_header->GetTransactionId(tuple_id) == INVALID_TXN_ID);
  assert(tile_group_header->GetBeginCommitId(tuple_id) == MAX_CID);
  assert(tile_group_header->GetEndCommitId(tuple_id) == MAX_CID);

  tile_group_header->SetTransactionId(tuple_id, transaction_id);

  // No need to set next item pointer.
  current_txn->RecordInsert(location);

  // Write down the head pointer's address in tile group header
  tile_group_header->SetIndirection(tuple_id, index_entry_ptr);

//  InitTupleReserved(current_txn->GetTransactionId(), tile_group_id, tuple_id);

//  return true;
}

void SsnTxnManager::PerformUpdate(Transaction *const current_txn,
                                  const ItemPointer &old_location,
                                  const ItemPointer &new_location) {
  auto transaction_id = current_txn->GetTransactionId();

  auto tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(old_location.block)->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(new_location.block)->GetHeader();

  // if we can perform update, then we must already locked the older version.
  assert(tile_group_header->GetTransactionId(old_location.offset) == transaction_id);
  // Set double linked list
  tile_group_header->SetNextItemPointer(old_location.offset, new_location);
  new_tile_group_header->SetPrevItemPointer(new_location.offset, old_location);

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);
  new_tile_group_header->SetBeginCommitId(new_location.offset, MAX_CID);
  new_tile_group_header->SetEndCommitId(new_location.offset, MAX_CID);

  auto old_prev = tile_group_header->GetPrevItemPointer(old_location.offset);
  // if the transaction is not updating the latest version,
  // then do not change item pointer header.
  if (old_prev.IsNull() == true) {
    // if we are updating the latest version.
    // Set the header information for the new version
    ItemPointer *index_entry_ptr =
        tile_group_header->GetIndirection(old_location.offset);

    if (index_entry_ptr != nullptr) {

      new_tile_group_header->SetIndirection(new_location.offset,
                                            index_entry_ptr);

      // Set the index header in an atomic way.
      // We do it atomically because we don't want any one to see a half-done
      // pointer.
      // In case of contention, no one can update this pointer when we are
      // updating it
      // because we are holding the write lock. This update should success in
      // its first trial.
      UNUSED_ATTRIBUTE auto res =
          AtomicUpdateItemPointer(index_entry_ptr, new_location);
      PL_ASSERT(res == true);
    }
  }else{
    auto old_prev_tile_group_header = catalog::Manager::GetInstance()
        .GetTileGroup(old_prev.block)->GetHeader();

    // once everything is set, we can allow traversing the new version.
    old_prev_tile_group_header->SetNextItemPointer(old_prev.offset,
                                                   new_location);
  }

//  InitTupleReserved(transaction_id, new_location.block, new_location.offset);
  current_txn->RecordUpdate(old_location);

//  return true;
}

void SsnTxnManager::PerformUpdate(Transaction *const current_txn,
                                  const ItemPointer &location) {
  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  assert(tile_group_header->GetTransactionId(tuple_id) == transaction_id);

  // Set MVCC info
  tile_group_header->SetTransactionId(tuple_id, transaction_id);
  tile_group_header->SetBeginCommitId(tuple_id, MAX_CID);
  tile_group_header->SetEndCommitId(tuple_id, MAX_CID);

  // Add the old tuple into the update set
  auto old_location = tile_group_header->GetPrevItemPointer(tuple_id);
  if (old_location.IsNull() == false) {
    // Update an inserted version
    current_txn->RecordUpdate(old_location);
  }

}

void SsnTxnManager::PerformDelete(Transaction *const current_txn,
                                  const ItemPointer &old_location,
                                  const ItemPointer &new_location) {
  auto tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(old_location.block)
      ->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  auto new_tile_group_header = catalog::Manager::GetInstance()
      .GetTileGroup(new_location.block)
      ->GetHeader();

  // Set up double linked list
  tile_group_header->SetNextItemPointer(old_location.offset, new_location);
  new_tile_group_header->SetPrevItemPointer(new_location.offset, old_location);

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);
  new_tile_group_header->SetBeginCommitId(new_location.offset, MAX_CID);
  new_tile_group_header->SetEndCommitId(new_location.offset, INVALID_CID);

  auto old_prev = tile_group_header->GetPrevItemPointer(old_location.offset);
  // if the transaction is not deleting the latest version,
  // then do not change item pointer header.
  if (old_prev.IsNull() == true) {
    // if we are deleting the latest version.
    // Set the header information for the new version
    ItemPointer *index_entry_ptr =
        tile_group_header->GetIndirection(old_location.offset);

    // if there's no primary index on a table, then index_entry_ptry == nullptr.
    if (index_entry_ptr != nullptr) {
      new_tile_group_header->SetIndirection(new_location.offset,
                                            index_entry_ptr);

      // Set the index header in an atomic way.
      // We do it atomically because we don't want any one to see a half-down
      // pointer
      // In case of contention, no one can update this pointer when we are
      // updating it
      // because we are holding the write lock. This update should success in
      // its first trial.
      UNUSED_ATTRIBUTE auto res =
          AtomicUpdateItemPointer(index_entry_ptr, new_location);
      PL_ASSERT(res == true);
    }
  }else{
    auto old_prev_tile_group_header = catalog::Manager::GetInstance()
        .GetTileGroup(old_prev.block)->GetHeader();

    old_prev_tile_group_header->SetNextItemPointer(old_prev.offset,
                                                   new_location);
  }

  // Add the old tuple into the delete set
//  InitTupleReserved(transaction_id, new_location.block, new_location.offset);
  current_txn->RecordUpdate(old_location);

  //set current txn pstamp(high watermark) with max(t_pstamp,v_pre_pstamp)
  auto pre_tile_group = catalog::Manager::GetInstance().GetTileGroup(old_prev.block);
  auto t_pstamp = GetTxnPstamp(current_ssn_txn_ctx);
  auto v_pre_pstamp = GetVnPstamp(pre_tile_group.get(), old_prev.offset);
  SetTxnPstamp(current_ssn_txn_ctx, std::max(t_pstamp, v_pre_pstamp));
//  return true;
}

void SsnTxnManager::PerformDelete(Transaction *const current_txn,
                                  const ItemPointer &location) {
  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto tile_group_header =
      catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();
  auto transaction_id = current_txn->GetTransactionId();

  tile_group_header->SetTransactionId(tuple_id, transaction_id);
  tile_group_header->SetBeginCommitId(tuple_id, MAX_CID);
  tile_group_header->SetEndCommitId(tuple_id, INVALID_CID);

  // Add the old tuple into the delete set
  auto old_location = tile_group_header->GetPrevItemPointer(tuple_id);
  if (old_location.IsNull() == false) {
    // delete an inserted version
    current_txn->RecordDelete(old_location);
  } else {
    // if this version is newly inserted.
    current_txn->RecordDelete(location);
  }
}

ResultType SsnTxnManager::CommitTransaction(Transaction *const current_txn) {
  LOG_TRACE("Committing peloton txn : %lu ", current_txn->GetTransactionId());

  if (current_txn->IsDeclaredReadOnly() == true) {
    EndReadonlyTransaction(current_txn);
    return ResultType::SUCCESS;
  }

  auto &manager = catalog::Manager::GetInstance();
  auto &rw_set = current_txn->GetReadWriteSet();
  auto gc_set = current_txn->GetGCSetPtr();
  cid_t end_commit_id = GetNextCommitId();
  txn_id_t t_cstamp = GetNextTransactionId();
//  ResultType ret;

  //pre-commit
  bool should_abort = false;
  {
    current_ssn_txn_ctx->lock_.Lock();
    current_ssn_txn_ctx->is_comitting_ = true;
    SetTxnCstamp(current_ssn_txn_ctx, t_cstamp);
    current_ssn_txn_ctx->lock_.Unlock();
    //SetTxnSstamp()
    auto t_sstamp = GetTxnSstamp(current_ssn_txn_ctx);
    //finilize the current txn sstamp(low watermark)
    SetTxnSstamp(current_ssn_txn_ctx, std::min(t_sstamp, t_cstamp));

    cid_t min_sstamp = MAX_CID;
    cid_t max_pstamp = 0;
    auto t_pstamp = GetTxnPstamp(current_ssn_txn_ctx);
    //traverse the rwset
    for (auto &tile_group_entry : rw_set) {
      oid_t tile_group_id = tile_group_entry.first;
      auto tile_group = manager.GetTileGroup(tile_group_id);
      auto tile_group_header = tile_group->GetHeader();
      for (auto &tuple_entry : tile_group_entry.second) {
        auto tuple_slot = tuple_entry.first;
        if (tuple_entry.second == RWType::UPDATE ||
                   tuple_entry.second == RWType::DELETE) {
            //old version pstamp
            auto v_pre_pstamp = GetVnPstamp(tile_group.get(), tuple_slot);
            ReadnList *header = GetReaderList(tile_group_header, tuple_slot);
            while (header != nullptr) {
              // For all owner of siread lock on this version
              auto reader_ctx = header->txn_ctx;
              // Lock the transaction context
              // Myself || owner is (or should be) aborted  skip
              if (reader_ctx == current_ssn_txn_ctx || reader_ctx->is_abort()) {
                header = header->next;
                continue;
              }
              auto r_cstamp = GetTxnCstamp(reader_ctx);
              if (r_cstamp < t_cstamp) {
                if (reader_ctx->is_finish()) {
                  max_pstamp = std::max(t_pstamp, r_cstamp);
                }
              }
              header = header->next;
            }

          max_pstamp = std::max(max_pstamp, v_pre_pstamp);
        }else if(tuple_entry.second == RWType::READ){
            //get the read tuple version
            auto txn_sstamp = GetTxnSstamp(current_ssn_txn_ctx);
            auto writer_ = tile_group->GetHeader()->GetTransactionId(tuple_slot);
            auto v_sstamp = tile_group->GetHeader()->GetEndCommitId(tuple_slot);
            if (v_sstamp != MAX_CID){
              //may someone is commiting overrite the version
              SsnTxnContext *writer_txn = nullptr;
              if (txn_table_.find(writer_, writer_txn) && !writer_txn->is_abort()){
                // overwriter might haven't committed, be commited after me, or before me
                // we only care if the successor is committed *before* me.
                // if someone start before me, then commit before me
                if(GetTxnCstamp(writer_txn) < GetTxnCstamp(current_ssn_txn_ctx)){
                  while(writer_txn->is_commiting()){
                    if(writer_txn->is_finish()){
                      auto writer_sstamp = GetTxnSstamp(writer_txn) ;
                      min_sstamp = std::min(txn_sstamp, writer_sstamp);
                      break;
                    }
                  }
                }
              }
            }else{
              min_sstamp = std::min(txn_sstamp, v_sstamp);
            }
        }
      }
    }

    SetTxnSstamp(current_ssn_txn_ctx, min_sstamp);
    SetTxnPstamp(current_ssn_txn_ctx, max_pstamp);

    if(GetTxnPstamp(current_ssn_txn_ctx) > GetTxnSstamp(current_ssn_txn_ctx)){
      should_abort = true;
    }
  }

  if (should_abort) {
    LOG_DEBUG("Abort because RW conflict");
    return AbortTransaction(current_txn);
  }
  current_ssn_txn_ctx->is_finish_ = true;

  //////////////////////////////////////////////////////////
  //post-commit
//  auto &log_manager = logging::LogManager::GetInstance();
//  log_manager.LogBeginTransaction(end_commit_id);
  // install everything.
  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();
    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      if (tuple_entry.second == RWType::UPDATE) {
        // we must guarantee that, at any time point,
        //  only one version is visible.
        ItemPointer new_version =
            tile_group_header->GetNextItemPointer(tuple_slot);

        PL_ASSERT(new_version.IsNull() == false);

        auto cid = tile_group_header->GetEndCommitId(tuple_slot);
        PL_ASSERT(cid > end_commit_id);
        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset,
                                                end_commit_id);
        new_tile_group_header->SetEndCommitId(new_version.offset, cid);

        COMPILER_MEMORY_FENCE;

//        tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);
        auto t_sstamp = GetTxnSstamp(current_ssn_txn_ctx);
        tile_group_header->SetEndCommitId(tuple_slot, t_sstamp);
        InitTupleReserved(t_cstamp, new_version.block, new_version.offset);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INITIAL_TXN_ID);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // add to gc set.
        gc_set->operator[](tile_group_id)[tuple_slot] = false;

      } else if (tuple_entry.second == RWType::DELETE) {
        ItemPointer new_version =
            tile_group_header->GetPrevItemPointer(tuple_slot);

        auto cid = tile_group_header->GetEndCommitId(tuple_slot);
        PL_ASSERT(cid > end_commit_id);
        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset,
                                                end_commit_id);
        new_tile_group_header->SetEndCommitId(new_version.offset, cid);

        COMPILER_MEMORY_FENCE;

//        tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);
        auto t_sstamp = GetTxnSstamp(current_ssn_txn_ctx);
        tile_group_header->SetEndCommitId(tuple_slot, t_sstamp);
        InitTupleReserved(t_cstamp, new_version.block, new_version.offset);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // add to gc set.
        // we need to recycle both old and new versions.
        // we require the GC to delete tuple from index only once.
        // recycle old version, delete from index
        gc_set->operator[](tile_group_id)[tuple_slot] = true;
        // recycle new version (which is an empty version), do not delete from index
        gc_set->operator[](new_version.block)[new_version.offset] = false;

      } else if (tuple_entry.second == RWType::INSERT) {
//        PL_ASSERT(tile_group_header->GetTransactionId(tuple_slot) ==
//                  current_txn->GetTransactionId());
        // set the begin commit id to persist insert
        tile_group_header->SetBeginCommitId(tuple_slot, end_commit_id);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
        InitTupleReserved(t_cstamp, tile_group_id, tuple_slot);

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

      } else if (tuple_entry.second == RWType::READ) {
        auto v_pstamp = GetVnPstamp(tile_group.get(), tuple_slot);
        SetVnPstamp(std::max(v_pstamp, t_cstamp), tile_group_id, tuple_slot);
        COMPILER_MEMORY_FENCE;
      }
    }
  }

//  log_manager.LogCommitTransaction(end_commit_id);

  current_ssn_txn_ctx->is_finish_ = true;

  end_txn_table_[current_ssn_txn_ctx->transaction_->GetEndCommitId()] = current_ssn_txn_ctx;

  EndTransaction(current_ssn_txn_ctx->transaction_);
//  LOG_DEBUG("Committing peloton txn finished: %lu ", current_txn->GetTransactionId());

  return ResultType::SUCCESS;
}

ResultType SsnTxnManager::AbortTransaction(Transaction *const current_txn) {
//  LOG_DEBUG("Aborting peloton txn : %lu ", current_txn->GetTransactionId());

  if (current_ssn_txn_ctx->is_abort_ == false) {
    // Set abort flag
    current_ssn_txn_ctx->lock_.Lock();
    current_ssn_txn_ctx->is_abort_ = true;
    current_ssn_txn_ctx->lock_.Unlock();
  }

  auto &manager = catalog::Manager::GetInstance();

  auto &rw_set = current_txn->GetReadWriteSet();

  auto gc_set = current_txn->GetGCSetPtr();

  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto tile_group = manager.GetTileGroup(tile_group_id);
    auto tile_group_header = tile_group->GetHeader();

    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;
      if (tuple_entry.second == RWType::UPDATE) {
        // we do not set begin cid for old tuple.
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
        ItemPointer new_version =
            tile_group_header->GetNextItemPointer(tuple_slot);
        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

        COMPILER_MEMORY_FENCE;

        // as the aborted version has already been placed in the version chain,
        // we need to unlink it by resetting the item pointers.
        auto old_prev =
            new_tile_group_header->GetPrevItemPointer(new_version.offset);

        // check whether the previous version exists.
        if (old_prev.IsNull() == true) {
          PL_ASSERT(tile_group_header->GetEndCommitId(tuple_slot) == MAX_CID);
          // if we updated the latest version.
          // We must first adjust the head pointer
          // before we unlink the aborted version from version list
          ItemPointer *index_entry_ptr =
              tile_group_header->GetIndirection(tuple_slot);
          UNUSED_ATTRIBUTE auto res = AtomicUpdateItemPointer(
              index_entry_ptr, ItemPointer(tile_group_id, tuple_slot));
          PL_ASSERT(res == true);
        }
        //////////////////////////////////////////////////

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);
//        LOG_DEBUG("Txn %lu free %u", current_txn->GetTransactionId(),
//                 tuple_slot);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

//        InitTupleReserved(t_cstamp, new_version.block, new_version.offset);

        gc_set->operator[](tile_group_id)[tuple_slot] = false;

      } else if (tuple_entry.second == RWType::DELETE) {
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);
        ItemPointer new_version =
            tile_group_header->GetNextItemPointer(tuple_slot);
        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset, MAX_CID);
        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

        COMPILER_MEMORY_FENCE;

        // as the aborted version has already been placed in the version chain,
        // we need to unlink it by resetting the item pointers.
        auto old_prev =
            new_tile_group_header->GetPrevItemPointer(new_version.offset);

        // check whether the previous version exists.
        if (old_prev.IsNull() == true) {
          // if we updated the latest version.
          // We must first adjust the head pointer
          // before we unlink the aborted version from version list
          ItemPointer *index_entry_ptr =
              tile_group_header->GetIndirection(tuple_slot);
          UNUSED_ATTRIBUTE auto res = AtomicUpdateItemPointer(
              index_entry_ptr, ItemPointer(tile_group_id, tuple_slot));
          PL_ASSERT(res == true);
        }
        //////////////////////////////////////////////////

        // we should set the version before releasing the lock.
        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INVALID_TXN_ID);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

//        InitTupleReserved(t_cstamp, new_version.block, new_version.offset);

        // add to gc set.
        // we need to recycle both old and new versions.
        // we require the GC to delete tuple from index only once.
        // recycle old version, delete from index
        gc_set->operator[](tile_group_id)[tuple_slot] = true;
        // recycle new version (which is an empty version), do not delete from index
        gc_set->operator[](new_version.block)[new_version.offset] = false;

      } else if (tuple_entry.second == RWType::INSERT) {
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);
      } else if (tuple_entry.second == RWType::INS_DEL) {
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);

        // add to gc set.
        gc_set->operator[](tile_group_id)[tuple_slot] = true;
      }
    }
  }


//  RemoveSsnReader(current_txn);

  if(current_ssn_txn_ctx->transaction_->GetEndCommitId() == MAX_CID) {
    current_ssn_txn_ctx->transaction_->SetEndCommitId(GetNextCommitId());
  }
  end_txn_table_[current_ssn_txn_ctx->transaction_->GetEndCommitId()] = current_ssn_txn_ctx;

  // delete current_ssn_txn_ctx;
  // delete current_txn;
  EndTransaction(current_ssn_txn_ctx->transaction_);

//  LOG_DEBUG("Aborting peloton txn finished: %lu ", current_txn->GetTransactionId());
  return ResultType::ABORTED;
}

void SsnTxnManager::RemoveSsnReader(Transaction *txn) {
//  LOG_DEBUG("release SILock");

  // Remove from the read list of accessed tuples
  auto &rw_set = txn->GetReadWriteSet();

  for (auto &tile_group_entry : rw_set) {
    oid_t tile_group_id = tile_group_entry.first;
    auto &manager = catalog::Manager::GetInstance();
    auto tile_group = manager.GetTileGroup(tile_group_id);
    if (tile_group == nullptr) continue;

    auto tile_group_header = tile_group->GetHeader();
    for (auto &tuple_entry : tile_group_entry.second) {
      auto tuple_slot = tuple_entry.first;

      // we don't have reader lock on insert
      if (tuple_entry.second == RWType::INSERT ||
          tuple_entry.second == RWType::INS_DEL) {
        continue;
      }
      RemoveSsnReader(tile_group_header, tuple_slot, txn->GetTransactionId());
    }
  }
//  LOG_DEBUG("release SILock finish");
}

}  // End storage namespace
}  // End peloton namespace
