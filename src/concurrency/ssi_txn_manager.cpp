//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ssi_txn_manager.cpp
//
// Identification: src/backend/concurrency/ssi_txn_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//#include "ssi_txn_manager.h"
//
//#include "backend/common/platform.h"
//#include "backend/logging/log_manager.h"
//#include "backend/logging/records/transaction_record.h"
//#include "backend/concurrency/transaction.h"
//#include "backend/catalog/manager.h"
//#include "backend/common/exception.h"
//#include "backend/common/logger.h"

#include "concurrency/ssi_txn_manager.h"
#include "catalog/manager.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/platform.h"
#include "concurrency/transaction.h"
#include "gc/gc_manager_factory.h"
#include "logging/log_manager.h"
#include "logging/records/transaction_record.h"

#include <set>
namespace peloton {
namespace concurrency {

thread_local SsiTxnContext *current_ssi_txn_ctx;

SsiTxnManager &SsiTxnManager::GetInstance() {
  static SsiTxnManager txn_manager;
  return txn_manager;
}

Transaction *SsiTxnManager::BeginTransaction(const size_t thread_id) {

//  auto &log_manager = logging::LogManager::GetInstance();
//  log_manager.PrepareLogging();

  // protect beginTransaction with a global lock
  // to ensure that:
  //    txn_id_a > txn_id_b --> begin_cid_a > begin_cid_b
  txn_id_t txn_id = GetNextTransactionId();
  cid_t begin_cid = GetNextCommitId();
  Transaction *txn = new Transaction(txn_id, begin_cid, thread_id);

  current_ssi_txn_ctx = new SsiTxnContext(txn);

  auto eid = EpochManagerFactory::GetInstance().EnterEpoch(thread_id);
  txn->SetEpochId(eid);


  txn_table_[txn->GetTransactionId()] = current_ssi_txn_ctx;
  // txn_manager_mutex_.Unlock();
//  LOG_DEBUG("Begin txn %lu", txn->GetTransactionId());


  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()
        ->GetTxnLatencyMetric()
        .StartTimer();
  }

  return txn;
}

Transaction *SsiTxnManager::BeginReadonlyTransaction(const size_t thread_id) {
  Transaction *txn = nullptr;
  txn_id_t txn_id = GetNextTransactionId();
  cid_t begin_cid = GetNextCommitId();
  txn = new Transaction(txn_id, begin_cid, thread_id, true);
  current_ssi_txn_ctx = new SsiTxnContext(txn);

  auto eid = EpochManagerFactory::GetInstance().EnterEpochRO(thread_id);
  txn->SetEpochId(eid);

  txn_table_[txn->GetTransactionId()] = current_ssi_txn_ctx;

  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()
        ->GetTxnLatencyMetric()
        .StartTimer();
  }
  return txn;
}

void SsiTxnManager::EndTransaction(Transaction *current_txn) {

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

  RemoveReader(current_txn);

  delete current_txn;
  current_txn = nullptr;

  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()
        ->GetTxnLatencyMetric()
        .RecordLatency();
  }
}

void SsiTxnManager::EndReadonlyTransaction(
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
VisibilityType SsiTxnManager::IsVisible(Transaction *const current_txn,
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
bool SsiTxnManager::IsOwner(Transaction *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,
    const oid_t &tuple_id) {
  auto tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  return tuple_txn_id == current_txn->GetTransactionId();
}

// if the tuple is not owned by any transaction and is visible to current
// transaction. will only be performed by deletes and updates.
bool SsiTxnManager::IsOwnable(UNUSED_ATTRIBUTE Transaction *const current_txn,
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
bool SsiTxnManager::AcquireOwnership(Transaction *const current_txn,
    const storage::TileGroupHeader *const tile_group_header,const oid_t &tuple_id) {
  auto txn_id = current_txn->GetTransactionId();
//  LOG_DEBUG("AcquireOwnership %lu", txn_id);

  // jump to abort directly
  if(current_ssi_txn_ctx->is_abort()){
    assert(current_ssi_txn_ctx->is_abort_ == false);
    LOG_DEBUG("detect conflicts");
    return false;
  }

  if (tile_group_header->SetAtomicTransactionId(tuple_id, txn_id) == false) {
//    LOG_DEBUG("Fail to insert new tuple. Set txn failure.");
    return false;
  }

  {
    GetReadLock(tile_group_header, tuple_id);
    ReadList *header = GetReaderList(tile_group_header, tuple_id);

    bool should_abort = false;
    while (header != nullptr) {
      // For all owner of siread lock on this version
      auto owner_ctx = header->txn_ctx;

      // Lock the transaction context
      owner_ctx->lock_.Lock();

      // Myself || owner is (or should be) aborted
      // skip
      if (owner_ctx == current_ssi_txn_ctx || owner_ctx->is_abort()) {
        header = header->next;

        // Unlock the transaction context
        owner_ctx->lock_.Unlock();
        continue;
      }

      auto end_cid = owner_ctx->transaction_->GetEndCommitId();

      // Owner is running, then SIread lock owner has an out edge to me
      if (end_cid == MAX_CID) {
        SetInConflict(current_ssi_txn_ctx);
        SetOutConflict(owner_ctx);
//        LOG_DEBUG("set %ld in, set %ld out", txn_id,
//                 owner_ctx->transaction_->GetTransactionId());
      } else {
        // Owner has commited and ownner commit after I start, then I must abort
        // Owner and I has read the same tuple slot
        // Owner has write the tuple slot
        if (end_cid > current_txn->GetBeginCommitId() &&
            GetInConflict(owner_ctx) && !owner_ctx->is_abort()) {
          should_abort = true;
//          LOG_DEBUG("abort in acquire");

          // Unlock the transaction context
          owner_ctx->lock_.Unlock();
          break;
        }
      }

      header = header->next;

      // Unlock the transaction context
      owner_ctx->lock_.Unlock();
    }
    ReleaseReadLock(tile_group_header, tuple_id);

    if (should_abort) return false;
  }

  return true;
}
//index check when insert tuple
bool SsiTxnManager::IsOccupied(Transaction *const current_txn, const void *position_ptr){
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
bool SsiTxnManager::IsWritten(UNUSED_ATTRIBUTE Transaction *const current_txn,
                              UNUSED_ATTRIBUTE const storage::TileGroupHeader *const tile_group_header,
                              UNUSED_ATTRIBUTE const oid_t &tuple_id){
  return  true;
}
void SsiTxnManager::YieldOwnership(UNUSED_ATTRIBUTE Transaction *const current_txn,
                                   UNUSED_ATTRIBUTE const oid_t &tile_group_id,
                                   UNUSED_ATTRIBUTE const oid_t &tuple_id){

}

bool SsiTxnManager::PerformRead(Transaction *const current_txn,
                                const ItemPointer &location,
                                UNUSED_ATTRIBUTE bool acquire_ownership ){
  auto tile_group_id = location.block;
  auto tuple_id = location.offset;
//   LOG_DEBUG("Perform Read %u %u", tile_group_id, tuple_id);
//  LOG_DEBUG("Perform Read acquire_ownership %d ", acquire_ownership);

  // jump to abort directly
  if(current_ssi_txn_ctx->is_abort()){
    assert(current_ssi_txn_ctx->is_abort_ == false);
    LOG_DEBUG("detect conflicts");
    return false;
  }

  auto tile_group = catalog::Manager::GetInstance().GetTileGroup(tile_group_id);
  auto tile_group_header = tile_group->GetHeader();

  auto txn_id = current_txn->GetTransactionId();

  auto &rw_set = current_txn->GetReadWriteSet();
  if (rw_set.count(tile_group_id) == 0 ||
      rw_set.at(tile_group_id).count(tuple_id) == 0) {
//    LOG_DEBUG("Not read before");
    // Previously, this tuple hasn't been read, add the txn to the reader list
    // of the tuple
    AddSIReader(tile_group.get(), tuple_id);

    auto writer = tile_group_header->GetTransactionId(tuple_id);
    // Another transaction is writting this tuple, add an edge
    if (writer != INVALID_TXN_ID && writer != INITIAL_TXN_ID &&
        writer != txn_id) {

      SsiTxnContext *writer_ptr = nullptr;
      if (txn_table_.find(writer, writer_ptr) && !writer_ptr->is_abort()) {
        // The writer have not been removed from the txn table
//        LOG_DEBUG("Writer %lu has no entry in txn table when read %u", writer, tuple_id);
        SetInConflict(writer_ptr);
        SetOutConflict(current_ssi_txn_ctx);
      }

    }
  }

  // existing SI code
  current_txn->RecordRead(location);

  // For each new version of the tuple
  {
    // read only section
    // read-lock
    // This is a potential big overhead for read operations
    // txn_manager_mutex_.ReadLock();

//    LOG_DEBUG("SI read phase 2");

    ItemPointer next_item = tile_group_header->GetNextItemPointer(tuple_id);
    while (!next_item.IsNull()) {
      auto tile_group =
          catalog::Manager::GetInstance().GetTileGroup(next_item.block);
      auto creator = GetCreatorTxnId(tile_group.get(), next_item.offset);

//      LOG_DEBUG("%u %u creator is %lu", next_item.block, next_item.offset,
//               creator);

      // Check creator status, skip if creator has commited before I start
      // or self is creator
      auto should_skip = false;
      SsiTxnContext *creator_ptr = nullptr;

      if (!txn_table_.find(creator, creator_ptr))
        should_skip = true;
      else {
        if (creator == txn_id)
          should_skip = true;
        else {
          auto ctx = creator_ptr;
          if (ctx->transaction_->GetEndCommitId() != INVALID_TXN_ID &&
              ctx->transaction_->GetEndCommitId() <
                  current_txn->GetBeginCommitId()) {
            should_skip = true;
          }
        }
      }

      if (should_skip) {
        next_item = tile_group->GetHeader()->GetNextItemPointer(next_item.offset);
        continue;
      }

      //creator is the perform insert/update/delete
      auto creator_ctx = creator_ptr;
      // Lock the transaction context
      creator_ctx->lock_.Lock();

      if (!creator_ctx->is_abort()) {
        // If creator committed and has out_confict, since creator has commited,
        // I must abort
        if (creator_ctx->transaction_->GetEndCommitId() != INVALID_TXN_ID &&
            creator_ctx->out_conflict_) {
          LOG_DEBUG("abort in read");
          // Unlock the transaction context
          creator_ctx->lock_.Unlock();
          // txn_manager_mutex_.Unlock();
          return false;
        }
        // Creator not commited, add an edge
        SetInConflict(creator_ctx);
        SetOutConflict(current_ssi_txn_ctx);
      }

      // Unlock the transaction context
      creator_ctx->lock_.Unlock();

      next_item = tile_group->GetHeader()->GetNextItemPointer(next_item.offset);
    }
//    LOG_DEBUG("SI read phase 2 finished");
    // txn_manager_mutex_.Unlock();
  }

  return true;
}

void SsiTxnManager::PerformInsert(Transaction *const current_txn,
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
  // Init the creator of this tuple
  InitTupleReserved(current_txn->GetTransactionId(), tile_group_id, tuple_id);

  // Write down the head pointer's address in tile group header
  tile_group_header->SetIndirection(tuple_id, index_entry_ptr);
//  return true;
}

void SsiTxnManager::PerformUpdate(Transaction *const current_txn,
                                  const ItemPointer &old_location,
                                  const ItemPointer &new_location) {
  auto transaction_id = current_txn->GetTransactionId();

  auto tile_group_header = catalog::Manager::GetInstance()
                               .GetTileGroup(old_location.block)
                               ->GetHeader();
  auto new_tile_group_header = catalog::Manager::GetInstance()
                                   .GetTileGroup(new_location.block)
                                   ->GetHeader();

  // if we can perform update, then we must already locked the older version.
  assert(tile_group_header->GetTransactionId(old_location.offset) == transaction_id);
  // Set double linked list
  tile_group_header->SetNextItemPointer(old_location.offset, new_location);
  new_tile_group_header->SetPrevItemPointer(new_location.offset, old_location);

  new_tile_group_header->SetTransactionId(new_location.offset, transaction_id);
  new_tile_group_header->SetBeginCommitId(new_location.offset, MAX_CID);
  new_tile_group_header->SetEndCommitId(new_location.offset, MAX_CID);

  auto old_prev = tile_group_header->GetPrevItemPointer(old_location.offset);

  if (old_prev.IsNull() == false) {
    auto old_prev_tile_group_header = catalog::Manager::GetInstance()
        .GetTileGroup(old_prev.block)
        ->GetHeader();

    // once everything is set, we can allow traversing the new version.
    old_prev_tile_group_header->SetNextItemPointer(old_prev.offset,
                                                   new_location);
  }

  InitTupleReserved(transaction_id, new_location.block, new_location.offset);

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
  }

  current_txn->RecordUpdate(old_location);
//  return true;
}

void SsiTxnManager::PerformUpdate(Transaction *const current_txn,
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

void SsiTxnManager::PerformDelete(Transaction *const current_txn,
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

  // Add the old tuple into the delete set
  current_txn->RecordDelete(old_location);
  InitTupleReserved(transaction_id, new_location.block, new_location.offset);

  auto old_prev = tile_group_header->GetPrevItemPointer(old_location.offset);

  if (old_prev.IsNull() == false) {
    auto old_prev_tile_group_header = catalog::Manager::GetInstance()
        .GetTileGroup(old_prev.block)
        ->GetHeader();

    old_prev_tile_group_header->SetNextItemPointer(old_prev.offset,
                                                   new_location);
  }
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
  }
//  return true;
}

void SsiTxnManager::PerformDelete(Transaction *const current_txn,
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

ResultType SsiTxnManager::CommitTransaction(Transaction *const current_txn) {
  LOG_TRACE("Committing peloton txn : %lu ", current_txn->GetTransactionId());

  if (current_txn->IsDeclaredReadOnly() == true) {
    EndReadonlyTransaction(current_txn);
    return ResultType::SUCCESS;
  }

  auto &manager = catalog::Manager::GetInstance();
  auto &rw_set = current_txn->GetReadWriteSet();
  auto gc_set = current_txn->GetGCSetPtr();
  cid_t end_commit_id = GetNextCommitId();
  ResultType ret;

  bool should_abort = false;
  {
    current_ssi_txn_ctx->lock_.Lock();
    //if T1->T2 and T2->T1, then abort
    if (GetInConflict(current_ssi_txn_ctx) &&
        GetOutConflict(current_ssi_txn_ctx)) {
      should_abort = true;
      current_ssi_txn_ctx->is_abort_ = true;
    }

    // generate transaction id.
    ret = current_txn->GetResult();

    current_txn->SetEndCommitId(end_commit_id);

    if (ret != ResultType::SUCCESS) {
      LOG_DEBUG("Wierd, result is not success but go into commit state");
    }
    current_ssi_txn_ctx->lock_.Unlock();
  }

  if (should_abort) {
    LOG_DEBUG("Abort because RW conflict");
    return AbortTransaction(current_txn);
  }

  //////////////////////////////////////////////////////////

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
        // we must guarantee that, at any time point, only one version is
        // visible.
        // we do not change begin cid for old tuple.
        tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);
        ItemPointer new_version =
            tile_group_header->GetNextItemPointer(tuple_slot);
        ItemPointer old_version(tile_group_id, tuple_slot);
//        log_manager.LogUpdate(end_commit_id, old_version, new_version);

        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset,
                                                end_commit_id);
        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

        COMPILER_MEMORY_FENCE;

        new_tile_group_header->SetTransactionId(new_version.offset,
                                                INITIAL_TXN_ID);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

        // add to gc set.
        gc_set->operator[](tile_group_id)[tuple_slot] = false;

      } else if (tuple_entry.second == RWType::DELETE) {
        // we do not change begin cid for old tuple.
        tile_group_header->SetEndCommitId(tuple_slot, end_commit_id);
        ItemPointer new_version =
            tile_group_header->GetNextItemPointer(tuple_slot);
        ItemPointer delete_location(tile_group_id, tuple_slot);
//        log_manager.LogDelete(end_commit_id, delete_location);
        auto new_tile_group_header =
            manager.GetTileGroup(new_version.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(new_version.offset,
                                                end_commit_id);
        new_tile_group_header->SetEndCommitId(new_version.offset, MAX_CID);

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
        assert(tile_group_header->GetTransactionId(tuple_slot) ==
               current_txn->GetTransactionId());
        // set the begin commit id to persist insert
        ItemPointer insert_location(tile_group_id, tuple_slot);
//        log_manager.LogInsert(end_commit_id, insert_location);

        tile_group_header->SetBeginCommitId(tuple_slot, end_commit_id);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);
      } else if (tuple_entry.second == RWType::INS_DEL) {
        assert(tile_group_header->GetTransactionId(tuple_slot) ==
               current_txn->GetTransactionId());

        // set the begin commit id to persist insert
        tile_group_header->SetBeginCommitId(tuple_slot, MAX_CID);
        tile_group_header->SetEndCommitId(tuple_slot, MAX_CID);

        COMPILER_MEMORY_FENCE;

        tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);

        // add to gc set.
        gc_set->operator[](tile_group_id)[tuple_slot] = true;

      }
    }
  }

//  log_manager.LogCommitTransaction(end_commit_id);

  current_ssi_txn_ctx->is_finish_ = true;

  end_txn_table_[current_ssi_txn_ctx->transaction_->GetEndCommitId()] = current_ssi_txn_ctx;

  EndTransaction(current_ssi_txn_ctx->transaction_);
//  LOG_DEBUG("Committing peloton txn finished: %lu ", current_txn->GetTransactionId());

  return ret;
}

ResultType SsiTxnManager::AbortTransaction(Transaction *const current_txn) {
//  LOG_DEBUG("Aborting peloton txn : %lu ", current_txn->GetTransactionId());

  if (current_ssi_txn_ctx->is_abort_ == false) {
    // Set abort flag
    current_ssi_txn_ctx->lock_.Lock();
    current_ssi_txn_ctx->is_abort_ = true;
    current_ssi_txn_ctx->lock_.Unlock();
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


//  RemoveReader(current_txn);

  if(current_ssi_txn_ctx->transaction_->GetEndCommitId() == MAX_CID) {
    current_ssi_txn_ctx->transaction_->SetEndCommitId(GetNextCommitId());
  }
  end_txn_table_[current_ssi_txn_ctx->transaction_->GetEndCommitId()] = current_ssi_txn_ctx;

  // delete current_ssi_txn_ctx;
  // delete current_txn;
  EndTransaction(current_ssi_txn_ctx->transaction_);

//  LOG_DEBUG("Aborting peloton txn finished: %lu ", current_txn->GetTransactionId());
  return ResultType::ABORTED;
}

void SsiTxnManager::RemoveReader(Transaction *txn) {
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
      RemoveSIReader(tile_group_header, tuple_slot, txn->GetTransactionId());
    }
  }
//  LOG_DEBUG("release SILock finish");
}
//
//// Clean obsolete txn record
//// Current implementation might be very expensive, consider using dependency
//// count
//void SsiTxnManager::CleanUp() {
//  LOG_DEBUG("release CleanUp start");
//  if(!stopped) {
//    stopped = true;
//    vacuum.join();
//  }
//
//  std::unordered_set <cid_t> gc_cids;
//  {
//    auto iter = end_txn_table_.lock_table();
//
//    for (auto &it : iter) {
//      auto ctx_ptr = it.second;
//      txn_table_.erase(ctx_ptr->transaction_->GetTransactionId());
//      gc_cids.insert(it.first);
//
//      if (!ctx_ptr->is_abort()) {
//        RemoveReader(ctx_ptr->transaction_);
//      }
//      delete ctx_ptr->transaction_;
//      delete ctx_ptr;
//      gc_cid = std::max(gc_cid, it.first);
//    }
//  }
//
//  for(auto cid : gc_cids) {
//    end_txn_table_.erase(cid);
//  }
//  LOG_INFO("release CleanUp finish");
//}
//
//void SsiTxnManager::CleanUpBg() {
//  while(!stopped) {
//    std::this_thread::sleep_for(std::chrono::milliseconds(EPOCH_LENGTH));
//    auto max_begin = GetMaxCommittedCid();
//    std::unordered_set<cid_t> gc_cids;
//    while (gc_cid < max_begin) {
//      SsiTxnContext *ctx_ptr = nullptr;
//      if (!end_txn_table_.find(gc_cid, ctx_ptr)) {
//        gc_cid++;
//        continue;
//      }
//
//      // find garbage
//      gc_cids.insert(gc_cid);
//      txn_table_.erase(ctx_ptr->transaction_->GetTransactionId());
//
//      gc_cids.insert(gc_cid);
//
//      if(!ctx_ptr->is_abort()) {
//        RemoveReader(ctx_ptr->transaction_);
//      }
//      delete ctx_ptr->transaction_;
//      delete ctx_ptr;
//      gc_cid++;
//    }
//
//    for(auto cid : gc_cids) {
//      end_txn_table_.erase(cid);
//    }
//  }
//}

}  // End storage namespace
}  // End peloton namespace
