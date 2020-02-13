/**
 *    SPDX-License-Identifier: AGPL-3.0-only
 *
 *    Copyright (C) 2017-2020 Micron Technology, Inc.
 *
 *    This code is derived from and modifies the mongo-rocks project.
 *
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */
#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <memory>
#include <string>
#include <vector>

#include <boost/thread/mutex.hpp>

#include "mongo/db/storage/capped_callback.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/thread.h"
#include "mongo/util/timer.h"

#include "hse.h"
#include "hse_counter_manager.h"
#include "hse_durability_manager.h"
#include "hse_exceptions.h"
#include "hse_oplog_block.h"
#include "hse_recovery_unit.h"
#include "hse_util.h"

using hse::KVDB;
using hse::KVDBRecordStoreKey;
using hse::KVDBNotImplementedError;
using hse::Status;
using hse::CompParms;
using hse::DEFAULT_PFX_LEN;
using hse::OPLOG_PFX_LEN;
using mongo::StringData;
using mongo::BSONObj;
using mongo::BSONObjBuilder;

namespace collconf {

mongo::Status parseOneOption(string& left, string& right, CompParms& compparms);
mongo::Status collectionCfgString2compParms(const StringData& sd, struct CompParms& compparms);
mongo::Status validateCollectionOptions(const BSONObj& options, CompParms& compparms);
void compParms2Ident(BSONObjBuilder* configBuilder, CompParms& compparms);
mongo::Status ident2compParms(const BSONObj& config, CompParms& compparms);

}  // collconf end

namespace mongo {

typedef std::list<RecordId> SortedRecordIds;

//
// There are three classes that together implement the "record store" portion of the hse
// storage engine: KVDBRecordStore, KVDBCappedRecordStore, and KVDBOplogStore.  The
// KVDBCappedRecordStore is a public subclass of KVDBRecordStore and KVDBOplogStore is a public
// subclass of KVDBCappedRecordStore.
//
// The rationale for this decomposition is that the oplog is a very restricted case of a capped
// collection, one where we know a great deal about how it is written to, how it is deleted from,
// and how it is read. Earlier versions of this code pushed all of this implementation into a
// single class, KVDBRecordStore with embedded booleans _isCapped and _isOplog controlling the
// details. Such a structure is dubious on its face, but wholly unsuited to implementing an
// performance aggressive oplog - which is critical to performance in a replica set.
//

class KVDBRecordStore : public RecordStore {
    MONGO_DISALLOW_COPYING(KVDBRecordStore);

public:
    KVDBRecordStore(OperationContext* ctx,
                    StringData ns,
                    StringData id,
                    KVDB& db,
                    KVSHandle& colKvs,
                    KVSHandle& largeKvs,
                    uint32_t prefix,
                    KVDBDurabilityManager& durabilityManager,
                    KVDBCounterManager& counterManager,
                    struct CompParms& compparms);

    virtual ~KVDBRecordStore();

    // metadata methods
    virtual const char* name() const;

    virtual long long dataSize(OperationContext* txn) const;

    virtual long long numRecords(OperationContext* txn) const;

    virtual int64_t storageSize(OperationContext* txn,
                                BSONObjBuilder* extraInfo = NULL,
                                int infoLevel = 0) const;

    virtual bool isCapped() const {
        return false;
    }

    virtual bool isOplog() const {
        return false;
    }

    // CRUD related
    //
    virtual bool findRecord(OperationContext* txn, const RecordId& loc, RecordData* out) const;

    virtual void deleteRecord(OperationContext* txn, const RecordId& dl);

    virtual StatusWith<RecordId> insertRecord(OperationContext* txn,
                                              const char* data,
                                              int len,
                                              bool enforceQuota);

    virtual Status insertRecordsWithDocWriter(OperationContext* txn,
                                              const DocWriter* const* docs,
                                              size_t nDocs,
                                              RecordId* idsOut = nullptr);

    virtual Status updateRecord(OperationContext* txn,
                                const RecordId& oldLocation,
                                const char* data,
                                int len,
                                bool enforceQuota,
                                UpdateNotifier* notifier);

    virtual bool updateWithDamagesSupported() const;

    virtual StatusWith<RecordData> updateWithDamages(OperationContext* txn,
                                                     const RecordId& loc,
                                                     const RecordData& oldRec,
                                                     const char* damageSource,
                                                     const mutablebson::DamageVector& damages);

    virtual std::unique_ptr<SeekableRecordCursor> getCursor(OperationContext* txn,
                                                            bool forward = true) const;

    virtual void waitForAllEarlierOplogWritesToBeVisible(OperationContext* txn) const;

    // higher level

    virtual Status truncate(OperationContext* txn);

    virtual void temp_cappedTruncateAfter(OperationContext* txn, RecordId end, bool inclusive);

    virtual Status validate(OperationContext* txn,
                            ValidateCmdLevel level,
                            ValidateAdaptor* adaptor,
                            ValidateResults* results,
                            BSONObjBuilder* output);

    virtual void appendCustomStats(OperationContext* txn,
                                   BSONObjBuilder* result,
                                   double scale) const;

    virtual void updateStatsAfterRepair(OperationContext* txn,
                                        long long numRecords,
                                        long long dataSize);

    virtual std::string getIdent() {
        return _ident;
    }


    virtual bool compactSupported() const override {
        return true;
    }

    /**
     * Does compact() leave RecordIds alone or can they change.
     */
    virtual bool compactsInPlace() const override {
        return true;
    }

    /**
     * Attempt to reduce the storage space used by this RecordStore.
     */
    virtual Status compact(OperationContext* txn,
                           RecordStoreCompactAdaptor* adaptor,
                           const CompactOptions* options,
                           CompactStats* stats) override {
        return Status::OK();
    }

    virtual const struct CompParms* getCompParms(void) const {
        return &(this->_compparms);
    }

    virtual void setCompParms(CompParms& compparms) {
        _compparms = compparms;
    }

    void updateCounters();  // write counters to kvdb
    void loadCounters();    // read counters from kvdb

    void overTake() {
        _overTaken = true;
    }


protected:
    bool _baseFindRecord(OperationContext* opctx,
                         struct KVDBRecordStoreKey* key,
                         const RecordId& loc,
                         RecordData* out) const;

    void _baseDeleteRecord(OperationContext* opctx,
                           struct KVDBRecordStoreKey* key,
                           const RecordId& dl);

    StatusWith<RecordId> _baseInsertRecord(OperationContext* opctx,
                                           struct KVDBRecordStoreKey* key,
                                           RecordId loc,
                                           const char* data,
                                           int len);

    hse::Status _baseUpdateRecord(OperationContext* opctx,
                                  struct KVDBRecordStoreKey* key,
                                  const RecordId& loc,
                                  const char* data,
                                  int len,
                                  bool noLenChange,
                                  bool* lenChangeFailure);

    void _changeNumRecords(OperationContext* txn, int64_t amount);
    void _increaseDataStorageSizes(OperationContext* txn, int64_t damount, int64_t samount);
    void _resetNumRecords(OperationContext* txn);
    void _resetDataStorageSizes(OperationContext* txn);

    hse::Status _putKey(OperationContext* txn,
                        struct KVDBRecordStoreKey* key,
                        const RecordId& loc,
                        const char* data,
                        const int len,
                        unsigned int* num_chunks,
                        int* len_comp);

    virtual RecordId _getLastId();

    RecordId _nextId();

    virtual void _setPrefix(KVDBRecordStoreKey* key, const RecordId& loc) const {
        KRSK_SET_PREFIX(*key, KRSK_RS_PREFIX(_prefixVal));
    }

    virtual int _getPrefixLen() {
        return DEFAULT_PFX_LEN;
    }

    virtual Status _cappedDeleteCallbackHelper(OperationContext* txn,
                                               KVDBData& oldValue,
                                               RecordId& newestOld) {
        return Status::OK();
    }

    KVDB& _db;
    KVSHandle& _colKvs;
    KVSHandle& _largeKvs;
    uint32_t _prefixVal;
    uint32_t _prefixValBE;
    KVDBDurabilityManager& _durabilityManager;
    KVDBCounterManager& _counterManager;  // not owned

    std::string _ident;
    AtomicInt64 _nextIdNum;
    std::atomic<long long> _dataSize;
    std::atomic<long long> _storageSize{0};
    std::atomic<long long> _numRecords;
    std::atomic<unsigned long long> _uncompressed_bytes{0};
    std::atomic<unsigned long long> _compressed_bytes{0};

    const std::string _dataSizeKey;
    const std::string _storageSizeKey;
    const std::string _numRecordsKey;

    void _encodeAndWriteCounter(const std::string& keyString, std::atomic<long long>& counter);
    void _readAndDecodeCounter(const std::string& keyString, std::atomic<long long>& counter);

    bool _shuttingDown{false};
    bool _hasBackgroundThread;

    // _overTaken is always false except when mongo renames the collection.
    // When mongo renames the collection, it creates a second RecordStore instance
    // on the same collection. For a short time there are two RecordStore
    // instances for the collection.
    // When the second instance is created, it overtakes the first instance (it does that in
    // its creator).
    // That means that the first instance stops managing things like
    // the collection counters or the lastId (for record ids).
    // The second instance becomes responsible for managing these things.
    // This is to avoid having both instances managing these things in parallel.
    // Managing them in parallel introduces inconsistencies.
    // When the second instance overtakes the first instance, it sets _overTaken to true in
    // in the first instance.
    // The first instance (overtaken now) will be destroyed by mongo a short time after the
    // second instance has been created.
    // During the time the two instances are present, the collection is idle, mongo does
    // not insert records.
    // _overTaken doesn't need to be atomic because mongo destroy the original recordstore
    // only after the new record store is visible to the connector.
    bool _overTaken{false};

    struct CompParms _compparms;
};


class KVDBCappedVisibilityManager;
class KVDBCappedInsertChange;

class KVDBCappedRecordStore : public KVDBRecordStore {
    MONGO_DISALLOW_COPYING(KVDBCappedRecordStore);

public:
    KVDBCappedRecordStore(OperationContext* ctx,
                          StringData ns,
                          StringData id,
                          KVDB& db,
                          KVSHandle& colKvs,
                          KVSHandle& largeKvs,
                          uint32_t prefix,
                          KVDBDurabilityManager& durabilityManager,
                          KVDBCounterManager& counterManager,
                          int64_t cappedMaxSize,
                          int64_t cappedMaxDocs,
                          struct CompParms& compparms);

    virtual ~KVDBCappedRecordStore();

    /* virtual */ StatusWith<RecordId> insertRecord(OperationContext* opctx,
                                                    const char* data,
                                                    int len,
                                                    bool enforceQuota);

    /* virtual */ Status updateRecord(OperationContext* txn,
                                      const RecordId& oldLocation,
                                      const char* data,
                                      int len,
                                      bool enforceQuota,
                                      UpdateNotifier* notifier);

    // metadata methods

    /* virtual */
    bool isCapped() const {
        return true;
    }

    /* virtual */
    bool isOplog() const {
        return false;
    }

    // higher level

    /* virtual */
    std::unique_ptr<SeekableRecordCursor> getCursor(OperationContext* txn,
                                                    bool forward = true) const;
    /* virtual */
    void temp_cappedTruncateAfter(OperationContext* txn, RecordId end, bool inclusive);

    /* virtual */
    void appendCustomStats(OperationContext* txn, BSONObjBuilder* result, double scale) const;

    virtual void setCappedCallback(CappedCallback* cb);

    friend KVDBCappedVisibilityManager;
    friend KVDBCappedInsertChange;

protected:
    virtual Status cappedDeleteAsNeeded(OperationContext* txn,
                                        const RecordId& justInserted,
                                        int64_t* removed);

    virtual Status _baseCappedDeleteAsNeeded(OperationContext* opctx,
                                             const RecordId& justInserted,
                                             int64_t* removed);

    bool _needDelete(long long dataSizeDelta, long long numRecordsDelta) const;

    virtual Status _cappedDeleteCallbackHelper(OperationContext* txn,
                                               KVDBData& oldValue,
                                               RecordId& newestOld) override;

    void _cappedTruncateAfter(OperationContext* txn, RecordId end, bool inclusive);

    const int64_t _cappedMaxSize;
    const int64_t _cappedMaxSizeSlack;  // when to start applying backpressure
    const int64_t _cappedMaxDocs;
    CappedCallback* _cappedCallback{nullptr};
    stdx::mutex _cappedCallbackMutex;  // guards _cappedCallback.

    RecordId _cappedOldestKeyHint{0};
    unique_ptr<KVDBCappedVisibilityManager> _cappedVisMgr;


    std::string _ident;
    AtomicInt64 _nextIdNum;
};

class KVDBOplogStore : public KVDBCappedRecordStore {
    MONGO_DISALLOW_COPYING(KVDBOplogStore);

public:
    KVDBOplogStore(OperationContext* ctx,
                   StringData ns,
                   StringData id,
                   KVDB& db,
                   KVSHandle& colKvs,
                   KVSHandle& largeKvs,
                   uint32_t prefix,
                   KVDBDurabilityManager& durabilityManager,
                   KVDBCounterManager& counterManager,
                   int64_t cappedMaxSize,
                   struct CompParms& compparms);

    virtual ~KVDBOplogStore();

    /* virtual */
    bool findRecord(OperationContext* txn, const RecordId& loc, RecordData* out) const;

    /* virtual */
    void deleteRecord(OperationContext* opctx, const RecordId& dl);

    /* virtual */
    StatusWith<RecordId> insertRecord(OperationContext* opctx,
                                      const char* data,
                                      int len,
                                      bool enforceQuota);

    /* virtual */
    Status updateRecord(OperationContext* opctx,
                        const RecordId& oldLocation,
                        const char* data,
                        int len,
                        bool enforceQuota,
                        UpdateNotifier* notifier);


    // higher level

    /* virtual */
    std::unique_ptr<SeekableRecordCursor> getCursor(OperationContext* txn,
                                                    bool forward = true) const;


    /* virtual */
    void waitForAllEarlierOplogWritesToBeVisible(OperationContext* txn) const;

    Status truncate(OperationContext* opctx);

    void temp_cappedTruncateAfter(OperationContext* opctx, RecordId end, bool inclusive);

    virtual Status oplogDiskLocRegister(OperationContext* txn, const Timestamp& opTime);

    virtual boost::optional<RecordId> oplogStartHack(OperationContext* txn,
                                                     const RecordId& startingPosition) const;

    /* virtual */
    Status cappedDeleteAsNeeded(OperationContext* txn,
                                const RecordId& justInserted,
                                int64_t* removed);

    // Returns false if the oplog was dropped while waiting for a deletion request.
    bool yieldAndAwaitOplogDeletionRequest(OperationContext* txn);

    void reclaimOplog(OperationContext* txn);

    // for testing
    KVDBOplogBlockManager* getOpBlkMgr();

    /* virtual */ bool isOplog() const {
        return true;
    }

    friend KVDBCappedVisibilityManager;
    friend KVDBCappedInsertChange;

private:
    /* virtual */
    void _setPrefix(KVDBRecordStoreKey* key, const RecordId& loc) const {
        if (_opBlkMgr) {
            KRSK_SET_PREFIX(*key, KRSK_OL_PREFIX(_prefixVal, _opBlkMgr->getBlockId(loc)));
        } else {
            KRSK_SET_PREFIX(*key, KRSK_RS_PREFIX(_prefixVal));
        }
    }

    /* virtual */
    int _getPrefixLen() {
        if (_opBlkMgr) {
            return OPLOG_PFX_LEN;
        } else {
            return DEFAULT_PFX_LEN;
        }
    }

    void _oplogTruncateAfter(OperationContext* txn, RecordId end, bool inclusive);

    shared_ptr<KVDBOplogBlockManager> _opBlkMgr{};
};

class KVDBCappedInsertChange : public RecoveryUnit::Change {
public:
    KVDBCappedInsertChange(KVDBCappedRecordStore& rs,
                           KVDBCappedVisibilityManager& cappedVisibilityManager,
                           SortedRecordIds::iterator it);
    virtual void commit();
    virtual void rollback();

private:
    KVDBCappedRecordStore& _crs;
    KVDBCappedVisibilityManager& _cappedVisMgr;
    const SortedRecordIds::iterator _it;
};

class KVDBRecordStoreCursor : public SeekableRecordCursor {
public:
    KVDBRecordStoreCursor(OperationContext* opctx,
                          KVDB& db,
                          KVSHandle& colKvs,
                          KVSHandle& largeKvs,
                          uint32_t prefix,
                          bool forward,
                          const struct CompParms& compparms);

    virtual ~KVDBRecordStoreCursor();

    boost::optional<Record> next() final;

    virtual boost::optional<Record> seekExact(const RecordId& id);

    void save() final;

    void saveUnpositioned() final;

    virtual bool restore();

    void detachFromOperationContext() final;

    void reattachToOperationContext(OperationContext* txn) final;

    virtual void _setPrefix(KVDBRecordStoreKey* key, const RecordId& loc) const {
        KRSK_SET_PREFIX(*key, KRSK_RS_PREFIX(_prefixVal));
    };

protected:
    virtual void _reallySeek(const RecordId& id);

    virtual hse::Status _currCursorRead(
        KVDBRecoveryUnit* ru, KvsCursor* cursor, KVDBData& elKey, KVDBData& elVal, bool& eof);

    virtual bool _currIsHidden(const RecordId& loc);

    virtual boost::optional<Record> _curr();

    virtual KvsCursor* _getMCursor();

    virtual void _destroyMCursor();

    OperationContext* _opctx;
    KVDB& _db;
    KVSHandle& _colKvs;
    KVSHandle& _largeKvs;
    uint32_t _prefixVal;
    uint32_t _prefixValBE;
    bool _forward;
    KvsCursor* _mCursor;

    bool _cursorValid = false;
    bool _eof = false;
    bool _needSeek = false;
    bool _needUpdate = false;
    long _updates = 0;

    KVDBData _seekVal{};
    KVDBData _largeVal{};
    RecordId _lastPos{};

    struct CompParms _compparms;
};

class KVDBCappedRecordStoreCursor : public KVDBRecordStoreCursor {
public:
    KVDBCappedRecordStoreCursor(OperationContext* txn,
                                KVDB& db,
                                KVSHandle& colKvs,
                                KVSHandle& largeKvs,
                                uint32_t prefix,
                                bool forward,
                                KVDBCappedVisibilityManager& cappedVisMgr,
                                const struct CompParms& compparms);

    virtual ~KVDBCappedRecordStoreCursor();

    // virtual
    bool restore();

    virtual void _setPrefix(KVDBRecordStoreKey* key, const RecordId& loc) const {
        KRSK_SET_PREFIX(*key, KRSK_RS_PREFIX(_prefixVal));
    };

protected:
    // virtual
    hse::Status _currCursorRead(
        KVDBRecoveryUnit* ru, KvsCursor* cursor, KVDBData& elKey, KVDBData& elVal, bool& eof);

    // virtual
    bool _currIsHidden(const RecordId& loc);

    virtual void _packKey(struct KVDBRecordStoreKey* key, uint32_t prefix, const RecordId& loc);

    KVDBCappedVisibilityManager& _cappedVisMgr;
};

class KVDBOplogStoreCursor : public KVDBCappedRecordStoreCursor {
public:
    KVDBOplogStoreCursor(OperationContext* opctx,
                         KVDB& db,
                         KVSHandle& colKvs,
                         KVSHandle& largeKvs,
                         uint32_t prefix,
                         bool forward,
                         KVDBCappedVisibilityManager& cappedVisMgr,
                         shared_ptr<KVDBOplogBlockManager> opBlkMgr,
                         const struct CompParms& compparms);

    virtual ~KVDBOplogStoreCursor();

    // virtual
    boost::optional<Record> seekExact(const RecordId& id);

    // virtual
    void _reallySeek(const RecordId& id);

    // virtual
    KvsCursor* _getMCursor();

    // virtual
    void _setPrefix(KVDBRecordStoreKey* key, const RecordId& loc) const {
        if (_opBlkMgr) {
            KRSK_SET_PREFIX(*key, KRSK_OL_PREFIX(_prefixVal, _opBlkMgr->getBlockId(loc)));
        } else {
            KRSK_SET_PREFIX(*key, KRSK_RS_PREFIX(_prefixVal));
        }
    }

private:
    // virtual
    hse::Status _currCursorRead(
        KVDBRecoveryUnit* ru, KvsCursor* cursor, KVDBData& elKey, KVDBData& elVal, bool& eof);
    inline bool _needReadAhead();

    // virtual
    bool _currIsHidden(const RecordId& loc);

    void _updateReadUntil();

    // virtual
    void _packKey(struct KVDBRecordStoreKey* key, uint32_t prefix, const RecordId& loc);

    RecordId _readUntilForOplog;
    shared_ptr<KVDBOplogBlockManager> _opBlkMgr{};

    // Heuristic oplog read rate that determines whether read ahead is needed.
    const int64_t READ_AHEAD_THRESHOLD{100};
};

class KVDBCappedVisibilityManager {
public:
    KVDBCappedVisibilityManager(KVDBCappedRecordStore& rs,
                                KVDBDurabilityManager& durabilityManager);
    void dealtWithCappedRecord(SortedRecordIds::iterator it);
    void updateHighestSeen(const RecordId& record);
    void setHighestSeen(const RecordId& record);
    RecordId getHighestSeen();
    int64_t getCommitBoundary();
    int64_t getPersistBoundary();
    void addUncommittedRecord(OperationContext* opctx, const RecordId& record);

    // a bit hacky function, but does the job
    RecordId getNextAndAddUncommitted(OperationContext* opctx, std::function<RecordId()> nextId);

    bool isCappedHidden(const RecordId& record) const;

    void waitForAllOplogWritesToBeVisible(OperationContext* opctx) const;
    void durableCallback(int64_t newPersistBoundary);
    virtual ~KVDBCappedVisibilityManager();

private:
    void _addUncommittedRecord_inlock(OperationContext* opctx, const RecordId& record);

    // protects the state
    mutable stdx::mutex _uncommittedRecordIdsMutex;
    KVDBCappedRecordStore& _crs;
    KVDBDurabilityManager& _durabilityManager;
    SortedRecordIds _uncommittedRecords;
    RecordId _oplog_highestSeen;
    bool _shuttingDown{false};
    bool _durable;
    int64_t _forceLag;

    // All records < _commitBoundary have committed/aborted.
    // All records < _persistBoundary have been synced.
    // _persistBoundary <= _commitBoundary
    volatile int64_t _commitBoundary{1};
    volatile int64_t _persistBoundary{1};

    mutable stdx::condition_variable _opsBecameVisibleCV;
};
}
