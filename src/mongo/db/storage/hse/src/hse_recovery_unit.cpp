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
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "hse_recovery_unit.h"
#include "mongo/platform/basic.h"
#include "mongo/util/log.h"

#include "hse_util.h"

using hse::ClientTxn;
using hse::CompParms;
using hse::KVDB;
using hse::KVDBData;

using namespace std;

using hse::VALUE_META_SIZE;

namespace mongo {

std::atomic<unsigned long> KVDBCounterMapUniqID;

namespace {
    // SnapshotIds need to be globally unique, as they are used in a WorkingSetMember
    // to determine if documents changed.  nextSnapShotID is a very heavily updated
    // atomic and hence lives all alone in its own cache line.
    static union alignas(128) {
        AtomicUInt64 nextSnapshotId{1};
    };

    thread_local unique_ptr<uint8_t[]> tlsReadBuf{new uint8_t[HSE_KVS_VLEN_MAX]};
}

/* Start  KVDBRecoveryUnit */
KVDBRecoveryUnit::KVDBRecoveryUnit(KVDB& kvdb,
                                   KVDBCounterManager& counterManager,
                                   KVDBDurabilityManager& durabilityManager)
    : _kvdb(kvdb),
      _snapId(nextSnapshotId.fetchAndAdd(1)),
      _txn(nullptr),
      _txn_cached(nullptr),
      _counterManager(counterManager),
      _durabilityManager(durabilityManager) {}

KVDBRecoveryUnit::~KVDBRecoveryUnit() {
    if (_txn_cached) {
        _txn_cached->~ClientTxn();
    } else if (_txn) {
        _txn->~ClientTxn();
    }
}

void KVDBRecoveryUnit::beginUnitOfWork(OperationContext* opCtx) {
    // validate the recovery unit in the context
    invariantHse(opCtx->recoveryUnit() == this);
}

void KVDBRecoveryUnit::commitUnitOfWork() {
    if (_txn) {
        hse::Status st(_txn->commit());
        invariantHseSt(st);

        _txn_cached = _txn;
        _txn = nullptr;

        // TODO: Can we move this into _ensure_txn() or beginUnitOfWork() ???
        // If so it would roughly halve the contention on this global atomic...
        // Also, seems like it could be relaxed, no?
        _snapId = nextSnapshotId.fetchAndAdd(1);
    }

    // Sync the counters
    if (!_deltaCounters.empty()) {
        for (auto& pair : _deltaCounters) {
            auto& counter = pair.second;
            counter._value->fetch_add(counter._delta, memory_order::memory_order_relaxed);
        }

        _counterManager.syncPeriodic();
    }

    // commit all changes
    if (!_changes.empty()) {
        try {
            for (const auto& change : _changes) {
                change->commit();
            }
            _changes.clear();
        } catch (...) {
            invariantHse(false);  // abort
        }
    }

    // deactivate
    _deltaCounters.clear(); // Can we move this up into the _deltaCounters block?
}

void KVDBRecoveryUnit::abortUnitOfWork() {
    if (_txn) {
        hse::Status st(_txn->abort());
        invariantHseSt(st);

        _txn_cached = _txn;
        _txn = nullptr;

        _snapId = nextSnapshotId.fetchAndAdd(1);
    }

    // rollback all changes
    try {
        for (Changes::const_reverse_iterator it = _changes.rbegin(); it != _changes.rend(); it++) {
            Change* change = *it;
            change->rollback();
        }
        _changes.clear();
    } catch (...) {
        invariantHse(false);  // abort
    }

    // deactivate
    _deltaCounters.clear();
}

bool KVDBRecoveryUnit::waitUntilDurable() {
    _durabilityManager.waitUntilDurable();
    return true;
}

void KVDBRecoveryUnit::abandonSnapshot() {
    if (_txn) {
        hse::Status st(_txn->abort());
        invariantHseSt(st);

        _txn_cached = _txn;
        _txn = nullptr;

        _snapId = nextSnapshotId.fetchAndAdd(1);
    }

    _deltaCounters.clear();
}

SnapshotId KVDBRecoveryUnit::getSnapshotId() const {
    return SnapshotId(_snapId);
}

void KVDBRecoveryUnit::registerChange(Change* change) {
    _changes.push_back(change);
}

void* KVDBRecoveryUnit::writingPtr(void* data, size_t len) {
    invariantHse(!"don't call writingPtr");
}

void KVDBRecoveryUnit::setRollbackWritesDisabled() {}

hse::Status KVDBRecoveryUnit::put(const KVSHandle& h, const KVDBData& key, const KVDBData& val) {
    _ensureTxn();
    hse::Status st = _kvdb.kvs_put(h, _txn, key, val);
    int errn = st.getErrno();
    if (ECANCELED == errn) {
        throw WriteConflictException();
    }
    return st;
}

hse::Status KVDBRecoveryUnit::probeVlen(
    const KVSHandle& h, const KVDBData& key, KVDBData& val, unsigned long len, bool& found) {
    _ensureTxn();
    invariantHse(tlsReadBuf);
    invariantHse(len <= 1 + VALUE_META_SIZE + MAX_BYTES_LEB128);
    val.setReadBuf(tlsReadBuf.get(), len);

    // On a compressed record store, "val" contains len bytes not having
    // been uncompressed yet. This read is used only to adjust length stats,
    // during a DeleteRecord/UpdateRecord. It reads up to 10 (len) bytes but
    // sets the full value length in _len.
    // This function does NOT update the value framing (updateFraming()).
    return _kvdb.kvs_probe_len(h, _txn, key, val, found);
}

hse::Status KVDBRecoveryUnit::_get(
    const KVSHandle& h, const KVDBData& key, KVDBData& val, bool& found, bool use_txn) {
    if (use_txn)
        _ensureTxn();

    // Allocate a new buffer if none exists, or if the owned buffer
    // isn't an incomplete chunked buffer (with room to copy more).
    if (val.getAllocLen() <= HSE_KVS_VLEN_MAX || val.getAllocLen() == val.len()) {
        invariantHse(tlsReadBuf);
        val.setReadBuf(tlsReadBuf.get(), HSE_KVS_VLEN_MAX);
    }

    return _kvdb.kvs_get(h, use_txn ? _txn : nullptr, key, val, found);
}

// On a compressed record store, "val" contains data not having
// been de-compressed yet.
// This function does NOT update the value framing (updateFraming()).
hse::Status KVDBRecoveryUnit::getMCo(
    const KVSHandle& h, const KVDBData& key, KVDBData& val, bool& found, bool use_txn) {
    return _get(h, key, val, found, use_txn);
}

hse::Status KVDBRecoveryUnit::prefixGet(const KVSHandle& h,
                                        const KVDBData& prefix,
                                        KVDBData& key,
                                        KVDBData& val,
                                        hse_kvs_pfx_probe_cnt& found) {
    _ensureTxn();

    return _kvdb.kvs_prefix_probe(h, _txn, prefix, key, val, found);
}


hse::Status KVDBRecoveryUnit::probeKey(const KVSHandle& h, const KVDBData& key, bool& found) {
    _ensureTxn();

    return _kvdb.kvs_probe_key(h, _txn, key, found);
}

hse::Status KVDBRecoveryUnit::del(const KVSHandle& h, const KVDBData& key) {
    _ensureTxn();
    hse::Status st = _kvdb.kvs_delete(h, _txn, key);
    int errn = st.getErrno();
    if (ECANCELED == errn) {
        throw WriteConflictException();
    }
    return st;
}

hse::Status KVDBRecoveryUnit::prefixDelete(const KVSHandle& h, const KVDBData& prefix) {
    hse::Status st;

    _ensureTxn();
    st = _kvdb.kvs_prefix_delete(h, _txn, prefix);
    if (st.getErrno() == ECANCELED)
        throw WriteConflictException();

    return st;
}

hse::Status KVDBRecoveryUnit::iterDelete(const KVSHandle& h, const KVDBData& prefix) {
    _ensureTxn();
    hse::Status st = _kvdb.kvs_iter_delete(h, _txn, prefix);
    int errn = st.getErrno();
    if (ECANCELED == errn) {
        throw WriteConflictException();
    }
    return st;
}

hse::Status KVDBRecoveryUnit::nonTxnPut(const KVSHandle& h,
                                        const KVDBData& key,
                                        const KVDBData& val) {
    // mongo bulk inserts use non transactional puts.
    hse::Status st = _kvdb.kvs_put(h, 0, key, val);
    return st;
}

hse::Status KVDBRecoveryUnit::nonTxnDel(const KVSHandle& h, const KVDBData& key) {
    hse::Status st = _kvdb.kvs_delete(h, 0, key);
    return st;
}

hse::Status KVDBRecoveryUnit::nonTxnPfxDel(const KVSHandle& h, const KVDBData& prefix) {
    hse::Status st = _kvdb.kvs_prefix_delete(h, 0, prefix);
    return st;
}

hse::Status KVDBRecoveryUnit::nonTxnIterDelete(const KVSHandle& h, const KVDBData& prefix) {
    hse::Status st = _kvdb.kvs_iter_delete(h, 0, prefix);
    return st;
}

hse::Status KVDBRecoveryUnit::beginScan(const KVSHandle& h,
                                        KVDBData pfx,
                                        bool forward,
                                        KvsCursor** cursor,
                                        const struct CompParms& compparm) {
    KvsCursor* lcursor = 0;

    _ensureTxn();

    try {
        lcursor = create_cursor(h, pfx, forward, compparm, _txn);
    } catch (...) {
        return hse::Status(ENOMEM);
    }
    invariantHse(lcursor != 0);
    *cursor = lcursor;

    return 0;
}

hse::Status KVDBRecoveryUnit::cursorUpdate(KvsCursor* cursor) {
    _ensureTxn();
    auto st = cursor->update(_txn);
    invariantHse(st.ok());

    return st;
}

hse::Status KVDBRecoveryUnit::cursorSeek(KvsCursor* cursor, const KVDBData& key, KVDBData* pos) {
    return cursor->seek(key, nullptr, pos);
}

hse::Status KVDBRecoveryUnit::cursorRead(KvsCursor* cursor,
                                         KVDBData& key,
                                         KVDBData& val,
                                         bool& eof) {
    return cursor->read(key, val, eof);
}

hse::Status KVDBRecoveryUnit::endScan(KvsCursor* cursor) {
    delete cursor;

    return 0;
}

hse::Status KVDBRecoveryUnit::beginOplogScan(const KVSHandle& h,
                                             KVDBData pfx,
                                             bool forward,
                                             KvsCursor** cursor,
                                             const struct CompParms& compparm) {
    KvsCursor* lcursor = 0;

    /* Make sure this is an unbound cursor in order to be see all commits so far. */
    try {
        lcursor = create_cursor(h, pfx, forward, compparm, nullptr);
    } catch (...) {
        return hse::Status(ENOMEM);
    }
    invariantHse(lcursor != 0);
    *cursor = lcursor;

    return 0;
}

hse::Status KVDBRecoveryUnit::oplogCursorUpdate(KvsCursor* cursor) {
    /* Make sure this is an unbound cursor in order to be see all commits so far. */
    auto st = cursor->update(nullptr);
    invariantHse(st.ok());

    return st;
}

hse::Status KVDBRecoveryUnit::oplogCursorSeek(KvsCursor* cursor,
                                              const KVDBData& key,
                                              const KVDBData* kmax,
                                              KVDBData* pos) {
    return cursor->seek(key, kmax, pos);
}

void KVDBRecoveryUnit::incrementCounter(unsigned long counterKey,
                                        std::atomic<long long>* counter,
                                        long long delta) {
    if (delta == 0) {
        return;
    }

    if (_deltaCounters.bucket_count() < 8)
        _deltaCounters.rehash(8);

    auto pair = _deltaCounters.find(counterKey);
    if (pair == _deltaCounters.end()) {
        _deltaCounters[counterKey] = KVDBCounter(counter, delta);
    } else {
        pair->second._delta += delta;
    }
}

void KVDBRecoveryUnit::resetCounter(unsigned long counterKey, std::atomic<long long>* counter) {
    counter->store(0);
}

long long KVDBRecoveryUnit::getDeltaCounter(unsigned long counterKey) {
    auto counter = _deltaCounters.find(counterKey);
    if (counter == _deltaCounters.end()) {
        return 0;
    } else {
        return counter->second._delta;
    }
}

// struct kvdb_txn* KVDBRecoveryUnit::getKvdbTxn() {
//     return _txn;
// }

KVDBRecoveryUnit* KVDBRecoveryUnit::newKVDBRecoveryUnit() {
    return new KVDBRecoveryUnit(_kvdb, _counterManager, _durabilityManager);
}

// private
void KVDBRecoveryUnit::_ensureTxn() {
    if (!_txn) {
        hse::Status st{};

        if (_txn_cached) {
            _txn = _txn_cached;
            _txn_cached = nullptr;
        } else {
            try {
                _txn = new (_txn_mem) ClientTxn(_kvdb.kvdb_handle());
            } catch (...) {
                st = hse::Status{1};
            }
            invariantHseSt(st);
        }

        // start a transaction.
        st = _txn->begin();
        invariantHseSt(st);
    }
}

/* End  KVDBRecoveryUnit */
}
