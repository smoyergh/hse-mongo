/**
 * TBD: HSE License
 */
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"
#include "mongo/platform/endian.h"
#include "mongo/util/log.h"

#include "hse_counter_manager.h"
#include "hse_index.h"
#include "hse_record_store.h"
#include "hse_util.h"

using hse::KVDBData;
using hse::KVDB;

using namespace std;

namespace mongo {
KVDBCounterManager::KVDBCounterManager(bool crashSafe)
    : _crashSafe(crashSafe), _syncing(false), _updates(0) {}


void KVDBCounterManager::registerRecordStore(KVDBRecordStore* rs) {
    stdx::lock_guard<stdx::mutex> lk(_setLock);
    _recordStores.insert(rs);
}

void KVDBCounterManager::deregisterRecordStore(KVDBRecordStore* rs) {
    stdx::lock_guard<stdx::mutex> lk(_setLock);
    _recordStores.erase(rs);
}

void KVDBCounterManager::registerIndex(KVDBIdxBase* idx) {
    stdx::lock_guard<stdx::mutex> lk(_setLock);
    _indexes.insert(idx);
}

void KVDBCounterManager::deregisterIndex(KVDBIdxBase* idx) {
    stdx::lock_guard<stdx::mutex> lk(_setLock);
    _indexes.erase(idx);
}

void KVDBCounterManager::_syncAllCounters(void) {
    stdx::lock_guard<stdx::mutex> lk(_setLock);
    for (auto& rs : _recordStores) {
        rs->updateCounters();
    }
    for (auto& idx : _indexes) {
        idx->updateCounter();
    }
}

void KVDBCounterManager::_syncCountersIfNeeded(void) {
    bool expected = false;

    auto changed = _syncing.compare_exchange_weak(expected, true);
    if (!changed) {
        return;
    }

    // This is the only thread that will be syncing the counters to kvdb
    _updates.store(0);
    _syncAllCounters();
    _syncing.store(false);
}

void KVDBCounterManager::incrementNumUpdates(void) {
    auto old = _updates.fetch_add(1, std::memory_order::memory_order_relaxed);

    if (old + 1 >= _kSyncEvery) {
        _syncCountersIfNeeded();
    }
}

void KVDBCounterManager::sync(void) {
    bool expected = false;

    // Wait for any currently running sync operation to finish
    while (!_syncing.compare_exchange_weak(expected, true))
        ;

    _updates.store(0);
    _syncAllCounters();
    _syncing.store(false);
}

void KVDBCounterManager::sync_for_rename(std::string& ident) {
    stdx::lock_guard<stdx::mutex> lk(_setLock);
    for (auto& rs : _recordStores) {
        if (ident.compare(rs->getIdent()) == 0) {
            // We are in the context of a collection rename.
            // We are in the context of the new/second instance of RecordStore
            // (the caller) starting.
            // "rs" is the old instance for the same collection.
            // "rs" is idle and is going to be destroyed by mongo shortly.
            // The new instance takes ownership of the counters.
            // Here, we force the old instance to flush them to media.
            // The caller (second instance) will fetch them from media shortly.
            rs->updateCounters();
            rs->overTake();
            break;
        }
    }
}
}
