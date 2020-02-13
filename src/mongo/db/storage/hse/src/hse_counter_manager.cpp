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
