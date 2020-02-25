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
#include "mongo/db/storage/journal_listener.h"
#include "mongo/platform/basic.h"

#include "hse.h"
#include "hse_durability_manager.h"
#include "hse_record_store.h"
#include "hse_util.h"

using hse::KVDB;
using hse::KVDBData;

using namespace std;
using namespace std::chrono;

namespace mongo {
/* Start KVDBDurabilityManager */

KVDBDurabilityManager::KVDBDurabilityManager(hse::KVDB& db, bool durable, int forceLag)
    : _db(db),
      _numSyncs(0),
      _forceLag(forceLag),
      _durable(durable),
      _oplogVisibilityManager(nullptr),
      _journalListener(&NoOpJournalListener::instance) {}

void KVDBDurabilityManager::setJournalListener(JournalListener* jl) {
    std::lock_guard<std::mutex> lock(_journalListenerMutex);
    _journalListener = jl;
}

void KVDBDurabilityManager::setOplogVisibilityManager(KVDBCappedVisibilityManager* kcvm) {
    std::lock_guard<std::mutex> lock(_oplogMutex);
    if (kcvm) {
        // [HSE_REVISIT] In an earlier version of the code we knew things about how many
        //              times the _oplogVisibilityManager could be set to a non-NULL
        //              value. It's unclear how and whether to bring back that sort of
        //              constraint. The issue is hit in the unit tests, at the least,
        //              where a durability manager persists across two instances of
        //              a KVDBOplogStore being created.
        _oplogVisibilityManager = kcvm;
    } else {
        _oplogVisibilityManager = nullptr;
    }
}

void KVDBDurabilityManager::sync() {
    if (!_durable)
        return;

    std::lock_guard<std::mutex> lock(_journalListenerMutex);
    JournalListener::Token token = _journalListener->getToken();

    int64_t newBound;

    _oplogMutex.lock();
    if (_oplogVisibilityManager) {
        // All records prior to the current commitBoundary are known to be durable after
        // this sync.
        newBound = _oplogVisibilityManager->getCommitBoundary();
    }

    invariantHseSt(_db.kvdb_sync());

    if (_oplogVisibilityManager) {
        // Some oplog records may have been persisted as a result of this sync. Notify
        // the visibility manager about the records newly persisted.
        // [HSE_REVISIT] Avoid calling this if the newBound hasn't changed. The only case
        // to handle is when persistBoundary changes to something other than what we
        // notified the visibility manager about (truncate/init/any reset).
        _oplogVisibilityManager->durableCallback(newBound);
    }
    _oplogMutex.unlock();

    _syncMutex.lock();
    _numSyncs++;
    _syncMutex.unlock();

    // Notify all waitUntilDurable threads that a sync just completed.
    _syncDoneCV.notify_all();

    _journalListener->onDurable(token);
}

void KVDBDurabilityManager::waitUntilDurable() {
    if (!_durable)
        return;

    stdx::unique_lock<stdx::mutex> lk(_syncMutex);

    const auto waitingFor = _numSyncs;

    _syncDoneCV.wait(lk, [&] { return _numSyncs > waitingFor + 1; });
}

/* End KVDBDurabilityManager */
}
