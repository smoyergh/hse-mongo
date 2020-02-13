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

#include "mongo/base/disallow_copying.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/platform/basic.h"
#include "mongo/stdx/condition_variable.h"

#include "hse.h"

namespace mongo {

class JournalListener;
class KVDBCappedVisibilityManager;

class KVDBDurabilityManager {
    MONGO_DISALLOW_COPYING(KVDBDurabilityManager);

public:
    KVDBDurabilityManager(hse::KVDB& db, bool durable, int rsLag);
    void setJournalListener(JournalListener* jl);
    void setOplogVisibilityManager(KVDBCappedVisibilityManager* kcvm);
    void sync();
    void waitUntilDurable();
    bool isDurable() const {
        return _durable;
    }

    int getForceLag() const {
        return _forceLag;
    }

private:
    hse::KVDB& _db;
    uint64_t _numSyncs;
    int _forceLag;
    bool _durable;

    // Notified when we persist records.
    KVDBCappedVisibilityManager* _oplogVisibilityManager;

    // Protects _oplogVisibilityManager.
    mutable stdx::mutex _oplogMutex;

    // Notified when we commit to the journal.
    JournalListener* _journalListener;

    // Protects _journalListener.
    std::mutex _journalListenerMutex;

    // Protects _numSyncs.
    mutable std::mutex _syncMutex;
    mutable stdx::condition_variable _syncDoneCV;
};

}  // namespace mongo
