/**
 * TODO: HSE License
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
