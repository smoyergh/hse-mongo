/**
 * TBD: HSE License
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
        // [MU_REVISIT] In an earlier version of the code we knew things about how many
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
        // [MU_REVISIT] Avoid calling this if the newBound hasn't changed. The only case
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
