/**
 * TODO: HSE License
 */
#pragma once

#include <atomic>
#include <list>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>

#include "mongo/base/string_data.h"
#include "mongo/stdx/mutex.h"

#include "hse.h"
#include "hse_exceptions.h"

namespace mongo {

class KVDBIdxBase;
class KVDBRecordStore;

class KVDBCounterManager {
public:
    KVDBCounterManager(bool crashSafe);

    void registerRecordStore(KVDBRecordStore* rs);
    void deregisterRecordStore(KVDBRecordStore* rs);
    void registerIndex(KVDBIdxBase* idx);
    void deregisterIndex(KVDBIdxBase* idx);

    void incrementNumUpdates();
    void sync();
    void sync_for_rename(std::string& ident);

private:
    void _syncAllCounters();
    void _syncCountersIfNeeded();

    // MU_REVISIT Implement a crash safe semantic
    bool _crashSafe = false;

    std::atomic<bool> _syncing{false};
    std::mutex _setLock;
    std::set<KVDBRecordStore*> _recordStores;
    std::set<KVDBIdxBase*> _indexes;

    std::atomic<long long> _updates{0};  // approx. number of updates since last sync
    static const int _kSyncEvery = 10000;
};
}
