/**
 * TODO: HSE License
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include <mutex>
#include <set>

#include "mongo/base/checked_cast.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/client.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"
#include "mongo/util/background.h"
#include "mongo/util/exit.h"
#include "mongo/util/log.h"

#include "hse_engine.h"
#include "hse_record_store.h"
#include "hse_recovery_unit.h"

namespace mongo {

namespace {

std::set<NamespaceString> _backgroundThreadNamespaces;
stdx::mutex _backgroundThreadMutex;

class KVDBOplogStoreThread : public BackgroundJob {
public:
    KVDBOplogStoreThread(const NamespaceString& ns)
        : BackgroundJob(true /* deleteSelf */), _ns(ns) {
        _name = std::string("KVDBOplogStoreThread for ") + _ns.toString();
    }

    virtual std::string name() const {
        return _name;
    }

    bool _deleteExcessDocuments() {
        if (!getGlobalServiceContext()->getGlobalStorageEngine()) {
            LOG(2) << "no global storage engine yet";
            return false;
        }

        const ServiceContext::UniqueOperationContext txnPtr = cc().makeOperationContext();
        OperationContext& txn = *txnPtr;

        try {
            ScopedTransaction transaction(&txn, MODE_IX);

            AutoGetDb autoDb(&txn, _ns.db(), MODE_IX);
            Database* db = autoDb.getDb();
            if (!db) {
                LOG(2) << "no local database yet";
                return false;
            }

            Lock::CollectionLock collectionLock(txn.lockState(), _ns.ns(), MODE_IX);
            Collection* collection = db->getCollection(_ns);
            if (!collection) {
                LOG(2) << "no collection " << _ns;
                return false;
            }

            OldClientContext ctx(&txn, _ns.ns(), false);
            KVDBOplogStore* rs = checked_cast<KVDBOplogStore*>(collection->getRecordStore());

            if (!rs->yieldAndAwaitOplogDeletionRequest(&txn)) {
                return false;  // Oplog went away.
            }
            rs->reclaimOplog(&txn);
        } catch (const std::exception& e) {
            severe() << "error in KVDBOplogStoreThread: " << e.what();
            fassertFailedNoTrace(!"error in KVDBOplogStoreThread");
        } catch (...) {
            fassertFailedNoTrace(!"unknown error in KVDBOplogStoreThread");
        }
        return true;
    }

    virtual void run() {
        Client::initThread(_name.c_str());

        while (!inShutdown()) {
            if (!_deleteExcessDocuments()) {
                sleepmillis(1000);  // Back off in case there were problems deleting.
            }
        }
    }


private:
    NamespaceString _ns;
    std::string _name;
};

}  // namespace

// static
bool KVDBEngine::initOplogStoreThread(StringData ns) {
    if (!NamespaceString::oplog(ns)) {
        return false;
    }

    if (storageGlobalParams.repair) {
        LOG(1) << "not starting KVDBOplogStoreThread for " << ns << " because we are in repair";
        return false;
    }

    stdx::lock_guard<stdx::mutex> lock(_backgroundThreadMutex);
    NamespaceString nss(ns);
    if (_backgroundThreadNamespaces.count(nss)) {
        log() << "KVDBOplogStoreThread " << ns << " already started";
    } else {
        log() << "Starting KVDBOplogStoreThread " << ns;
        BackgroundJob* backgroundThread = new KVDBOplogStoreThread(nss);
        backgroundThread->go();
        _backgroundThreadNamespaces.insert(nss);
    }
    return true;
}

}  // namespace mongo
