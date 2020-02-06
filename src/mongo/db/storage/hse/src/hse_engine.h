/**
 * TODO: License
 */
#pragma once

#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>

#include <boost/optional.hpp>


#include "mongo/base/disallow_copying.h"
#include "mongo/bson/ordering.h"
#include "mongo/db/storage/kv/kv_engine.h"
#include "mongo/util/background.h"
#include "mongo/util/string_map.h"


#include "hse_counter_manager.h"
#include "hse_durability_manager.h"
#include "hse_exceptions.h"
#include "hse_impl.h"
#include "hse_index.h"
#include "hse_record_store.h"

using std::string;

using hse::KVDBImpl;
using hse::CompAlgo;

namespace mongo {

enum KVDBIdentType {
    COLL,       // Collection
    STDINDEX,   // Standard index
    UNIQINDEX,  // Unique index
    OPLOG       // Oplog
};

class KVDBJournalFlusher : public BackgroundJob {
public:
    explicit KVDBJournalFlusher(KVDBDurabilityManager& durabilityManager);

    virtual string name() const;

    virtual void run();

    void shutdown();

private:
    KVDBDurabilityManager& _durabilityManager;
    std::atomic<bool> _shuttingDown{false};  // NOLINT
};

class KVDBEngine final : public KVEngine {
    MONGO_DISALLOW_COPYING(KVDBEngine);

public:
    KVDBEngine(const string& path, bool durable, int formatVersion, bool readOnly);
    virtual ~KVDBEngine();

    virtual RecoveryUnit* newRecoveryUnit() override;

    virtual Status createRecordStore(OperationContext* opCtx,
                                     StringData ns,
                                     StringData ident,
                                     const CollectionOptions& options) override;

    virtual std::unique_ptr<RecordStore> getRecordStore(OperationContext* opCtx,
                                                        StringData ns,
                                                        StringData ident,
                                                        const CollectionOptions& colOpts) override;

    virtual Status createSortedDataInterface(OperationContext* opCtx,
                                             StringData ident,
                                             const IndexDescriptor* desc) override;

    virtual SortedDataInterface* getSortedDataInterface(OperationContext* opCtx,
                                                        StringData ident,
                                                        const IndexDescriptor* desc) override;

    virtual Status dropIdent(OperationContext* opCtx, StringData ident) override;

    virtual bool hasIdent(OperationContext* opCtx, StringData ident) const override;

    virtual std::vector<std::string> getAllIdents(OperationContext* opCtx) const override;

    virtual bool supportsDocLocking() const override;

    virtual bool supportsDirectoryPerDB() const override;

    virtual int flushAllFiles(bool sync) override;

    virtual Status beginBackup(OperationContext* txn) override;

    virtual void endBackup(OperationContext* txn) override;

    virtual bool isDurable() const override;

    virtual bool isEphemeral() const override;

    virtual int64_t getIdentSize(OperationContext* opCtx, StringData ident);

    virtual Status repairIdent(OperationContext* opCtx, StringData ident);

    virtual void cleanShutdown();

    virtual SnapshotManager* getSnapshotManager() const final;

    /**
     * Initializes a background job to remove excess documents in the oplog collections.
     * This applies to the capped collections in the local.oplog.* namespaces (specifically
     * local.oplog.rs for replica sets and local.oplog.$main for master/slave replication).
     * Returns true if a background job is running for the namespace.
     */
    static bool initOplogStoreThread(StringData ns);


    virtual void setJournalListener(JournalListener* jl);


private:
    void _setupDb();
    void _open_kvdb(const string& mpoolName, const string& kvdbName, unsigned int snap);
    void _open_kvs(const string& kvsName, KVSHandle& h, struct hse_params* params);
    void _cleanShutdown();
    void _loadMaxPrefix();
    Status _createIdent(OperationContext* opCtx,
                        StringData ident,
                        KVDBIdentType type,
                        CompParms& compparms,
                        BSONObjBuilder* configBuilder);
    BSONObj _getIdentConfig(StringData ident);
    uint32_t _extractPrefix(const BSONObj& config);
    KVDBIdentType _extractType(const BSONObj& config);

    bool _durable;
    const int _formatVersion;

    // KVDB
    KVDBImpl _db{};

    // Const values for db
    static const string kMainKvsName;
    static const string kUniqIdxKvsName;
    static const string kStdIdxKvsName;
    static const string kLargeKvsName;
    static const string kOplogKvsName;
    static const string kOplogLargeKvsName;

    // Special prefixes
    static const string kMetadataPrefix;

    KVSHandle _mainKvs;
    KVSHandle _stdIdxKvs;
    KVSHandle _uniqIdxKvs;
    KVSHandle _largeKvs;
    KVSHandle _oplogKvs;
    KVSHandle _oplogLargeKvs;

    // ident map stores mapping from ident to a BSON config
    mutable stdx::mutex _identMapMutex;
    typedef StringMap<BSONObj> IdentMap;
    IdentMap _identMap;

    // protected by _identMapMutex
    uint32_t _maxPrefix;

    // _identObjectMapMutex protects both _identIndexMap and _identCollectionMap. It should
    // never be locked together with _identMapMutex
    mutable stdx::mutex _identObjectMapMutex;
    // mapping from ident --> index object. we don't own the object
    StringMap<KVDBIdxBase*> _identIndexMap;
    // mapping from ident --> collection object
    StringMap<KVDBRecordStore*> _identCollectionMap;


    std::unique_ptr<KVDBDurabilityManager> _durabilityManager;
    // CounterManages manages counters like numRecords and dataSize for record stores
    std::unique_ptr<KVDBCounterManager> _counterManager;

    std::unique_ptr<KVDBJournalFlusher> _journalFlusher;  // Depends on _durabilityManager
};
}
