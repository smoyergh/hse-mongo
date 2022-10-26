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

#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>

#include <boost/optional.hpp>


#include "mongo/base/disallow_copying.h"
#include "mongo/bson/ordering.h"
#include "mongo/db/storage/kv/kv_engine.h"
#include "mongo/util/string_map.h"


#include "hse_counter_manager.h"
#include "hse_durability_manager.h"
#include "hse_exceptions.h"
#include "hse_impl.h"
#include "hse_index.h"
#include "hse_record_store.h"

using std::string;


using hse::KVDBImpl;

namespace mongo {

enum KVDBIdentType {
    COLL,       // Collection
    STDINDEX,   // Standard index
    UNIQINDEX,  // Unique index
    OPLOG       // Oplog
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
     * local.oplog.rs for replica sets and local.oplog.$main for primary/secondary replication).
     * Returns true if a background job is running for the namespace.
     */
    static bool initOplogStoreThread(StringData ns);


    virtual void setJournalListener(JournalListener* jl);


private:
    void _prepareConfig();
    void _setupDb();
    void _open_kvdb(const string& dbHome,
                    const vector<string>& cParams,
                    const vector<string>& rParams);
    void _open_kvs(const string& kvsName,
                   KVSHandle& h,
                   const vector<string>& cParams,
                   const vector<string>& rParams);
    void _cleanShutdown();
    uint32_t _getMaxPrefixInKvs(KVSHandle& kvs);
    void _checkMaxPrefix();
    void _loadMaxPrefix();
    Status _createIdent(OperationContext* opCtx,
                        StringData ident,
                        KVDBIdentType type,
                        BSONObjBuilder* configBuilder);
    BSONObj _getIdentConfig(StringData ident);
    uint32_t _extractPrefix(const BSONObj& config);
    KVDBIdentType _extractType(const BSONObj& config);
    string _getMongoConfigStr(void);

    const string _dbHome;
    bool _durable;
    const int _formatVersion;

    // KVDB
    KVDBImpl _db{};

    // Const values for db
    static const string kMetaKvsName;
    static const string kMainKvsName;
    static const string kUniqIdxKvsName;
    static const string kStdIdxKvsName;
    static const string kLargeKvsName;
    static const string kOplogKvsName;
    static const string kOplogLargeKvsName;

    // Special prefixes
    static const string kMetadataPrefix;

    // configuration
    vector<string> _kvdbCParams{};
    vector<string> _kvdbRParams{};
    vector<string> _metaKvsCParams{};
    vector<string> _metaKvsRParams{};
    vector<string> _mainKvsCParams{};
    vector<string> _mainKvsRParams{};
    vector<string> _largeKvsCParams{};
    vector<string> _largeKvsRParams{};
    vector<string> _oplogKvsCParams{};
    vector<string> _oplogKvsRParams{};
    vector<string> _oplogLargeKvsCParams{};
    vector<string> _oplogLargeKvsRParams{};
    vector<string> _uniqIdxKvsCParams{};
    vector<string> _uniqIdxKvsRParams{};
    vector<string> _stdIdxKvsCParams{};
    vector<string> _stdIdxKvsRParams{};

    KVSHandle _metaKvs;
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

    std::shared_ptr<KVDBOplogBlockManager> _oplogBlkMgr{};
};
}  // namespace mongo
