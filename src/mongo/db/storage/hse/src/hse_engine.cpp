/**
 *    SPDX-License-Identifier: AGPL-3.0-only
 *
 *    Copyright (C) 2017-2021 Micron Technology, Inc.
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

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/tokenizer.hpp>
#include <chrono>
#include <iostream>
#include <vector>

#include "mongo/platform/basic.h"

#include "mongo/db/client.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/stdx/memory.h"

#include "mongo/util/log.h"

#include "hse_engine.h"
#include "hse_global_options.h"
#include "hse_kvscursor.h"
#include "hse_recovery_unit.h"
#include "hse_stats.h"
#include "hse_util.h"

using namespace std;
using namespace std::chrono;

using hse::DEFAULT_PFX_LEN;
using hse::DUR_LAG;
using hse::KVDB_prefix;
using hse::KVDBData;
using hse::OPLOG_PFX_LEN;
using hse::STDIDX_SFX_LEN;

using hse_stat::KVDBStatRate;

namespace mongo {

namespace {

std::string encodePrefix(uint32_t prefix) {
    uint32_t bigEndianPrefix = endian::nativeToBig(prefix);
    return std::string(reinterpret_cast<const char*>(&bigEndianPrefix), sizeof(uint32_t));
}

uint32_t decodePrefix(const uint8_t* prefixPtr) {
    const uint32_t* bigEndianPrefix = reinterpret_cast<const uint32_t*>(prefixPtr);
    return endian::bigToNative(*bigEndianPrefix);
}

}  // namespace

/* Start KVDBEngine */
const string KVDBEngine::kMainKvsName = "MainKvs";
const string KVDBEngine::kUniqIdxKvsName = "UniqIdxKvs";
const string KVDBEngine::kStdIdxKvsName = "StdIdxKvs";
const string KVDBEngine::kLargeKvsName = "LargeKvs";
const string KVDBEngine::kOplogKvsName = "OplogKvs";
const string KVDBEngine::kOplogLargeKvsName = "OplogLargeKvs";
const string KVDBEngine::kMetadataPrefix = KVDB_prefix + "meta-";


KVDBEngine::KVDBEngine(const std::string& path, bool durable, int formatVersion, bool readOnly)
    : _dbHome(path), _durable(durable), _formatVersion(formatVersion), _maxPrefix(0) {
    _setupDb();

    _loadMaxPrefix();

    _counterManager.reset(new KVDBCounterManager(kvdbGlobalOptions.getCrashSafeCounters()));
    _durabilityManager.reset(
        new KVDBDurabilityManager(_db, _durable, kvdbGlobalOptions.getForceLag()));

    // init thread for rate calc
    KVDBStatRate::init();
}

KVDBEngine::~KVDBEngine() {
    _cleanShutdown();
}

RecoveryUnit* KVDBEngine::newRecoveryUnit() {
    return new KVDBRecoveryUnit(_db, *(_counterManager.get()), *(_durabilityManager.get()));
}

Status KVDBEngine::createRecordStore(OperationContext* opCtx,
                                     StringData ns,
                                     StringData ident,
                                     const CollectionOptions& options) {
    BSONObjBuilder configBuilder;
    KVDBIdentType iType = NamespaceString::oplog(ns) ? KVDBIdentType::OPLOG : KVDBIdentType::COLL;

    // The options are in a BSON whose name is "hse".
    BSONObj engine = options.storageEngine.getObjectField("hse");


    if (!engine.isEmpty()) {
        // HSE_REVIST: TBD when we have put options for compression.
    }

    return _createIdent(opCtx, ident, iType, &configBuilder);
}

std::unique_ptr<RecordStore> KVDBEngine::getRecordStore(OperationContext* opCtx,
                                                        StringData ns,
                                                        StringData ident,
                                                        const CollectionOptions& colOpts) {
    BSONObj config = _getIdentConfig(ident);
    uint32_t prefix = _extractPrefix(config);
    KVDBIdentType iType = _extractType(config);
    std::unique_ptr<KVDBRecordStore> recordStore{};

    KVDBDurabilityManager& durRef = *(_durabilityManager.get());
    KVDBCounterManager& counterRef = *(_counterManager.get());


    if (!colOpts.capped) {
        recordStore = stdx::make_unique<KVDBRecordStore>(
            opCtx, ns, ident, _db, _mainKvs, _largeKvs, prefix, durRef, counterRef);
    } else {
        int64_t cappedMaxSize = colOpts.cappedSize ? colOpts.cappedSize : 4096;
        int64_t cappedMaxDocs = colOpts.cappedMaxDocs ? colOpts.cappedMaxDocs : -1;

        if (iType != KVDBIdentType::OPLOG) {
            recordStore = stdx::make_unique<KVDBCappedRecordStore>(opCtx,
                                                                   ns,
                                                                   ident,
                                                                   _db,
                                                                   _mainKvs,
                                                                   _largeKvs,
                                                                   prefix,
                                                                   durRef,
                                                                   counterRef,
                                                                   cappedMaxSize,
                                                                   cappedMaxDocs);
        } else {
            invariantHse(colOpts.capped);
            std::unique_ptr<KVDBOplogStore> oplogRs =
                stdx::make_unique<KVDBOplogStore>(opCtx,
                                                  ns,
                                                  ident,
                                                  _db,
                                                  _oplogKvs,
                                                  _oplogLargeKvs,
                                                  prefix,
                                                  durRef,
                                                  counterRef,
                                                  cappedMaxSize);

            _oplogBlkMgr = oplogRs->getOplogBlkMgr();
            recordStore = std::move(oplogRs);
        }
    }

    {
        stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);
        _identCollectionMap[ident] = recordStore.get();
    }

    return std::move(recordStore);
}

Status KVDBEngine::createSortedDataInterface(OperationContext* opCtx,
                                             StringData ident,
                                             const IndexDescriptor* desc) {
    BSONObjBuilder configBuilder;
    KVDBIdentType iType = desc->unique() ? KVDBIdentType::UNIQINDEX : KVDBIdentType::STDINDEX;

    // let index add its own config things
    KVDBIdxBase::generateConfig(&configBuilder, _formatVersion, desc->version());
    return _createIdent(opCtx, ident, iType, &configBuilder);
}

SortedDataInterface* KVDBEngine::getSortedDataInterface(OperationContext* opCtx,
                                                        StringData ident,
                                                        const IndexDescriptor* desc) {
    auto config = _getIdentConfig(ident);
    std::string prefix = encodePrefix(_extractPrefix(config));

    KVDBIdxBase* index;
    const std::string indexSizeKey = KVDB_prefix + "indexsize-" + ident.toString();

    if (desc->unique()) {
        index = new KVDBUniqIdx(_db,
                                _uniqIdxKvs,
                                *(_counterManager.get()),
                                prefix,
                                ident.toString(),
                                Ordering::make(desc->keyPattern()),
                                std::move(config),
                                desc->isPartial(),
                                desc->getNumFields(),
                                indexSizeKey);
    } else {
        index = new KVDBStdIdx(_db,
                               _stdIdxKvs,
                               *(_counterManager.get()),
                               prefix,
                               ident.toString(),
                               Ordering::make(desc->keyPattern()),
                               std::move(config),
                               desc->getNumFields(),
                               indexSizeKey);
    }
    {
        stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);
        _identIndexMap[ident] = index;
    }
    return index;
}

Status KVDBEngine::dropIdent(OperationContext* opCtx, StringData ident) {

    string delKeyStr = kMetadataPrefix + ident.toString();
    KVDBData keyToDel{delKeyStr};

    // delete metadata
    auto s = _db.kvs_sub_txn_delete(_mainKvs, keyToDel);
    if (!s.ok()) {
        return hseToMongoStatus(s);
    }

    KVDBIdentType type = _extractType(_getIdentConfig(ident));
    uint32_t prefixVal = _extractPrefix(_getIdentConfig(ident));
    string prefixStr = encodePrefix(prefixVal);
    KVDBData pKeyToDel{prefixStr};

    if (KVDBIdentType::COLL == type) {
        string dataSizeKeyStr = KVDB_prefix + "datasize-" + ident.toString();
        string storageSizeKeyStr = KVDB_prefix + "storagesize-" + ident.toString();
        string numRecordsKeyStr = KVDB_prefix + "numrecords-" + ident.toString();

        KVDBData dataSizeKey{dataSizeKeyStr};
        KVDBData storageSizeKey{storageSizeKeyStr};
        KVDBData numRecordsKey{numRecordsKeyStr};

        s = _db.kvs_sub_txn_prefix_delete(_mainKvs, pKeyToDel);
        if (!s.ok()) {
            return hseToMongoStatus(s);
        }
        s = _db.kvs_sub_txn_prefix_delete(_largeKvs, pKeyToDel);
        if (!s.ok()) {
            return hseToMongoStatus(s);
        }

        s = _db.kvs_sub_txn_delete(_mainKvs, dataSizeKey);
        if (!s.ok()) {
            return hseToMongoStatus(s);
        }

        s = _db.kvs_sub_txn_delete(_mainKvs, storageSizeKey);
        if (!s.ok()) {
            return hseToMongoStatus(s);
        }

        s = _db.kvs_sub_txn_delete(_mainKvs, numRecordsKey);
        if (!s.ok()) {
            return hseToMongoStatus(s);
        }

        _identCollectionMap.erase(ident);
    } else if (KVDBIdentType::OPLOG == type) {
        _oplogBlkMgr->dropAllBlocks(opCtx, prefixVal);
        _identCollectionMap.erase(ident);
    } else {  // Index
        string indexSizeKeyStr = KVDB_prefix + "indexsize-" + ident.toString();
        KVDBData indexSizeKey{indexSizeKeyStr};

        if (KVDBIdentType::STDINDEX == type) {
            s = _db.kvs_sub_txn_prefix_delete(_stdIdxKvs, pKeyToDel);
            if (!s.ok()) {
                return hseToMongoStatus(s);
            }

            s = _db.kvs_sub_txn_delete(_stdIdxKvs, indexSizeKey);
            if (!s.ok()) {
                return hseToMongoStatus(s);
            }
        } else {
            invariantHse(type == KVDBIdentType::UNIQINDEX);
            s = _db.kvs_sub_txn_prefix_delete(_uniqIdxKvs, pKeyToDel);
            if (!s.ok()) {
                return hseToMongoStatus(s);
            }

            s = _db.kvs_sub_txn_delete(_uniqIdxKvs, indexSizeKey);
            if (!s.ok()) {
                return hseToMongoStatus(s);
            }
        }
        _identIndexMap.erase(ident);
    }

    // remove from map
    {
        stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
        _identMap.erase(ident);
    }

    return Status::OK();
}

bool KVDBEngine::hasIdent(OperationContext* opCtx, StringData ident) const {
    stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
    return _identMap.find(ident) != _identMap.end();
}

std::vector<std::string> KVDBEngine::getAllIdents(OperationContext* opCtx) const {
    std::vector<std::string> indents;
    for (auto& entry : _identMap) {
        indents.push_back(entry.first);
    }
    return indents;
}

bool KVDBEngine::supportsDocLocking() const {
    return true;
}

bool KVDBEngine::supportsDirectoryPerDB() const {
    return false;
}

int KVDBEngine::flushAllFiles(bool sync) {
    LOG(1) << "KVDBEngine::flushAllFiles";
    _counterManager->sync();
    _durabilityManager->sync();
    return 1;
}

Status KVDBEngine::beginBackup(OperationContext* txn) {
    // HSE_REVISIT: nothing to do here yet.
    return Status::OK();
}

void KVDBEngine::endBackup(OperationContext* txn) {}

bool KVDBEngine::isDurable() const {
    return _durable;
}

bool KVDBEngine::isEphemeral() const {
    return false;
}

int64_t KVDBEngine::getIdentSize(OperationContext* opCtx, StringData ident) {
    stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);

    auto indexIter = _identIndexMap.find(ident);
    if (indexIter != _identIndexMap.end()) {
        return static_cast<int64_t>(indexIter->second->getSpaceUsedBytes(opCtx));
    }
    auto collectionIter = _identCollectionMap.find(ident);
    if (collectionIter != _identCollectionMap.end()) {
        return collectionIter->second->storageSize(opCtx);
    }

    // this can only happen if collection or index exists, but it's not opened (i.e.
    // getRecordStore or getSortedDataInterface are not called)
    return 1;
}

Status KVDBEngine::repairIdent(OperationContext* opCtx, StringData ident) {
    return Status::OK();
}

void KVDBEngine::cleanShutdown() {
    _cleanShutdown();
}

// Not supported
SnapshotManager* KVDBEngine::getSnapshotManager() const {
    return nullptr;
}


void KVDBEngine::setJournalListener(JournalListener* jl) {
    _durabilityManager->setJournalListener(jl);
}

void KVDBEngine::_open_kvdb(const string& dbHome,
                            const vector<string>& cParams,
                            const vector<string>& rParams) {

    auto st = _db.kvdb_open(dbHome.c_str(), rParams);
    if (st.getErrno()) {
        if (st.getErrno() != ENOENT)
            invariantHseSt(st);

        st = _db.kvdb_make(dbHome.c_str(), cParams);
        invariantHseSt(st);

        st = _db.kvdb_open(dbHome.c_str(), rParams);
    }
    invariantHseSt(st);
}

void KVDBEngine::_open_kvs(const string& kvs,
                           KVSHandle& h,
                           const vector<string>& cParams,
                           const vector<string>& rParams) {

    auto st = _db.kvdb_kvs_open(kvs.c_str(), rParams, h);
    if (st.getErrno()) {
        if (st.getErrno() != ENOENT)
            invariantHseSt(st);

        st = _db.kvdb_kvs_make(kvs.c_str(), cParams);
        invariantHseSt(st);

        st = _db.kvdb_kvs_open(kvs.c_str(), rParams, h);
    }
    invariantHseSt(st);
}

void KVDBEngine::_prepareConfig() {
    unsigned int ms = DUR_LAG;

    if (isDurable()) {
        if (storageGlobalParams.journalCommitIntervalMs > 0)
            ms = storageGlobalParams.journalCommitIntervalMs;
    }

    if (!kvdbGlobalOptions.getStagingPathStr().empty()) {
        _kvdbCParams.push_back("storage.staging.path=" + kvdbGlobalOptions.getStagingPathStr());
    }

    if (!kvdbGlobalOptions.getPmemPathStr().empty()) {
        _kvdbCParams.push_back("storage.pmem.path=" + kvdbGlobalOptions.getPmemPathStr());
    }

    const string vComprDefault = kvdbGlobalOptions.getCompressionDefaultStr();

    _kvdbRParams.push_back("txn_timeout=8589934591");
    _kvdbRParams.push_back("durability.interval_ms=" + std::to_string(ms));

    _mainKvsCParams.push_back("prefix.length=" + std::to_string(DEFAULT_PFX_LEN));
    _mainKvsRParams.push_back("transactions.enabled=true");
    _mainKvsRParams.push_back("compression.default=" + vComprDefault);

    _largeKvsCParams.push_back("prefix.length=" + std::to_string(DEFAULT_PFX_LEN));
    _largeKvsRParams.push_back("transactions.enabled=true");
    _largeKvsRParams.push_back("compression.default=" + vComprDefault);

    _oplogKvsCParams.push_back("prefix.length=" + std::to_string(OPLOG_PFX_LEN));
    _oplogKvsCParams.push_back("kvs_ext01=1");
    _oplogKvsRParams.push_back("transactions.enabled=true");

    _oplogLargeKvsCParams.push_back("prefix.length=" + std::to_string(OPLOG_PFX_LEN));
    _oplogLargeKvsCParams.push_back("kvs_ext01=1");
    _oplogLargeKvsRParams.push_back("transactions.enabled=true");

    _uniqIdxKvsCParams.push_back("prefix.length=" + std::to_string(DEFAULT_PFX_LEN));
    _uniqIdxKvsRParams.push_back("transactions.enabled=true");
    _uniqIdxKvsRParams.push_back("compression.default=" + vComprDefault);

    _stdIdxKvsCParams.push_back("prefix.length=" + std::to_string(DEFAULT_PFX_LEN));
    _stdIdxKvsRParams.push_back("transactions.enabled=true");
    _stdIdxKvsRParams.push_back("compression.default=" + vComprDefault);
    _stdIdxKvsRParams.push_back("kvs_sfx_len=" + std::to_string(STDIDX_SFX_LEN));
}

void KVDBEngine::_setupDb() {
    namespace fs = boost::filesystem;
    fs::path dbHomePath(_dbHome);
    if (fs::create_directory(dbHomePath))
        fs::permissions(dbHomePath,
                        fs::perms::owner_all | fs::perms::group_read | fs::perms::group_exe);

    _prepareConfig();

    const std::string configPath = kvdbGlobalOptions.getConfigPathStr();
    if (configPath.empty()) {
        auto st = hse::init();
        invariantHseSt(st);
    } else {
        auto st = hse::init(configPath);
        invariantHseSt(st);
    }

    _open_kvdb(_dbHome, _kvdbCParams, _kvdbRParams);

    _open_kvs(kMainKvsName, _mainKvs, _mainKvsCParams, _mainKvsRParams);
    _open_kvs(kLargeKvsName, _largeKvs, _largeKvsCParams, _largeKvsRParams);

    _open_kvs(kOplogKvsName, _oplogKvs, _oplogKvsCParams, _oplogKvsRParams);
    _open_kvs(kOplogLargeKvsName, _oplogLargeKvs, _oplogLargeKvsCParams, _oplogLargeKvsRParams);

    _open_kvs(kUniqIdxKvsName, _uniqIdxKvs, _uniqIdxKvsCParams, _uniqIdxKvsRParams);

    _open_kvs(kStdIdxKvsName, _stdIdxKvs, _stdIdxKvsCParams, _stdIdxKvsRParams);
}

uint32_t KVDBEngine::_getMaxPrefixInKvs(KVSHandle& kvs) {

    uint32_t retPrefix = 0;

    // create a reverse cursor
    KvsCursor* cursor;
    KVDBData kPrefix{(uint8_t*)"", 0};  // no prefix
    cursor = new KvsCursor(kvs, kPrefix, false, 0);
    invariantHse(cursor != 0);

    KVDBData key{};
    KVDBData val{};
    bool eof = false;

    // read one kv
    auto st = cursor->read(key, val, eof);
    invariantHseSt(st);
    if (eof) {
        retPrefix = 0;
    } else {
        retPrefix = decodePrefix(key.data());
    }

    delete cursor;

    return retPrefix;
}

void KVDBEngine::_checkMaxPrefix() {

    uint32_t maxPrefix = 0, tmpMaxPrefix = 0;

    // for each kvs figure out maxPrefix.
    // _mainkvs
    tmpMaxPrefix = _getMaxPrefixInKvs(_mainKvs);
    maxPrefix = std::max(maxPrefix, tmpMaxPrefix);

    // _stdIdxKvs
    tmpMaxPrefix = _getMaxPrefixInKvs(_stdIdxKvs);
    maxPrefix = std::max(maxPrefix, tmpMaxPrefix);

    // _uniqIdxKvs
    tmpMaxPrefix = _getMaxPrefixInKvs(_uniqIdxKvs);
    maxPrefix = std::max(maxPrefix, tmpMaxPrefix);

    // _oplogKvs
    tmpMaxPrefix = _getMaxPrefixInKvs(_oplogKvs);
    maxPrefix = std::max(maxPrefix, tmpMaxPrefix);

    // if maxPrefix is > _maxPrefix - we have a problem..
    // for now set the new _maxPrefix == maxPrefix.
    // this should be very rare, we could consider deleting the
    // orphan prefixes in a later release.
    if (maxPrefix > _maxPrefix) {
        log() << "Orphan prefixes detected!!, increasing the _maxPrefix value to avoid prefix "
                 "pollution.";
        _maxPrefix = maxPrefix;
    }
}

void KVDBEngine::_loadMaxPrefix() {
    // load ident to prefix map. also update _maxPrefix if there's any prefix bigger than
    // current _maxPrefix
    stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
    KVDBData kPrefix{(uint8_t*)kMetadataPrefix.c_str(), kMetadataPrefix.size()};
    KvsCursor* cursor;

    cursor = new KvsCursor(_mainKvs, kPrefix, true, 0);
    invariantHse(cursor != 0);

    KVDBData key{};
    KVDBData val{};
    bool eof = false;
    while (!eof) {
        auto st = cursor->read(key, val, eof);
        invariantHseSt(st);
        if (eof) {
            break;
        }

        KVDBData ident =
            KVDBData(key.data() + kMetadataPrefix.size(), key.len() - kMetadataPrefix.size());
        // this could throw DBException, which then means DB corruption. We just let it fly
        // to the caller
        BSONObj identConfig((const char*)val.data());
        BSONElement element = identConfig.getField("prefix");

        if (element.eoo() || !element.isNumber()) {
            log() << "Mongo metadata in KVDB is corrupted.";
            invariantHse(false);
        }
        uint32_t identPrefix = static_cast<uint32_t>(element.numberInt());

        LOG(1) << "HSE: Loading Ident " << string((const char*)ident.data(), ident.len());

        _identMap[StringData((const char*)ident.clone().data(), ident.len())] =
            std::move(identConfig.getOwned());

        _maxPrefix = std::max(_maxPrefix, identPrefix);
    }
    invariantHse(eof);

    delete cursor;

    _checkMaxPrefix();
}

void KVDBEngine::_cleanShutdown() {
    _durabilityManager->prepareForShutdown();
    _durabilityManager.reset();

    _counterManager->sync();
    _counterManager.reset();

    KVDBStatRate::finish();

    _db.kvdb_close();
    hse::fini();
}

// non public api
Status KVDBEngine::_createIdent(OperationContext* opCtx,
                                StringData ident,
                                KVDBIdentType type,
                                BSONObjBuilder* configBuilder) {
    BSONObj config;
    uint32_t prefix = 0;
    {
        stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
        if (_identMap.find(ident) != _identMap.end()) {
            // already exists
            return Status::OK();
        }

        prefix = ++_maxPrefix;
        configBuilder->append("prefix", static_cast<int32_t>(prefix));
        configBuilder->append("type", static_cast<int32_t>(type));

        config = std::move(configBuilder->obj());
    }

    string keyStr = kMetadataPrefix + ident.toString();
    KVDBData key{(uint8_t*)keyStr.c_str(), keyStr.size()};
    KVDBData val{(uint8_t*)config.objdata(), (unsigned long)config.objsize()};

    LOG(1) << "HSE: recording ident to kvs : " << ident.toString();
    auto ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opCtx);
    auto s = ru->put(_mainKvs, key, val);

    {
        stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
        _identMap[ident] = config.copy();
    }

    return hseToMongoStatus(s);
}

BSONObj KVDBEngine::_getIdentConfig(StringData ident) {
    stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
    auto identIter = _identMap.find(ident);
    invariantHse(identIter != _identMap.end());
    return identIter->second.copy();
}

uint32_t KVDBEngine::_extractPrefix(const BSONObj& config) {
    return config.getField("prefix").numberInt();
}

KVDBIdentType KVDBEngine::_extractType(const BSONObj& config) {
    return (KVDBIdentType)config.getField("type").numberInt();
}

/* End KVDBEngine */

}  // namespace mongo
