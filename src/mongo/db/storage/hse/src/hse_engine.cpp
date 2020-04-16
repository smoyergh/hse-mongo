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

#include <boost/algorithm/string.hpp>
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
using hse::OPLOG_PFX_LEN;
using hse::DEFAULT_SFX_LEN;
using hse::STDIDX_SFX_LEN;
using hse::OPLOG_FANOUT;
using hse::KVDBData;
using hse::KVDB_prefix;
using hse::DUR_LAG;

using hse_stat::KVDBStatRate;

namespace mongo {

namespace {

std::string encodePrefix(uint32_t prefix) {
    uint32_t bigEndianPrefix = endian::nativeToBig(prefix);
    return std::string(reinterpret_cast<const char*>(&bigEndianPrefix), sizeof(uint32_t));
}

void parseParams(KVDB& db, string paramStr, struct hse_params* params) {
    if (paramStr.size() == 0) {
        return;
    }

    stringstream ss1{paramStr};
    string kvStrTok{};

    while (std::getline(ss1, kvStrTok, ';')) {
        boost::trim(kvStrTok);
        stringstream ss2{kvStrTok};
        string key{};
        string val{};

        invariantHse(std::getline(ss2, key, '='));
        invariantHse(std::getline(ss2, val, '='));

        // HSE_REVISIT: remove
        LOG(1) << "CMD Params : " << key << "=" << val;

        invariantHseSt(db.kvdb_params_set(params, key, val));
    }
}
}

/* Start KVDBEngine */
const string KVDBEngine::kMainKvsName = "MainKvs";
const string KVDBEngine::kUniqIdxKvsName = "UniqIdxKvs";
const string KVDBEngine::kStdIdxKvsName = "StdIdxKvs";
const string KVDBEngine::kLargeKvsName = "LargeKvs";
const string KVDBEngine::kOplogKvsName = "OplogKvs";
const string KVDBEngine::kOplogLargeKvsName = "OplogLargeKvs";
const string KVDBEngine::kMetadataPrefix = KVDB_prefix + "meta-";

KVDBEngine::KVDBEngine(const std::string& path, bool durable, int formatVersion, bool readOnly)
    : _durable(durable), _formatVersion(formatVersion), _maxPrefix(0) {
    _setupDb();

    _loadMaxPrefix();

    _counterManager.reset(new KVDBCounterManager(kvdbGlobalOptions.getCrashSafeCounters()));
    _durabilityManager.reset(
        new KVDBDurabilityManager(_db, _durable, kvdbGlobalOptions.getForceLag()));

    if (_durable) {
        _journalFlusher = stdx::make_unique<KVDBJournalFlusher>(*_durabilityManager.get());
        _journalFlusher->go();
    }

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
    CompParms compparms = {};

    // Get the compression options from mongodb for this recordstore.
    // The options are in a BSON whose name is "hse".
    BSONObj engine = options.storageEngine.getObjectField("hse");

    if (engine.isEmpty()) {
        // If no compression option where provided at the time of the
        // collection creation, use the global options.
        Status st = collconf::collectionOptions2compParms(
            kvdbGlobalOptions.getCollectionCompressionStr(),
            kvdbGlobalOptions.getCollectionCompressionMinBytesStr(),
            compparms);
        if (!st.isOK())
            return st;

    } else {
        // The collection creation is passing some storage engine options.
        Status st = collconf::validateCollectionOptions(engine, compparms);
        if (!st.isOK())
            return st;
    }

    return _createIdent(opCtx, ident, iType, compparms, &configBuilder);
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
    struct CompParms compparms = {};


    // The compression parameters are stored in the connector metadata BSON
    // keyed with "meta-"<ident>. Get them from there.
    Status st = collconf::ident2compParms(config, compparms);
    if (!st.isOK())
        //  This binary doesn't understand how the record store was compressed
        return std::unique_ptr<RecordStore>{};

    // Determine if this recordstore is compressed or not
    hse::compressneeded(NamespaceString::oplog(ns), compparms);

    if (!colOpts.capped) {
        recordStore = stdx::make_unique<KVDBRecordStore>(
            opCtx, ns, ident, _db, _mainKvs, _largeKvs, prefix, durRef, counterRef, compparms);
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
                                                                   cappedMaxDocs,
                                                                   compparms);
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
                                                  cappedMaxSize,
                                                  compparms);

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
    CompParms compparms = {};  // Indexes are not compressed.
    KVDBIdentType iType = desc->unique() ? KVDBIdentType::UNIQINDEX : KVDBIdentType::STDINDEX;

    // let index add its own config things
    KVDBIdxBase::generateConfig(&configBuilder, _formatVersion, desc->version());
    return _createIdent(opCtx, ident, iType, compparms, &configBuilder);
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

    auto ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opCtx);

    // delete metadata
    auto s = ru->nonTxnDel(_mainKvs, keyToDel);
    if (!s.ok()) {
        return hseToMongoStatus(s);
    }

    KVDBIdentType type = _extractType(_getIdentConfig(ident));
    uint32_t prefixVal = _extractPrefix(_getIdentConfig(ident));
    string prefixStr = encodePrefix(prefixVal);
    KVDBData pKeyToDel{prefixStr};

    if (KVDBIdentType::COLL == type) {
        KVDBData dataSizeKey{KVDB_prefix + "datasize-" + ident.toString()};
        KVDBData storageSizeKey{KVDB_prefix + "storagesize-" + ident.toString()};
        KVDBData numRecordsKey{KVDB_prefix + "numrecords-" + ident.toString()};

        s = ru->nonTxnPfxDel(_mainKvs, pKeyToDel);
        if (!s.ok()) {
            return hseToMongoStatus(s);
        }
        s = ru->nonTxnPfxDel(_largeKvs, pKeyToDel);
        if (!s.ok()) {
            return hseToMongoStatus(s);
        }

        s = ru->nonTxnDel(_mainKvs, dataSizeKey);
        if (!s.ok()) {
            return hseToMongoStatus(s);
        }

        s = ru->nonTxnDel(_mainKvs, storageSizeKey);
        if (!s.ok()) {
            return hseToMongoStatus(s);
        }

        s = ru->nonTxnDel(_mainKvs, numRecordsKey);
        if (!s.ok()) {
            return hseToMongoStatus(s);
        }

        _identCollectionMap.erase(ident);
    } else if (KVDBIdentType::OPLOG == type) {
        _oplogBlkMgr->dropAllBlocks(opCtx, prefixVal);
        _identCollectionMap.erase(ident);
    } else {  // Index
        KVDBData indexSizeKey{KVDB_prefix + "indexsize-" + ident.toString()};

        if (KVDBIdentType::STDINDEX == type) {
            s = ru->nonTxnPfxDel(_stdIdxKvs, pKeyToDel);
            if (!s.ok()) {
                return hseToMongoStatus(s);
            }

            s = ru->nonTxnDel(_stdIdxKvs, indexSizeKey);
            if (!s.ok()) {
                return hseToMongoStatus(s);
            }
        } else {
            invariantHse(type == KVDBIdentType::UNIQINDEX);
            s = ru->nonTxnPfxDel(_uniqIdxKvs, pKeyToDel);
            if (!s.ok()) {
                return hseToMongoStatus(s);
            }

            s = ru->nonTxnDel(_uniqIdxKvs, indexSizeKey);
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

void KVDBEngine::_open_kvdb(const string& mp, const string& db, struct hse_params* params) {

    auto st = _db.kvdb_open(mp.c_str(), db.c_str(), params);
    if (st.getErrno()) {
        /**
         * KVDBs must be created by an application script external to
         * mongod (as root with appropriate uid/gid). This will allow
         * mongod to be run as a non-root user.
         *
         * Leaving the error handling logic intact for test programs.
         */
        if (st.getErrno() != ENOENT)
            invariantHseSt(st);

        st = _db.kvdb_make(mp.c_str(), db.c_str(), params);
        invariantHseSt(st);

        st = _db.kvdb_open(mp.c_str(), db.c_str(), params);
    }

    invariantHseSt(st);
}

void KVDBEngine::_open_kvs(const string& kvs, KVSHandle& h, struct hse_params* params) {

    auto st = _db.kvdb_kvs_open(kvs.c_str(), params, h);
    if (st.getErrno()) {
        /**
         * For EA-2, KVSes must be created by an application script,
         * external to mongod. Leaving the error handling logic
         * intact for the test programs.
         */
        if (st.getErrno() != ENOENT)
            invariantHseSt(st);

        st = _db.kvdb_kvs_make(kvs.c_str(), params);
        invariantHseSt(st);

        st = _db.kvdb_kvs_open(kvs.c_str(), params, h);
    }
    invariantHseSt(st);
}

void KVDBEngine::_set_hse_params(struct hse_params* params) {
    // get config path  string
    const string configPath = kvdbGlobalOptions.getConfigPathStr();

    // load params from config
    if (configPath != "") {
        invariantHseSt(_db.kvdb_params_from_file(params, configPath));
    }

    // get params from cmdline
    parseParams(_db, kvdbGlobalOptions.getParamsStr(), params);

    // set internal params that cannot be overridden
    // Set a long KVDB txn timeout (99 days)
    invariantHseSt(_db.kvdb_params_set(params, string("kvdb.txn_timeout"), string("0x1FFFFFFFF")));

    unsigned int ms = DUR_LAG;
    if (isDurable()) {
        if (storageGlobalParams.journalCommitIntervalMs > 0)
            ms = storageGlobalParams.journalCommitIntervalMs;
    }
    // Set KVDB c1 dur_lag to the journal commit interval.
    invariantHseSt(_db.kvdb_params_set(params, string("kvdb.dur_intvl_ms"), std::to_string(ms)));

    // applies to all kvses
    string paramName = string("kvs.pfx_len");
    invariantHseSt(_db.kvdb_params_set(params, paramName, std::to_string(DEFAULT_PFX_LEN)));

    // applies to oplog
    paramName = string("kvs.") + kOplogKvsName + string(".fanout");
    invariantHseSt(_db.kvdb_params_set(params, paramName, std::to_string(OPLOG_FANOUT)));

    paramName = string("kvs.") + kOplogKvsName + string(".pfx_len");
    invariantHseSt(_db.kvdb_params_set(params, paramName, std::to_string(OPLOG_PFX_LEN)));

    paramName = string("kvs.") + kOplogKvsName + string(".kvs_ext01");
    invariantHseSt(_db.kvdb_params_set(params, paramName, std::to_string(1)));

    // applies to oploglarge
    paramName = string("kvs.") + kOplogLargeKvsName + string(".fanout");
    invariantHseSt(_db.kvdb_params_set(params, paramName, std::to_string(OPLOG_FANOUT)));

    paramName = string("kvs.") + kOplogLargeKvsName + string(".pfx_len");
    invariantHseSt(_db.kvdb_params_set(params, paramName, std::to_string(OPLOG_PFX_LEN)));

    // applies to uniq idx
    paramName = string("kvs.") + kUniqIdxKvsName + string(".sfx_len");
    invariantHseSt(_db.kvdb_params_set(params, paramName, std::to_string(DEFAULT_SFX_LEN)));

    // applies to std idx
    paramName = string("kvs.") + kStdIdxKvsName + string(".sfx_len");
    invariantHseSt(_db.kvdb_params_set(params, paramName, std::to_string(STDIDX_SFX_LEN)));
}

void KVDBEngine::_setupDb() {
    auto st = _db.kvdb_init();
    invariantHseSt(st);

    const string mpoolName = kvdbGlobalOptions.getMpoolName();

    struct hse_params* params{nullptr};

    hse_params_create(&params);
    invariantHse(params != nullptr);

    _set_hse_params(params);

    // kvdb name and mpool name are the same for now as per the HSE API.
    _open_kvdb(mpoolName, mpoolName, params);

    _open_kvs(kMainKvsName, _mainKvs, params);
    _open_kvs(kLargeKvsName, _largeKvs, params);

    _open_kvs(kOplogLargeKvsName, _oplogLargeKvs, params);
    _open_kvs(kOplogKvsName, _oplogKvs, params);

    _open_kvs(kUniqIdxKvsName, _uniqIdxKvs, params);

    _open_kvs(kStdIdxKvsName, _stdIdxKvs, params);

    hse_params_destroy(params);
}

void KVDBEngine::_loadMaxPrefix() {
    // load ident to prefix map. also update _maxPrefix if there's any prefix bigger than
    // current _maxPrefix
    stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
    KVDBData kPrefix{(uint8_t*)kMetadataPrefix.c_str(), kMetadataPrefix.size()};
    KvsCursor* cursor;

    const struct CompParms compparms = {};  // connector metadata is not compressed.
    cursor = new KvsCursor(_mainKvs, kPrefix, true, 0, compparms);
    invariantHse(cursor != 0);

    KVDBData key{};
    KVDBData val{};
    bool eof = false;
    while (!eof) {
        cursor->read(key, val, eof);
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

    // just to be extra sure. we need this if last collection is oplog -- in that case we
    // reserve prefix+1 for oplog key tracker
    ++_maxPrefix;
}

void KVDBEngine::_cleanShutdown() {
    if (_journalFlusher) {
        _journalFlusher->shutdown();
        _journalFlusher.reset();
    }
    _durabilityManager->prepareForShutdown();
    _durabilityManager.reset();

    _counterManager->sync();
    _counterManager.reset();

    KVDBStatRate::finish();

    _db.kvdb_close();
    _db.kvdb_fini();
}

// non public api
Status KVDBEngine::_createIdent(OperationContext* opCtx,
                                StringData ident,
                                KVDBIdentType type,
                                CompParms& compparms,
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
        // Add the compression parameters in the connector metadata.
        collconf::compParms2Ident(configBuilder, compparms);

        config = std::move(configBuilder->obj());
    }

    string keyStr = kMetadataPrefix + ident.toString();
    KVDBData key{(uint8_t*)keyStr.c_str(), keyStr.size()};
    KVDBData val{(uint8_t*)config.objdata(), (unsigned long)config.objsize()};

    LOG(1) << "HSE: recording ident to kvs : " << ident.toString();
    auto ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opCtx);
    auto s = ru->nonTxnPut(_mainKvs, key, val);

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

/* Start KVDBJournalFlusher */
KVDBJournalFlusher::KVDBJournalFlusher(KVDBDurabilityManager& durabilityManager)
    : BackgroundJob(false /* deleteSelf */), _durabilityManager(durabilityManager) {}

std::string KVDBJournalFlusher::name() const {
    return "KVDBJournalFlusher";
}

void KVDBJournalFlusher::run() {
    Client::initThread(name().c_str());

    uint64_t lsync_ms, now_ms, dur_ms;
    lsync_ms = now_ms = dur_ms = 0;
    unsigned int ms = DUR_LAG;

    LOG(1) << "starting " << name() << " thread";

    if (storageGlobalParams.journalCommitIntervalMs > 0)
        ms = storageGlobalParams.journalCommitIntervalMs;

    while (!_shuttingDown.load()) {
        now_ms = std::chrono::duration_cast<milliseconds>(system_clock::now().time_since_epoch())
                     .count();

        dur_ms = (now_ms > lsync_ms) ? now_ms - lsync_ms : 0;
        if (dur_ms < ms) {
            sleepmillis(ms - dur_ms);
            continue;
        }

        try {
            lsync_ms =
                std::chrono::duration_cast<milliseconds>(system_clock::now().time_since_epoch())
                    .count();
            _durabilityManager.sync();
        } catch (const UserException& e) {
            invariantHse(e.getCode() == ErrorCodes::ShutdownInProgress);
        }
    }
    LOG(1) << "stopping " << name() << " thread";
}

void KVDBJournalFlusher::shutdown() {
    _shuttingDown.store(true);
    wait();
}

/* End KVDBJournalFlusher */
}
