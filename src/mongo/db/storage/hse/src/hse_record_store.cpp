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

#include "mongo/platform/basic.h"

#include "mongo/db/client.h"
#include "mongo/db/storage/oplog_hack.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"

#include <boost/thread/locks.hpp>


#include "hse_engine.h"
#include "hse_global_options.h"
#include "hse_kvscursor.h"
#include "hse_record_store.h"
#include "hse_stats.h"

using namespace std;

using hse::KVDBData;
using hse::KVDBRecordStoreKey;
using hse::KVDB_prefix;

using hse::VALUE_META_THRESHOLD_LEN;
using hse::VALUE_META_SIZE;
using hse::DEFAULT_PFX_LEN;
using hse::_getNumChunks;
using hse::_getValueLength;
using hse::_getValueOffset;
using hse::_cursorRead;
using hse::arrayToHexStr;

using hse_stat::_hseAppBytesReadCounter;
using hse_stat::_hseAppBytesWrittenCounter;
using hse_stat::_hseOplogCursorCreateCounter;
using hse_stat::_hseOplogCursorReadRate;

using mongo::BSONElement;
using mongo::ErrorCodes;
using mongo::BSONType;
using mongo::BSONObjBuilder;

// Collection configuration namespace
namespace collconf {

mongo::Status parseOneOption(string left, string right, CompParms& compparms) {
    std::stringstream ss;

    if (left.compare("compalgo") == 0) {
        if (right.compare("lz4") == 0)
            compparms.compalgo = CompAlgo::ALGO_LZ4;
        else if (right.compare("none") == 0)
            compparms.compalgo = CompAlgo::ALGO_NONE;
        else
            // only lz4 supported for now
            goto errout;
    } else if (left.compare("compminsize") == 0) {
        ss << right;
        if (ss.fail())
            goto errout;
        ss >> std::dec >> compparms.compminsize;
        if (ss.fail())
            goto errout;
    } else {
        // unknown option
        goto errout;
    }
    return mongo::Status::OK();

errout:
    return mongo::Status(mongo::ErrorCodes::FailedToParse, left + "=" + right);
}

// Reads/parses a list of options:
// <option name>=<option value>,
// and updates "comppparms".
//
mongo::Status collectionCfgString2compParms(const StringData& sd, struct CompParms& compparms) {
    std::stringstream ss(sd.rawData());
    std::string s;
    size_t found;

    compparms = {};
    while (std::getline(ss, s, ',')) {
        found = s.find("=");
        if (found == string::npos)
            return mongo::Status(mongo::ErrorCodes::FailedToParse, s);

        mongo::Status st =
            parseOneOption(s.substr(0, found), s.substr(found + 1, string::npos), compparms);
        if (!st.isOK())
            return st;
    }
    return mongo::Status::OK();
}

// options is coming from the create collection command line.
// option is a BSON obj with one field/element whose name is "configString"
// The value of this field is list options:
// <option name>=<option value>,
mongo::Status validateCollectionOptions(const BSONObj& options, CompParms& compparms) {

    BSONForEach(elem, options) {
        if (elem.fieldNameStringData() != "configString") {
            return {ErrorCodes::InvalidOptions,
                    mongoutils::str::stream() << '\'' << elem.fieldNameStringData() << '\''
                                              << " should be field configString"};
        }
        if (elem.type() != BSONType::String) {
            return {ErrorCodes::TypeMismatch, "'configString' must be a string."};
        }
        StringData config = elem.valueStringData();
        if (config.size() != strlen(config.rawData())) {
            return {ErrorCodes::FailedToParse, "malformed 'configString' value."};
        }
        mongo::Status st = collectionCfgString2compParms(config, compparms);
        if (!st.isOK())
            return st;
    }
    return mongo::Status::OK();
}

// Used to fail gracefully the access to a collection via a old connection binary that
// does not understand how the collection was compressed (by a newer connector binary).
#define COMPRESSION_VERSION 1

// Append into the Ident+prefix metadata key/val the compression parameters.
void compParms2Ident(BSONObjBuilder* configBuilder, CompParms& compparms) {
    configBuilder->append("compversion", COMPRESSION_VERSION);
    configBuilder->append("compalgo", static_cast<int32_t>(compparms.compalgo));
    configBuilder->append("compminsize", static_cast<int32_t>(compparms.compminsize));
}

// Get the compression parameters from the Ident+prefix metadata key/val
mongo::Status ident2compParms(const BSONObj& config, CompParms& compparms) {
    int val;

    compparms = {};  // Initialize to un compressed.

    val = config.getIntField("compversion");
    if (val == INT_MIN)
        // "compversion" field absent
        // old record store not compressed
        return mongo::Status::OK();
    if (val > COMPRESSION_VERSION)
        return mongo::Status(mongo::ErrorCodes::UnsupportedFormat,
                             "Connector compression version " +
                                 std::to_string(COMPRESSION_VERSION) +
                                 " in regards to compressed record store " + std::to_string(val));
    val = config.getIntField("compalgo");
    if (val != INT_MIN)
        compparms.compalgo = static_cast<CompAlgo>(val);
    val = config.getIntField("compminsize");
    if (val != INT_MIN)
        compparms.compminsize = val;

    return mongo::Status::OK();
}

}  // collconf end

namespace mongo {

namespace {
static const int RS_RETRIES_ON_CANCELED = 5;

bool _getKey(OperationContext* opctx,
             const struct CompParms& compparms,
             struct KVDBRecordStoreKey* key,
             const KVSHandle& baseKvs,
             const KVSHandle& chunkKvs,
             const RecordId& loc,
             KVDBData& value,
             bool use_txn) {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey chunkKey;
    hse::Status st;
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);
    unsigned int val_len;
    bool found;
    unsigned long uncTotalLen;
    unsigned int offset;
    unsigned int off_comp;  // offset from beginning first chunk where the
                            // compression headers start. May be 0 or 4.

    // read compressed first chunk
    KRSK_SET_SUFFIX(*key, loc.repr());
    KVDBData compatKey{key->data, KRSK_KEY_LEN(*key)};

    st = ru->getMCo(baseKvs, compatKey, value, found, use_txn);
    invariantHseSt(st);

    if (!found)
        return false;

    updateFraming(compparms, value);
    uncTotalLen = value.getTotalLen();
    offset = value.getOffset();
    val_len = _getValueLength(value);
    off_comp = _getValueOffset(value);

    if (val_len > VALUE_META_THRESHOLD_LEN) {
        // The value spans multiple chunks so we will read it all into a large buffer
        // If compressed, largevalue will contain the 4bytes length +
        // algo byte + leb128 bytes + compressed user value.
        hse::Status st;
        KVDBData largeValue{};

        // Allocate space and copy the first chunk just read into the larger buffer.
        largeValue.createOwned(val_len + VALUE_META_SIZE);
        st = largeValue.copy(value.data(), HSE_KVS_VLEN_MAX);
        invariantHse(st.ok());
        invariantHse(largeValue.len() == HSE_KVS_VLEN_MAX);

        uint32_t chunk = 0;

        KRSK_CLEAR(chunkKey);
        KRSK_CHUNK_COPY_MASTER(*key, chunkKey);

        while (largeValue.len() < val_len + VALUE_META_SIZE) {
            KRSK_SET_CHUNK(chunkKey, chunk);

            KVDBData compatKey{chunkKey.data, KRSK_KEY_LEN(chunkKey)};

            st = ru->getMCo(chunkKvs, compatKey, largeValue, found, use_txn);
            invariantHseSt(st);
            if (!found) {
                log() << "_getKey: key "
                      << arrayToHexStr((const char*)chunkKey.data, KRSK_KEY_LEN(chunkKey))
                      << " not found";
                invariantHse(found);
            }

            chunk++;
        }

        invariantHse(largeValue.len() == val_len + VALUE_META_SIZE);
        invariantHse(value.getNumChunks() == chunk);

        value = largeValue;
    }

    // If compressed, "value" contain the algo byte +
    // leb128 bytes + compressed user value.
    if (compparms.compdoit) {
        // Decompress the value.
        KVDBData unc{};

        hse::decompressdata(compparms, value, off_comp, unc);

        value = unc;  // No copy of the data, a move is done.
    }
    invariantHse(value.len() == offset + uncTotalLen);
    // offset below will be used to pass the value to mongo
    value.setFraming(uncTotalLen, val_len, 0, offset);

    return true;
}
}

//
// Implementation of record store classes
// ------------------------------------------------------------------------------------------
//
// See the comment about the class relationships in hse_record_store.h
//

//
// Begin Implementation of KVDBRecordStore
//

// KVDBRecordStore - Constructor / Destructor

KVDBRecordStore::KVDBRecordStore(OperationContext* ctx,
                                 StringData ns,
                                 StringData id,
                                 KVDB& db,
                                 KVSHandle& colKvs,
                                 KVSHandle& largeKvs,
                                 uint32_t prefix,
                                 KVDBDurabilityManager& durabilityManager,
                                 KVDBCounterManager& counterManager,
                                 struct CompParms& compparms)
    : RecordStore(ns),
      _db(db),
      _colKvs(colKvs),
      _largeKvs(largeKvs),
      _prefixVal(prefix),
      _durabilityManager(durabilityManager),
      _counterManager(counterManager),
      _ident(id.toString()),
      _dataSizeKey(KVDB_prefix + "datasize-" + _ident),
      _storageSizeKey(KVDB_prefix + "storagesize-" + _ident),
      _numRecordsKey(KVDB_prefix + "numrecords-" + _ident),
      _compparms(compparms) {

    _prefixValBE = htobe32(_prefixVal);

    LOG(1) << "opening collection " << ns;

    // When Mongodb rename a collection, it creates a second RecordStore (with a new namespace and
    // same
    // ident) before destroying the old one. The counters in the old recorstore needs to be flushed
    // to
    // media before loadCounters() below read them from media.

    _counterManager.sync_for_rename(_ident);
    loadCounters();

    _counterManager.registerRecordStore(this);

    RecordId lastSeenId = this->_getLastId();

    _nextIdNum.store(lastSeenId.repr() + 1);
}

KVDBRecordStore::~KVDBRecordStore() {


    if (!_overTaken) {
        // Main code path
        updateCounters();
    }
    _counterManager.deregisterRecordStore(this);

    // TODO: HSE, does this need to be protected?
    _shuttingDown = true;

    // if (_uncompressed_bytes.load())
    //    log() << "uncompressed " << _uncompressed_bytes.load() << " compressed "
    //          << _compressed_bytes.load();
}

// KVDBRecordStore - Metadata Methods

void KVDBRecordStore::_readAndDecodeCounter(const std::string& keyString,
                                            std::atomic<long long>& counter) {
    bool found;

    KVDBData key{keyString};
    KVDBData val{};
    val.createOwned(sizeof(int64_t));

    auto st = _db.kvs_get(_colKvs, 0, key, val, found);
    invariantHseSt(st);
    if (!found) {
        counter.store(0);
    } else {
        counter.store(endian::bigToNative(*(uint64_t*)val.data()));
    }
}

void KVDBRecordStore::loadCounters() {
    _readAndDecodeCounter(_numRecordsKey, _numRecords);
    _readAndDecodeCounter(_dataSizeKey, _dataSize);
    _readAndDecodeCounter(_storageSizeKey, _storageSize);
}


void KVDBRecordStore::_encodeAndWriteCounter(const std::string& keyString,
                                             std::atomic<long long>& counter) {
    uint64_t bigCtr = endian::nativeToBig(counter.load());
    string valString = std::string(reinterpret_cast<const char*>(&bigCtr), sizeof(bigCtr));
    KVDBData key{keyString};
    KVDBData val = KVDBData{valString};

    auto st = _db.kvs_put(_colKvs, key, val);
    invariantHseSt(st);
}

void KVDBRecordStore::updateCounters() {
    _encodeAndWriteCounter(_numRecordsKey, _numRecords);
    _encodeAndWriteCounter(_dataSizeKey, _dataSize);
    _encodeAndWriteCounter(_storageSizeKey, _storageSize);
}

const char* KVDBRecordStore::name() const {
    return "HSE";
};

long long KVDBRecordStore::dataSize(OperationContext* opctx) const {
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    return _dataSize.load(std::memory_order::memory_order_relaxed) +
        ru->getDeltaCounter(_dataSizeKey);
}

long long KVDBRecordStore::numRecords(OperationContext* opctx) const {
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    return _numRecords.load(std::memory_order::memory_order_relaxed) +
        ru->getDeltaCounter(_numRecordsKey);
}

int64_t KVDBRecordStore::storageSize(OperationContext* opctx,
                                     BSONObjBuilder* extraInfo,
                                     int infoLevel) const {
    // We need to make it multiple of 256 to make
    // jstests/concurrency/fsm_workloads/convert_to_capped_collection.js happy
    return static_cast<int64_t>(
        std::max(_storageSize.load() & (~255), static_cast<long long>(256)));
}

// KVDBRecordStore - CRUD-Type Methods

bool KVDBRecordStore::findRecord(OperationContext* opctx,
                                 const RecordId& loc,
                                 RecordData* out) const {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey key;

    KRSK_CLEAR(key);
    _setPrefix(&key, loc);

    return _baseFindRecord(opctx, &key, loc, out);
}

bool KVDBRecordStore::_baseFindRecord(OperationContext* opctx,
                                      struct KVDBRecordStoreKey* key,
                                      const RecordId& loc,
                                      RecordData* out) const {
    KVDBData val{};
    bool found;
    unsigned int offset;

    found = _getKey(opctx, this->_compparms, key, _colKvs, _largeKvs, loc, val, true);

    if (!found)
        return false;

    offset = val.getOffset();
    uint64_t dataLen = val.len() - offset;
    invariantHse(val.getTotalLen() == dataLen);

    // [MU_REVISIT] The value is copied from KVDBData to RecordData.
    // Avoid the copy by reading into a pre-allocated SharedBuffer.
    RecordData rd((const char*)val.data() + offset, dataLen);
    rd.makeOwned();
    *out = std::move(rd);

    _hseAppBytesReadCounter.add(dataLen);

    return true;
}

void KVDBRecordStore::deleteRecord(OperationContext* opctx, const RecordId& loc) {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey key;

    KRSK_CLEAR(key);
    KRSK_SET_PREFIX(key, KRSK_RS_PREFIX(_prefixVal));

    _baseDeleteRecord(opctx, &key, loc);
}

void KVDBRecordStore::_baseDeleteRecord(OperationContext* opctx,
                                        struct KVDBRecordStoreKey* key,
                                        const RecordId& loc) {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey chunkKey;
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);
    KRSK_SET_SUFFIX(*key, loc.repr());
    KVDBData compatKey{key->data, KRSK_KEY_LEN(*key)};

    KVDBData oldValue{};
    bool found = false;
    unsigned long _lenBytes =
        _compparms.compdoit ? 1 + VALUE_META_SIZE + MAX_BYTES_LEB128 : VALUE_META_SIZE;

    hse::Status st = ru->probeVlen(_colKvs, compatKey, oldValue, _lenBytes, found);
    invariantHseSt(st);

    // Mongo does not issue a delete on a record that doesn't exist. If the document
    // has been changed by another thread, it reestablishes the read snapshot and
    // issues a delete only if the record still exists.
    if (!found) {
        log() << "deleteRecord: key "
              << arrayToHexStr((const char*)compatKey.data(), compatKey.len()) << " not found";
        invariantHse(found);
    }

    updateFraming(this->_compparms, oldValue);
    int chunk, num_chunks = oldValue.getNumChunks();
    st = ru->del(_colKvs, compatKey);
    invariantHseSt(st);

    if (num_chunks > 0) {
        KRSK_CLEAR(chunkKey);
        KRSK_CHUNK_COPY_MASTER(*key, chunkKey);

        // Delete constituent chunks, if any.
        for (chunk = 0; chunk < num_chunks; ++chunk) {
            KRSK_SET_CHUNK(chunkKey, chunk);
            KVDBData cKey{chunkKey.data, KRSK_KEY_LEN(chunkKey)};
            st = ru->del(_largeKvs, cKey);
            invariantHseSt(st);
        }
    }

    _changeNumRecords(opctx, -1);
    _increaseDataStorageSizes(opctx, -oldValue.getTotalLen(), -oldValue.getTotalLenComp());
}

StatusWith<RecordId> KVDBRecordStore::insertRecord(OperationContext* opctx,
                                                   const char* data,
                                                   int len,
                                                   bool enforceQuota) {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey key;
    KRSK_CLEAR(key);
    KRSK_SET_PREFIX(key, KRSK_RS_PREFIX(_prefixVal));
    RecordId loc = _nextId();
    return _baseInsertRecord(opctx, &key, loc, data, len);
}


StatusWith<RecordId> KVDBRecordStore::_baseInsertRecord(OperationContext* opctx,
                                                        struct KVDBRecordStoreKey* key,
                                                        RecordId loc,
                                                        const char* data,
                                                        int len) {
    uint32_t num_chunks;
    int len_comp;

    hse::Status st = _putKey(opctx, key, loc, data, len, &num_chunks, &len_comp);
    if (st.ok()) {
        _changeNumRecords(opctx, 1);
        _increaseDataStorageSizes(opctx, len, len_comp);
    } else {
        return hseToMongoStatus(st);
    }

    _hseAppBytesWrittenCounter.add(len);

    return StatusWith<RecordId>(loc);
}

Status KVDBRecordStore::insertRecordsWithDocWriter(OperationContext* opctx,
                                                   const DocWriter* const* docs,
                                                   size_t nDocs,
                                                   RecordId* idsOut) {
    std::unique_ptr<Record[]> records(new Record[nDocs]);

    size_t totalSize = 0;
    for (size_t i = 0; i < nDocs; i++) {
        const size_t docSize = docs[i]->documentSize();
        records[i].data = RecordData(nullptr, docSize);  // We fill in the real ptr in next loop.
        totalSize += docSize;
    }

    std::unique_ptr<char[]> buffer(new char[totalSize]);
    char* pos = buffer.get();
    for (size_t i = 0; i < nDocs; i++) {
        docs[i]->writeDocument(pos);
        const size_t size = records[i].data.size();
        records[i].data = RecordData(pos, size);
        pos += size;
    }
    invariantHse(pos == (buffer.get() + totalSize));

    for (size_t i = 0; i < nDocs; ++i) {
        StatusWith<RecordId> s =
            insertRecord(opctx, records[i].data.data(), records[i].data.size(), true);
        if (!s.isOK())
            return s.getStatus();
        if (idsOut)
            idsOut[i] = s.getValue();
    }

    return Status::OK();
}

Status KVDBRecordStore::updateRecord(OperationContext* opctx,
                                     const RecordId& oldLoc,
                                     const char* data,
                                     int len,
                                     bool enforceQuota,
                                     UpdateNotifier* notifier) {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey key;

    KRSK_CLEAR(key);
    KRSK_SET_PREFIX(key, KRSK_RS_PREFIX(_prefixVal));

    return hseToMongoStatus(_baseUpdateRecord(opctx, &key, oldLoc, data, len, false, 0));
}

hse::Status KVDBRecordStore::_baseUpdateRecord(OperationContext* opctx,
                                               struct KVDBRecordStoreKey* key,
                                               const RecordId& loc,
                                               const char* data,
                                               int len,
                                               bool noLenChange,
                                               bool* lenChangeFailure) {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey innerKey;
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);
    hse::Status st;

    if (noLenChange)
        *lenChangeFailure = false;

    KRSK_SET_SUFFIX(*key, loc.repr());
    KVDBData compatKey{key->data, KRSK_KEY_LEN(*key)};

    KVDBData oldValue{};
    bool found = false;
    int new_len_comp;
    unsigned long _lenBytes =
        _compparms.compdoit ? 1 + VALUE_META_SIZE + MAX_BYTES_LEB128 : VALUE_META_SIZE;

    // getMCo() reads the first chunk and does no de-compress it (if it was
    // compressed). In the case the value required several chunks, the
    // overall length itself, placed at the beginning of the first chunk is
    // never compressed, hence i can be obtained without de-compressing the
    // first chunk.
    st = ru->probeVlen(_colKvs, compatKey, oldValue, _lenBytes, found);
    invariantHseSt(st);
    int oldLen;
    uint32_t chunk;
    unsigned int old_nchunks, new_nchunks;

    // Mongo does not issue an update on a record that doesn't exist. If the
    // document has been changed by another thread, it reestablishes the read
    // snapshot to verify the filters are still satisfied, before an update.
    if (!found) {
        log() << "updateRecord: key "
              << arrayToHexStr((const char*)compatKey.data(), compatKey.len()) << " not found";
        invariantHse(found);
    }

    updateFraming(this->_compparms, oldValue);
    oldLen = oldValue.getTotalLen();

    if (noLenChange && (len != oldLen)) {
        *lenChangeFailure = true;
        return hse::Status{EINVAL};
    }
    old_nchunks = oldValue.getNumChunks();

    st = _putKey(opctx, key, loc, data, len, &new_nchunks, &new_len_comp);
    if (!st.ok())
        return st;

    // Delete the chunks that were invalidated, if any.
    KRSK_CLEAR(innerKey);
    KRSK_CHUNK_COPY_MASTER(*key, innerKey);

    for (chunk = new_nchunks; chunk < old_nchunks; ++chunk) {
        KRSK_SET_CHUNK(innerKey, chunk);

        KVDBData cKey{innerKey.data, KRSK_KEY_LEN(innerKey)};

        st = ru->del(_largeKvs, cKey);
        invariantHseSt(st);
    }

    _increaseDataStorageSizes(opctx, len - oldLen, new_len_comp - oldValue.getTotalLenComp());

    // MU_REVISIT - updateRecord currently treated as a whole app write for accounting.
    _hseAppBytesWrittenCounter.add(len);


    return st;
}

bool KVDBRecordStore::updateWithDamagesSupported() const {
    return false;
};

StatusWith<RecordData> KVDBRecordStore::updateWithDamages(
    OperationContext* opctx,
    const RecordId& loc,
    const RecordData& oldRec,
    const char* damageSource,
    const mutablebson::DamageVector& damages) {
    MONGO_UNREACHABLE;
};

// KVDBRecordStore - Higher-Level Methods

std::unique_ptr<SeekableRecordCursor> KVDBRecordStore::getCursor(OperationContext* opctx,
                                                                 bool forward) const {
    return stdx::make_unique<KVDBRecordStoreCursor>(
        opctx, _db, _colKvs, _largeKvs, _prefixVal, forward, this->_compparms);
};

void KVDBRecordStore::waitForAllEarlierOplogWritesToBeVisible(OperationContext* txn) const {
    invariantHse(false);
}

Status KVDBRecordStore::truncate(OperationContext* opctx) {
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);
    hse::Status st;
    KVDBData prefix{(uint8_t*)&_prefixValBE, sizeof(_prefixValBE)};

    st = ru->prefixDelete(_colKvs, prefix);
    invariantHseSt(st);

    st = ru->prefixDelete(_largeKvs, prefix);
    invariantHseSt(st);

    _resetNumRecords(opctx);
    _resetDataStorageSizes(opctx);

    return Status::OK();
};

void KVDBRecordStore::temp_cappedTruncateAfter(OperationContext* opctx,
                                               RecordId end,
                                               bool inclusive) {
    invariantHse(false);
}

Status KVDBRecordStore::validate(OperationContext* opctx,
                                 ValidateCmdLevel level,
                                 ValidateAdaptor* adaptor,
                                 ValidateResults* results,
                                 BSONObjBuilder* output) {
    long long nrecords = 0;
    long long dataSizeTotal = 0;

    if (level == kValidateRecordStore || level == kValidateFull) {
        std::unique_ptr<SeekableRecordCursor> cursor = getCursor(opctx, true);
        const int interruptInterval = 4096;

        results->valid = true;
        while (boost::optional<Record> record = cursor->next()) {
            if (!(nrecords % interruptInterval))
                opctx->checkForInterrupt();
            ++nrecords;
            if (level == kValidateFull) {
                size_t dataSize;
                Status status = adaptor->validate(record->id, record->data, &dataSize);

                if (!status.isOK()) {
                    results->valid = false;
                    results->errors.push_back(str::stream() << record->id << " is corrupted");
                }
                dataSizeTotal += static_cast<long long>(dataSize);
            }
        }

        if (level == kValidateFull && results->valid) {
            long long storedNumRecords = numRecords(opctx);
            long long storedDataSize = dataSize(opctx);

            if (nrecords != storedNumRecords || dataSizeTotal != storedDataSize) {
                warning() << _ident << ": Existing record and data size counters ("
                          << storedNumRecords << " records " << storedDataSize << " bytes) "
                          << "are inconsistent with full validation results (" << nrecords
                          << " records " << dataSizeTotal << " bytes). "
                          << "Updating counters with new values.";
                if (nrecords != storedNumRecords) {
                    updateStatsAfterRepair(opctx, nrecords, dataSizeTotal);
                }
            }
        }
        output->appendNumber("nrecords", nrecords);
    } else {
        output->appendNumber("nrecords", numRecords(opctx));
    }

    return Status::OK();
}

void KVDBRecordStore::appendCustomStats(OperationContext* opctx,
                                        BSONObjBuilder* result,
                                        double scale) const {
    if (!result->hasField("capped"))
        result->appendBool("capped", false);
}

void KVDBRecordStore::updateStatsAfterRepair(OperationContext* opctx,
                                             long long numRecords,
                                             long long dataSize) {
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    invariantHse(ru->ActiveClientTxn());

    _numRecords.store(numRecords);
    _dataSize.store(dataSize);
    updateCounters();
}


// KVDBRecordStore - Protected Methods

void KVDBRecordStore::_changeNumRecords(OperationContext* opctx, int64_t amount) {
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    ru->incrementCounter(_numRecordsKey, &_numRecords, amount);
}

void KVDBRecordStore::_increaseDataStorageSizes(OperationContext* opctx,
                                                int64_t damount,
                                                int64_t samount) {
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    ru->incrementCounter(_dataSizeKey, &_dataSize, damount);
    ru->incrementCounter(_storageSizeKey, &_storageSize, samount);
}

void KVDBRecordStore::_resetNumRecords(OperationContext* opctx) {
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    ru->resetCounter(_numRecordsKey, &_numRecords);
}

void KVDBRecordStore::_resetDataStorageSizes(OperationContext* opctx) {
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    ru->resetCounter(_dataSizeKey, &_dataSize);
    ru->resetCounter(_storageSizeKey, &_storageSize);
}

// In the case the value is compressed and is larger than one chunck:
//  - the overall length of the uncompressed value is placed at the beginning
//    of the first chunk.
//  - it is followed by the compressed value.
// "len_comp" is the size of the whole value (all chunks) as placed on media. Does not includes the
// 4 bytes length
// header. If compression is on, includes the compression headers and the compressed value.
// If compression is off "len_comp" is equal to "len_unc".
hse::Status KVDBRecordStore::_putKey(OperationContext* opctx,
                                     struct KVDBRecordStoreKey* key,
                                     const RecordId& loc,
                                     const char* data_unc,
                                     const int len_unc,
                                     unsigned int* num_chunks,
                                     int* len_comp) {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey chunkKey;
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);
    const uint8_t* data;
    int len;
    KVDBData comp{};

    KRSK_SET_SUFFIX(*key, loc.repr());

    KVDBData compatKey{key->data, KRSK_KEY_LEN(*key)};

    if (this->_compparms.compdoit) {

        // Compress the value.
        hse::compressdata(this->_compparms, data_unc, len_unc, comp);
        _uncompressed_bytes += len_unc;
        _compressed_bytes += comp.len();

        // Substitute the uncompressed data with the compressed data.
        data = comp.data();
        len = comp.len();
    } else {
        data = (const uint8_t*)data_unc;
        len = len_unc;
    }
    *len_comp = len;

    if (len < VALUE_META_THRESHOLD_LEN) {
        KVDBData val{(uint8_t*)data, (unsigned long)len};
        *num_chunks = 0;

        return ru->put(_colKvs, compatKey, val);
    }

    // This value may span multiple chunks. Encode the total value length in
    // the first four bytes of the value as metadata.
    int written;
    uint32_t bigLen = endian::nativeToBig(len);
    *num_chunks = _getNumChunks(len);  // may be 0
    invariantHse(*num_chunks <= 256);

    KRSK_CLEAR(chunkKey);
    KRSK_CHUNK_COPY_MASTER(*key, chunkKey);

    string value = std::string(reinterpret_cast<const char*>(&bigLen), sizeof(uint32_t)) +
        std::string((const char*)data, VALUE_META_THRESHOLD_LEN);
    KVDBData val{value};

    hse::Status st = ru->put(_colKvs, compatKey, val);
    if (!st.ok())
        return st;

    written = VALUE_META_THRESHOLD_LEN;

    // Insert additional chunks into the large KVS. Any failure aborts the inserting transaction.
    for (uint32_t chunk = 0; chunk < *num_chunks; ++chunk) {
        KRSK_SET_CHUNK(chunkKey, chunk);

        unsigned int chunk_len = (unsigned int)len - written;
        if (chunk_len > HSE_KVS_VLEN_MAX)
            chunk_len = HSE_KVS_VLEN_MAX;

        KVDBData compatKey{chunkKey.data, KRSK_KEY_LEN(chunkKey)};
        KVDBData val{(uint8_t*)data + written, chunk_len};

        st = ru->put(_largeKvs, compatKey, val);
        if (!st.ok())
            break;

        written += chunk_len;
    }

    dassert(!st.ok() || (written == len));

    return st;
}

RecordId KVDBRecordStore::_getLastId() {
    hse::Status st;
    RecordId lastId{};

    KVDBData compatKey{(uint8_t*)&_prefixValBE, sizeof(_prefixValBE)};

    // create a reverse cursor
    KvsCursor* cursor = new KvsCursor(_colKvs, compatKey, false, 0, this->_compparms);

    // get the last element, whatever it is
    KVDBData elKey{};
    KVDBData elVal{};
    bool eof = false;

    st = cursor->read(elKey, elVal, eof);
    invariantHseSt(st);

    if (!eof) {
        lastId = _recordIdFromKey(elKey);
    } else {
        lastId = RecordId{};
    }
    delete cursor;

    return lastId;
}

RecordId KVDBRecordStore::_nextId() {
    return RecordId(_nextIdNum.fetchAndAdd(1));
}

//
// End Implementation of KVDBRecordStore
//

//
// Begin Implementation of KVDBCappedRecordStore
//

// KVDBCappedRecordStore - Constructor / Destructor

KVDBCappedRecordStore::KVDBCappedRecordStore(OperationContext* ctx,
                                             StringData ns,
                                             StringData id,
                                             KVDB& db,
                                             KVSHandle& colKvs,
                                             KVSHandle& largeKvs,
                                             uint32_t prefix,
                                             KVDBDurabilityManager& durabilityManager,
                                             KVDBCounterManager& counterManager,
                                             int64_t cappedMaxSize,
                                             int64_t cappedMaxDocs,
                                             struct CompParms& compparms)
    : KVDBRecordStore(
          ctx, ns, id, db, colKvs, largeKvs, prefix, durabilityManager, counterManager, compparms),
      _cappedMaxSize(cappedMaxSize),
      _cappedMaxSizeSlack(std::min(cappedMaxSize / 10, int64_t(16 * 1024 * 1024))),
      _cappedMaxDocs(cappedMaxDocs),
      _cappedVisMgr(new KVDBCappedVisibilityManager(*this, durabilityManager)) {

    LOG(1) << "opening capped collection " << ns;

    invariantHse(_cappedMaxSize > 0);
    invariantHse(_cappedMaxDocs == -1 || _cappedMaxDocs > 0);

    _cappedVisMgr->updateHighestSeen(this->_getLastId());
}

KVDBCappedRecordStore::~KVDBCappedRecordStore() {}

StatusWith<RecordId> KVDBCappedRecordStore::insertRecord(OperationContext* opctx,
                                                         const char* data,
                                                         int len,
                                                         bool enforceQuota) {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey key;

    if (len > _cappedMaxSize)
        return StatusWith<RecordId>(ErrorCodes::BadValue, "object to insert exceeds cappedMaxSize");

    KRSK_CLEAR(key);
    KRSK_SET_PREFIX(key, KRSK_RS_PREFIX(_prefixVal));

    RecordId loc = _cappedVisMgr->getNextAndAddUncommitted(opctx, [&]() { return _nextId(); });
    StatusWith<RecordId> result = _baseInsertRecord(opctx, &key, loc, data, len);

    if (result.isOK()) {
        int64_t removed = 0;
        Status st = cappedDeleteAsNeeded(opctx, loc, &removed);
        if (!st.isOK())
            return st;
    }

    return result;
}

Status KVDBCappedRecordStore::updateRecord(OperationContext* opctx,
                                           const RecordId& loc,
                                           const char* data,
                                           int len,
                                           bool enforceQuota,
                                           UpdateNotifier* notifier) {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey key;
    hse::Status st;
    int64_t removed = 0;

    KRSK_CLEAR(key);
    KRSK_SET_PREFIX(key, KRSK_RS_PREFIX(_prefixVal));

    st = _baseUpdateRecord(opctx, &key, loc, data, len, false, 0);

    if (!st.ok())
        return hseToMongoStatus(st);

    return cappedDeleteAsNeeded(opctx, loc, &removed);
}

std::unique_ptr<SeekableRecordCursor> KVDBCappedRecordStore::getCursor(OperationContext* opctx,
                                                                       bool forward) const {
    return stdx::make_unique<KVDBCappedRecordStoreCursor>(
        opctx, _db, _colKvs, _largeKvs, _prefixVal, forward, *_cappedVisMgr.get(), _compparms);
};

void KVDBCappedRecordStore::temp_cappedTruncateAfter(OperationContext* opctx,
                                                     RecordId end,
                                                     bool inclusive) {
    _cappedTruncateAfter(opctx, end, inclusive);
}

void KVDBCappedRecordStore::appendCustomStats(OperationContext* opctx,
                                              BSONObjBuilder* result,
                                              double scale) const {
    result->appendBool("capped", true);
    KVDBRecordStore::appendCustomStats(opctx, result, scale);
    result->appendIntOrLL("max", _cappedMaxDocs);
    result->appendIntOrLL("maxSize", _cappedMaxSize / scale);
}

void KVDBCappedRecordStore::setCappedCallback(CappedCallback* cb) {
    stdx::lock_guard<stdx::mutex> lk(_cappedCallbackMutex);

    _cappedCallback = cb;
}

bool KVDBCappedRecordStore::_needDelete(long long dataSizeDelta, long long numRecordsDelta) const {
    if (_dataSize.load() + dataSizeDelta > _cappedMaxSize)
        return true;

    if ((_cappedMaxDocs != -1) && (_numRecords.load() + numRecordsDelta > _cappedMaxDocs))
        return true;

    return false;
}

Status KVDBCappedRecordStore::cappedDeleteAsNeeded(OperationContext* opctx,
                                                   const RecordId& justInserted,
                                                   int64_t* removed) {
    long long dataSizeDelta = 0, numRecordsDelta = 0;
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    *removed = 0;
    dataSizeDelta = ru->getDeltaCounter(_dataSizeKey);
    numRecordsDelta = ru->getDeltaCounter(_numRecordsKey);

    if (!_needDelete(dataSizeDelta, numRecordsDelta))
        return Status::OK();

    return _baseCappedDeleteAsNeeded(opctx, justInserted, removed);
}

Status KVDBCappedRecordStore::_cappedDeleteCallbackHelper(OperationContext* opctx,
                                                          KVDBData& oldValue,
                                                          RecordId& newestOld) {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey key;
    int offset = 0;

    stdx::lock_guard<stdx::mutex> lk(_cappedCallbackMutex);

    if (!_cappedCallback)
        return Status::OK();

    if (oldValue.getNumChunks()) {
        // Read all chunks.

        KRSK_CLEAR(key);
        _setPrefix(&key, newestOld);

        bool found =
            _getKey(opctx, this->_compparms, &key, _colKvs, _largeKvs, newestOld, oldValue, true);
        invariantHse(found);
    }

    offset = oldValue.getOffset();
    invariantHse(oldValue.getTotalLen() == oldValue.len() - offset);

    uassertStatusOK(_cappedCallback->aboutToDeleteCapped(
        opctx,
        newestOld,
        RecordData(reinterpret_cast<const char*>(oldValue.data() + offset),
                   oldValue.getTotalLen() - offset)));

    return Status::OK();
}

Status KVDBCappedRecordStore::_baseCappedDeleteAsNeeded(OperationContext* opctx,
                                                        const RecordId& justInserted,
                                                        int64_t* removed) {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey key;
    __attribute__((aligned(16))) struct KVDBRecordStoreKey chunkKey;

    *removed = 0;

    // we do this is a sub transaction in case it aborts
    KVDBRecoveryUnit* realRecoveryUnit =
        checked_cast<KVDBRecoveryUnit*>(opctx->releaseRecoveryUnit());
    invariantHse(realRecoveryUnit);

    OperationContext::RecoveryUnitState const realRUstate = opctx->setRecoveryUnit(
        realRecoveryUnit->newKVDBRecoveryUnit(), OperationContext::kNotInUnitOfWork);

    int64_t dataSize = _dataSize.load() + realRecoveryUnit->getDeltaCounter(_dataSizeKey);
    int64_t numRecords = _numRecords.load() + realRecoveryUnit->getDeltaCounter(_numRecordsKey);

    int64_t sizeOverCap = (dataSize > _cappedMaxSize) ? dataSize - _cappedMaxSize : 0;
    int64_t sizeSaved = 0, sizeSavedComp = 0;
    int64_t docsOverCap = 0, docsRemoved = 0;
    BSONObj emptyBson;

    if (_cappedMaxDocs != -1 && numRecords > _cappedMaxDocs)
        docsOverCap = numRecords - _cappedMaxDocs;

    try {
        WriteUnitOfWork wuow(opctx);
        KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);
        KvsCursor* cursor;
        hse::Status st;
        RecordId newestOld;

        // [MU_REVISIT] - Why is this a map? We only ever iterate over it. It even has a capped
        //                size of 20000 elements - shouldn't it just be a vector.
        //
        //                Even worse, we're making a heap-allocated copy of every key that we are
        //                going to delete in the second loop.
        map<KVDBData, unsigned int> keysToDelete;
        KVDBData prefixKey{(uint8_t*)&_prefixValBE, sizeof(_prefixValBE)};

        st = ru->beginScan(_colKvs, prefixKey, true, &cursor, this->_compparms);
        invariantHseSt(st);

        while ((sizeSaved < sizeOverCap || docsRemoved < docsOverCap) && (docsRemoved < 20000)) {
            KVDBData elKey{};
            KVDBData elVal{};
            bool eof = false;
            st = cursor->read(elKey, elVal, eof);  // that updates the framing info
            invariantHseSt(st);

            if (eof)
                break;

            newestOld = _recordIdFromKey(elKey);

            if (_cappedVisMgr->isCappedHidden(
                    newestOld))  // This means we have an older record that
                break;           // hasn't been committed yet. Let's wait
                                 // until it gets committed before deleting

            if (newestOld >= justInserted)  // don't go past the record we just inserted
                break;

            if (_shuttingDown)
                break;

            ++docsRemoved;
            KVDBData oldValue = elVal;

            sizeSaved += elVal.getTotalLen();
            sizeSavedComp += elVal.getTotalLenComp();
            _cappedDeleteCallbackHelper(opctx, oldValue, newestOld);
            keysToDelete[elKey.clone()] = elVal.getNumChunks();
        }

        st = ru->endScan(cursor);
        invariantHseSt(st);

        for (auto& p : keysToDelete) {
            KVDBData k = p.first;
            unsigned int num_chunks = p.second;

            st = ru->del(_colKvs, k);
            invariantHseSt(st);

            unsigned int chunk;

            if (num_chunks > 0) {
                RecordId loc = _recordIdFromKey(k);

                KRSK_CLEAR(key);
                KRSK_SET_PREFIX(key, KRSK_RS_PREFIX(_prefixVal));
                KRSK_SET_SUFFIX(key, loc.repr());

                KRSK_CLEAR(chunkKey);
                KRSK_CHUNK_COPY_MASTER(key, chunkKey);

                // Delete constituent chunks
                for (chunk = 0; chunk < num_chunks; ++chunk) {
                    KRSK_SET_CHUNK(chunkKey, chunk);
                    KVDBData compatKey{chunkKey.data, KRSK_KEY_LEN(chunkKey)};
                    st = ru->del(_largeKvs, compatKey);
                    invariantHseSt(st);
                }
            }
        }

        if (docsRemoved > 0) {
            _changeNumRecords(opctx, -docsRemoved);
            _increaseDataStorageSizes(opctx, -sizeSaved, -sizeSavedComp);
            wuow.commit();
        }
    } catch (const WriteConflictException& wce) {
        delete opctx->releaseRecoveryUnit();
        opctx->setRecoveryUnit(realRecoveryUnit, realRUstate);
        log() << "got conflict truncating capped, ignoring";
        return Status::OK();
    } catch (...) {
        delete opctx->releaseRecoveryUnit();
        opctx->setRecoveryUnit(realRecoveryUnit, realRUstate);
        throw;
    }

    delete opctx->releaseRecoveryUnit();
    opctx->setRecoveryUnit(realRecoveryUnit, realRUstate);

    *removed = docsRemoved;

    return Status::OK();
}

void KVDBCappedRecordStore::_cappedTruncateAfter(OperationContext* opctx,
                                                 RecordId end,
                                                 bool inclusive) {
    // TODO: needs to be redone

    // copied from WiredTigerRecordStore::temp_cappedTruncateAfter()
    WriteUnitOfWork wuow(opctx);
    RecordId lastKeptId = end;
    int64_t recordsRemoved = 0;

    if (inclusive) {
        auto reverseCursor = getCursor(opctx, false);
        invariantHse(reverseCursor->seekExact(end));
        auto prev = reverseCursor->next();
        lastKeptId = prev ? prev->id : RecordId();
    }

    {
        auto cursor = getCursor(opctx, true);
        stdx::lock_guard<stdx::mutex> lk(_cappedCallbackMutex);

        for (auto record = cursor->seekExact(end); record; record = cursor->next()) {
            if (end < record->id || (inclusive && end == record->id)) {
                if (_cappedCallback) {
                    uassertStatusOK(
                        _cappedCallback->aboutToDeleteCapped(opctx, record->id, record->data));
                }
                deleteRecord(opctx, record->id);

                ++recordsRemoved;
            }
        }
    }

    wuow.commit();

    if (recordsRemoved) {
        // Forget that we've ever seen a higher timestamp than we now have.
        _cappedVisMgr->setHighestSeen(lastKeptId);
    }
}

//
// End Implementation of KVDBCappedRecordStore
//

//
// Begin Implementation of KVDBOplogStore
//

KVDBOplogStore::KVDBOplogStore(OperationContext* opctx,
                               StringData ns,
                               StringData id,
                               KVDB& db,
                               KVSHandle& colKvs,
                               KVSHandle& largeKvs,
                               uint32_t prefix,
                               KVDBDurabilityManager& durabilityManager,
                               KVDBCounterManager& counterManager,
                               int64_t cappedMaxSize,
                               struct CompParms& compparms)
    : KVDBCappedRecordStore(opctx,
                            ns,
                            id,
                            db,
                            colKvs,
                            largeKvs,
                            prefix,
                            durabilityManager,
                            counterManager,
                            cappedMaxSize,
                            -1,
                            compparms) {

    _durabilityManager.setOplogVisibilityManager(_cappedVisMgr.get());

    // oplog cleanup thread
    _opBlkMgr = 0;
    if (KVDBEngine::initOplogStoreThread(ns)) {
        // attempting to start the maintenance thread returns false if we're in repair mode
        _opBlkMgr = make_shared<KVDBOplogBlockManager>(
            opctx, _db, _colKvs, _largeKvs, _prefixVal, _cappedMaxSize);
        invariantHse(_opBlkMgr != nullptr);
    }

    _cappedVisMgr->setHighestSeen(_opBlkMgr->getHighestSeenLoc());
}

KVDBOplogStore::~KVDBOplogStore() {
    // this will be set again in the base destructor, but it's idempotent
    _shuttingDown = true;

    // [MU_REVISIT] - I suspect there might be a race condition here. The oplog background thread
    //                could run at any time after seeing that _shuttingDown is false. What happens
    //                if this destructor has completed first?
    if (_opBlkMgr) {
        _opBlkMgr->stop();
    }

    _durabilityManager.setOplogVisibilityManager(nullptr);
}

bool KVDBOplogStore::findRecord(OperationContext* opctx,
                                const RecordId& loc,
                                RecordData* out) const {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey key;

    KRSK_CLEAR(key);
    KRSK_SET_PREFIX(key, KRSK_OL_PREFIX(_prefixVal, _opBlkMgr->getBlockId(loc)));

    return _baseFindRecord(opctx, &key, loc, out);
}

void KVDBOplogStore::deleteRecord(OperationContext* opctx, const RecordId& loc) {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey key;

    KRSK_CLEAR(key);
    KRSK_SET_PREFIX(key, KRSK_OL_PREFIX(_prefixVal, _opBlkMgr->getBlockId(loc)));

    _baseDeleteRecord(opctx, &key, loc);
}

StatusWith<RecordId> KVDBOplogStore::insertRecord(OperationContext* opctx,
                                                  const char* data,
                                                  int len,
                                                  bool enforceQuota) {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey key;

    if (len > _cappedMaxSize)
        return StatusWith<RecordId>(ErrorCodes::BadValue, "object to insert exceeds cappedMaxSize");

    if (!_opBlkMgr)
        invariantHse(false);

    StatusWith<RecordId> status = oploghack::extractKey(data, len);
    if (!status.isOK())
        return status;

    const RecordId loc{status.getValue()};
    _cappedVisMgr->updateHighestSeen(loc);

    uint32_t blockId = _opBlkMgr->getBlockIdToInsertAndGrow(loc, 1, len);
    KRSK_CLEAR(key);
    KRSK_SET_PREFIX(key, KRSK_OL_PREFIX(_prefixVal, blockId));

    StatusWith<RecordId> result = _baseInsertRecord(opctx, &key, loc, data, len);

    if (result.isOK()) {
        int64_t removed = 0;
        Status st = cappedDeleteAsNeeded(opctx, loc, &removed);

        if (!st.isOK())
            return st;
    }

    return result;
}

Status KVDBOplogStore::updateRecord(OperationContext* opctx,
                                    const RecordId& loc,
                                    const char* data,
                                    int len,
                                    bool enforceQuota,
                                    UpdateNotifier* notifier) {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey key;
    hse::Status st;
    bool lenChangeFailure;
    int64_t removed = 0;

    KRSK_CLEAR(key);
    KRSK_SET_PREFIX(key, KRSK_OL_PREFIX(_prefixVal, _opBlkMgr->getBlockId(loc)));

    st = _baseUpdateRecord(opctx, &key, loc, data, len, true, &lenChangeFailure);
    if (!st.ok()) {
        if (lenChangeFailure)
            return {ErrorCodes::IllegalOperation, "Cannot change the size of a document"};
        else
            return hseToMongoStatus(st);
    }

    return cappedDeleteAsNeeded(opctx, loc, &removed);
}

std::unique_ptr<SeekableRecordCursor> KVDBOplogStore::getCursor(OperationContext* opctx,
                                                                bool forward) const {
    return stdx::make_unique<KVDBOplogStoreCursor>(opctx,
                                                   _db,
                                                   _colKvs,
                                                   _largeKvs,
                                                   _prefixVal,
                                                   forward,
                                                   *_cappedVisMgr.get(),
                                                   _opBlkMgr,
                                                   _compparms);
};

void KVDBOplogStore::waitForAllEarlierOplogWritesToBeVisible(OperationContext* opctx) const {
    _cappedVisMgr->waitForAllOplogWritesToBeVisible(opctx);
}

Status KVDBOplogStore::truncate(OperationContext* opctx) {
    if (!_opBlkMgr)
        invariantHse(false);

    Status st = _opBlkMgr->truncate(opctx);
    if (!st.isOK())
        return st;

    _resetNumRecords(opctx);
    _resetDataStorageSizes(opctx);

    return Status::OK();
}

Status KVDBOplogStore::cappedDeleteAsNeeded(OperationContext* opctx,
                                            const RecordId& justInserted,
                                            int64_t* removed) {
    return Status::OK();
}

void KVDBOplogStore::temp_cappedTruncateAfter(OperationContext* opctx,
                                              RecordId end,
                                              bool inclusive) {
    _oplogTruncateAfter(opctx, end, inclusive);
}

Status KVDBOplogStore::oplogDiskLocRegister(OperationContext* opctx, const Timestamp& opTime) {
    StatusWith<RecordId> record = oploghack::keyForOptime(opTime);
    if (record.isOK())
        _cappedVisMgr->addUncommittedRecord(opctx, record.getValue());

    return record.getStatus();
}

/**
 * Return the RecordId of an oplog entry as close to startingPosition as possible without
 * being higher. If there are no entries <= startingPosition, return RecordId().
 */
boost::optional<RecordId> KVDBOplogStore::oplogStartHack(OperationContext* opctx,
                                                         const RecordId& startingPosition) const {
    hse::Status st;

    if (!_opBlkMgr)
        invariantHse(false);

    // [MU_REVISIT] Should this cursor be able to see records that haven't persisted?

    auto ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    // Find the oplog block and do a scan on it
    uint32_t opBlk = _opBlkMgr->getBlockId(startingPosition);
    uint64_t olScanKey;

    KRSK_SET_OL_SCAN_KEY(olScanKey, _prefixVal, opBlk);

    KVDBData compatKey{(const uint8_t*)&olScanKey, sizeof(olScanKey)};
    KvsCursor* cursor = 0;
    st = ru->beginScan(_colKvs, compatKey, true, &cursor, this->_compparms);
    invariantHseSt(st);

    bool eof = false;
    RecordId lastLoc{};
    RecordId loc{};
    KVDBData elKey{};
    KVDBData elVal{};

    while (true) {
        st = _cursorRead(ru, _opBlkMgr, cursor, elKey, elVal, eof);
        invariantHseSt(st);
        if (eof)
            break;

        loc = _recordIdFromKey(elKey);
        if (loc > startingPosition)
            break;

        lastLoc = loc;
    }

    st = ru->endScan(cursor);
    invariantHseSt(st);

    if (lastLoc == RecordId(0)) {
        // TODO OPB
        // we may need to iterate backward..
        lastLoc = _opBlkMgr->getHighestFromPrevBlk(opctx, opBlk);
    }

    return lastLoc;
}

bool KVDBOplogStore::yieldAndAwaitOplogDeletionRequest(OperationContext* txn) {
    if (!_opBlkMgr)
        invariantHse(false);

    // Create another reference to the oplog stones while holding a lock on the collection to
    // prevent it from being destructed.
    std::shared_ptr<KVDBOplogBlockManager> opBlkMgr = _opBlkMgr;

    Locker* locker = txn->lockState();
    Locker::LockSnapshot snapshot;

    // Release any locks before waiting on the condition variable. It is illegal to access any
    // methods or members of this record store after this line because it could be deleted.
    bool releasedAnyLocks = locker->saveLockStateAndUnlock(&snapshot);
    invariantHse(releasedAnyLocks);

    // The top-level locks were freed, so also release any potential low-level (storage engine)
    // locks that might be held.
    txn->recoveryUnit()->abandonSnapshot();

    // Wait for an oplog deletion request, or for this record store to have been destroyed.
    opBlkMgr->awaitHasExcessBlocksOrDead();

    // Reacquire the locks that were released.
    locker->restoreLockState(snapshot);

    return !opBlkMgr->isDead();
}

void KVDBOplogStore::reclaimOplog(OperationContext* opctx) {
    if (!_opBlkMgr)
        invariantHse(false);

    while (auto block = _opBlkMgr->getOldestBlockIfExcess()) {
        invariantHse(block->highestRec.isNormal());

        LOG(1) << "Deleting Oplog Block id = " << block->blockId << " to remove approximately "
               << block->numRecs.load() << " records totaling to " << block->sizeInBytes.load()
               << " bytes";

        KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

        try {
            hse::Status st;
            WriteUnitOfWork wuow(opctx);

            st = _opBlkMgr->updateLastBlkDeleted(ru, block->blockId);
            invariantHseSt(st);

            st = _opBlkMgr->deleteBlock(ru, true, _prefixVal, *block);
            invariantHseSt(st);

            _changeNumRecords(opctx, -block->numRecs.load());
            _increaseDataStorageSizes(
                opctx, -block->sizeInBytes.load(), -block->sizeInBytes.load());

            wuow.commit();

            // Remove the stone after a successful truncation.
            _opBlkMgr->removeOldestBlock();
        } catch (const WriteConflictException& wce) {
            LOG(1) << "Caught WriteConflictException while truncating cleaning entries, retrying";
        }
    }

    LOG(1) << "Finished truncating the oplog, it now contains approximately " << _numRecords.load()
           << " records totaling to " << _dataSize.load() << " bytes";
}

KVDBOplogBlockManager* KVDBOplogStore::getOpBlkMgr() {
    return _opBlkMgr.get();
}

void KVDBOplogStore::_oplogTruncateAfter(OperationContext* opctx, RecordId end, bool inclusive) {
    RecordId lastKeptId{};
    int64_t recDel = 0;
    int64_t sizeDel = 0;

    WriteUnitOfWork wuow(opctx);

    if (!_opBlkMgr)
        invariantHse(false);

    auto st = _opBlkMgr->cappedTruncateAfter(opctx, end, inclusive, lastKeptId, recDel, sizeDel);
    invariantHse(st.isOK());

    _changeNumRecords(opctx, -recDel);
    _increaseDataStorageSizes(opctx, -sizeDel, -sizeDel);

    wuow.commit();

    if (recDel) {
        // Forget that we've ever seen a higher timestamp than we now have.
        _cappedVisMgr->setHighestSeen(lastKeptId);
    }
}

//
// End Implementation of KVDBOplogStore
//


//
// Begin Implementation of KVDBRecordStoreCursor
//

KVDBRecordStoreCursor::KVDBRecordStoreCursor(OperationContext* opctx,
                                             KVDB& db,
                                             KVSHandle& colKvs,
                                             KVSHandle& largeKvs,
                                             uint32_t prefix,
                                             bool forward,
                                             const struct CompParms& compparms)
    : _opctx(opctx),
      _db(db),
      _colKvs(colKvs),
      _largeKvs(largeKvs),
      _prefixVal(prefix),
      _forward(forward),
      _compparms(compparms) {
    _prefixValBE = htobe32(_prefixVal);
    if (_forward)
        _lastPos = RecordId(0);
    else
        _lastPos = RecordId::max();
}

KVDBRecordStoreCursor::~KVDBRecordStoreCursor() {
    _destroyMCursor();
}

void KVDBRecordStoreCursor::_reallySeek(const RecordId& id) {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey key;
    hse::Status st;

    KRSK_CLEAR(key);
    KRSK_SET_PREFIX(key, KRSK_RS_PREFIX(_prefixVal));
    KRSK_SET_SUFFIX(key, id.repr());

    KVDBData compatKey{key.data, KRSK_KEY_LEN(key)};
    KVDBData found;
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(_opctx);

    KvsCursor* mCursor = _getMCursor();
    st = ru->cursorSeek(mCursor, compatKey, &found);
    invariantHseSt(st);

    _needSeek = false;
}

boost::optional<Record> KVDBRecordStoreCursor::next() {
    if (_eof)
        return {};

    _getMCursor();
    if (_needSeek) {
        if (_forward)
            _reallySeek(RecordId(_lastPos.repr() + 1));
        else
            _reallySeek(RecordId(_lastPos.repr() - 1));
    }

    return _curr();
}

// Do a get instead of a cursor seek/read and remember the last position
// that the cursor (which need not exist) would have seeked to.
// A cursor is created (and updated) only in KVDBRecordStoreCursor::next().

boost::optional<Record> KVDBRecordStoreCursor::seekExact(const RecordId& id) {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey key;
    hse::Status st;

    KRSK_CLEAR(key);
    _setPrefix(&key, id);

    bool found = false;
    unsigned int offset;

    found = _getKey(_opctx, this->_compparms, &key, _colKvs, _largeKvs, id, _seekVal, true);
    if (!found)
        return {};

    offset = _seekVal.getOffset();
    unsigned int dataLen = _seekVal.len() - offset;
    invariantHse(_seekVal.getTotalLen() == dataLen);

    _eof = false;
    _lastPos = id;
    _needSeek = true;

    _hseAppBytesReadCounter.add(dataLen);

    return {{id, {(const char*)_seekVal.data() + offset, static_cast<int>(dataLen)}}};
}

void KVDBRecordStoreCursor::save() {}

void KVDBRecordStoreCursor::saveUnpositioned() {
    save();
}

bool KVDBRecordStoreCursor::restore() {
    // The cursor (should one exist) needs to be updated to reflect the current
    // txn being used in the recovery unit.
    _needUpdate = true;

    return true;
}

void KVDBRecordStoreCursor::detachFromOperationContext() {
    _destroyMCursor();
    _opctx = nullptr;
}

void KVDBRecordStoreCursor::reattachToOperationContext(OperationContext* opctx) {
    _opctx = opctx;
}

hse::Status KVDBRecordStoreCursor::_currCursorRead(
    KVDBRecoveryUnit* ru, KvsCursor* cursor, KVDBData& elKey, KVDBData& elVal, bool& eof) {
    return ru->cursorRead(cursor, elKey, elVal, _eof);
}

bool KVDBRecordStoreCursor::_currIsHidden(const RecordId& loc) {
    return false;
}

boost::optional<Record> KVDBRecordStoreCursor::_curr() {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey key;
    hse::Status st;

    if (_eof)
        return {};

    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(_opctx);
    KVDBData elKey{};
    KVDBData elVal{};
    bool found;
    unsigned int offset;

    st = _currCursorRead(ru, _mCursor, elKey, elVal, _eof);
    invariantHseSt(st);
    if (_eof)
        return {};

    RecordId loc = _recordIdFromKey(elKey);

    if (_currIsHidden(loc)) {
        _eof = true;

        return {};
    }

    _lastPos = loc;
    if (elVal.getNumChunks()) {
        // The value is "large", so we switch to the get interface to read its contents
        KRSK_CLEAR(key);
        _krskSetPrefixFromKey(key, elKey);
        found = _getKey(_opctx, this->_compparms, &key, _colKvs, _largeKvs, loc, _largeVal, true);
        invariantHse(found);
        elVal = _largeVal;
    }

    offset = elVal.getOffset();

    int dataLen = elVal.len() - offset;

    invariantHse(elVal.getTotalLen() == static_cast<unsigned int>(dataLen));

    _hseAppBytesReadCounter.add(dataLen);

    return {{loc, {(const char*)elVal.data() + offset, dataLen}}};
}

KvsCursor* KVDBRecordStoreCursor::_getMCursor() {
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(_opctx);
    hse::Status st;

    if (!_cursorValid) {
        KVDBData compatKey{(uint8_t*)&_prefixValBE, sizeof(_prefixValBE)};

        st = ru->beginScan(_colKvs, compatKey, _forward, &_mCursor, this->_compparms);
        invariantHseSt(st);
        _cursorValid = true;
        _needSeek =
            (_forward && (_lastPos != RecordId())) || (!_forward && (_lastPos != RecordId::max()));
    } else if (_needUpdate) {
        st = ru->cursorUpdate(_mCursor);
        invariantHseSt(st);
    }

    _needUpdate = false;

    return _mCursor;
}

void KVDBRecordStoreCursor::_destroyMCursor() {
    hse::Status st;

    if (_cursorValid) {
        KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(_opctx);

        st = ru->endScan(_mCursor);
        invariantHseSt(st);
        _mCursor = 0;
        _cursorValid = false;
    }
}

//
// End Implementation of KVDBRecordStoreCursor
//


//
// Begin Implementation of KVDBCappedRecordStoreCursor
//

KVDBCappedRecordStoreCursor::KVDBCappedRecordStoreCursor(OperationContext* opctx,
                                                         KVDB& db,
                                                         KVSHandle& colKvs,
                                                         KVSHandle& largeKvs,
                                                         uint32_t prefix,
                                                         bool forward,
                                                         KVDBCappedVisibilityManager& cappedVisMgr,
                                                         const struct CompParms& compparms)
    : KVDBRecordStoreCursor(opctx, db, colKvs, largeKvs, prefix, forward, compparms),
      _cappedVisMgr(cappedVisMgr) {}

KVDBCappedRecordStoreCursor::~KVDBCappedRecordStoreCursor() {}

// virtual
hse::Status KVDBCappedRecordStoreCursor::_currCursorRead(
    KVDBRecoveryUnit* ru, KvsCursor* cursor, KVDBData& elKey, KVDBData& elVal, bool& eof) {
    return ru->cursorRead(cursor, elKey, elVal, _eof);
}

// virtual
bool KVDBCappedRecordStoreCursor::_currIsHidden(const RecordId& loc) {
    if (_forward)
        return _cappedVisMgr.isCappedHidden(loc);
    else
        return false;
}

bool KVDBCappedRecordStoreCursor::restore() {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey key;
    hse::Status st;

    // The cursor (should one exist) needs to be updated to reflect the current
    // txn being used in the recovery unit.
    _needUpdate = true;

    if (_lastPos.isNormal()) {
        KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(_opctx);
        bool found = false;

        _packKey(&key, _prefixVal, _lastPos);

        KVDBData compatKey{key.data, KRSK_KEY_LEN(key)};

        st = ru->probeKey(_colKvs, compatKey, found);
        invariantHseSt(st);

        if (!found)
            return false;
    }

    return true;
}

void KVDBCappedRecordStoreCursor::_packKey(struct KVDBRecordStoreKey* key,
                                           uint32_t prefix,
                                           const RecordId& loc) {
    KRSK_CLEAR(*key);
    KRSK_SET_PREFIX(*key, KRSK_RS_PREFIX(_prefixVal));
    KRSK_SET_SUFFIX(*key, loc.repr());
}

//
// End Implementation of KVDBCappedRecordStoreCursor
//


//
// Begin Implementation of KVDBOplogStoreCursor
//

KVDBOplogStoreCursor::KVDBOplogStoreCursor(OperationContext* opctx,
                                           KVDB& db,
                                           KVSHandle& colKvs,
                                           KVSHandle& largeKvs,
                                           uint32_t prefix,
                                           bool forward,
                                           KVDBCappedVisibilityManager& cappedVisMgr,
                                           shared_ptr<KVDBOplogBlockManager> opBlkMgr,
                                           const struct CompParms& compparms)
    : KVDBCappedRecordStoreCursor(
          opctx, db, colKvs, largeKvs, prefix, forward, cappedVisMgr, compparms),
      _opBlkMgr{opBlkMgr} {
    if (_forward)
        _readUntilForOplog = RecordId(_cappedVisMgr.getPersistBoundary());

    _hseOplogCursorCreateCounter.add();
}

KVDBOplogStoreCursor::~KVDBOplogStoreCursor() {}

boost::optional<Record> KVDBOplogStoreCursor::seekExact(const RecordId& id) {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey key;
    hse::Status st;

    KRSK_CLEAR(key);
    KRSK_SET_PREFIX(key, KRSK_OL_PREFIX(_prefixVal, _opBlkMgr->getBlockId(id)));

    bool found = false;
    unsigned int offset;

    found = _getKey(_opctx, this->_compparms, &key, _colKvs, _largeKvs, id, _seekVal, false);
    if (!found)
        return {};

    offset = _seekVal.getOffset();
    invariantHse(_seekVal.getTotalLen() == _seekVal.len() - offset);

    _eof = false;
    _lastPos = id;
    _needSeek = true;

    return {{id, {(const char*)_seekVal.data() + offset, int(_seekVal.len() - offset)}}};
}

void KVDBOplogStoreCursor::_reallySeek(const RecordId& id) {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey key;
    __attribute__((aligned(16))) struct KVDBRecordStoreKey kmax;
    hse::Status st;

    _packKey(&key, _prefixVal, id);
    _packKey(&kmax, _prefixVal, _readUntilForOplog);

    KVDBData compatKey{key.data, KRSK_KEY_LEN(key)};
    KVDBData compatKeyMax{kmax.data, KRSK_KEY_LEN(kmax)};
    KVDBData found;
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(_opctx);

    KvsCursor* mCursor = _getMCursor();
    st = ru->oplogCursorSeek(mCursor, compatKey, (const KVDBData*)&compatKeyMax, &found);
    invariantHseSt(st);

    _needSeek = false;
}

bool KVDBOplogStoreCursor::_needReadAhead() {
    return (_hseOplogCursorReadRate.getRate() > READ_AHEAD_THRESHOLD);
}

KvsCursor* KVDBOplogStoreCursor::_getMCursor() {
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(_opctx);
    hse::Status st;

    if (!_cursorValid) {
        KVDBData compatKey{(uint8_t*)&_prefixValBE, sizeof(_prefixValBE)};
        _updateReadUntil();
        st = ru->beginOplogScan(
            _colKvs, compatKey, _forward, &_mCursor, this->_compparms, _needReadAhead());
        invariantHseSt(st);
        _cursorValid = true;
        _needSeek =
            (_forward && (_lastPos != RecordId())) || (!_forward && (_lastPos != RecordId::max()));
    } else if (_needUpdate) {
        _updateReadUntil();
        st = ru->oplogCursorUpdate(_mCursor);
        invariantHseSt(st);
    }

    _needUpdate = false;

    return _mCursor;
}

// virtual
hse::Status KVDBOplogStoreCursor::_currCursorRead(
    KVDBRecoveryUnit* ru, KvsCursor* cursor, KVDBData& elKey, KVDBData& elVal, bool& eof) {
    _hseOplogCursorReadRate.update(1);
    return _opBlkMgr->cursorRead(ru, cursor, elKey, elVal, _eof);
}

// virtual
bool KVDBOplogStoreCursor::_currIsHidden(const RecordId& loc) {
    if (_forward)
        return loc >= _readUntilForOplog;
    else
        return false;
}

void KVDBOplogStoreCursor::_updateReadUntil() {
    if (!_forward)
        return;

    // For forward oplog cursors, update the record that we can safely
    // read until (exclusive). Oplog records must be read in order and must be
    // durable. An oplog record that is still outstanding (i.e. it hasn't
    // committed or aborted) or whose durability state is unknown (i.e. not
    // known to have been persisted even if it has committed) cannot be read.

    // Query the oldest record whose persist state is unknown.  A new KVDB txn will begin
    // in ru during this cursor create/update, which can see all records
    // persisted thus far.
    _readUntilForOplog = RecordId(_cappedVisMgr.getPersistBoundary());
}

void KVDBOplogStoreCursor::_packKey(struct KVDBRecordStoreKey* key,
                                    uint32_t prefix,
                                    const RecordId& loc) {
    KRSK_CLEAR(*key);
    KRSK_SET_PREFIX(*key, KRSK_OL_PREFIX(prefix, _opBlkMgr->getBlockId(loc)));
    KRSK_SET_SUFFIX(*key, loc.repr());
}

//
// End Implementation of KVDBOplogStoreCursor
//


//
// Start Implementation of KVDBCappedVisibilityManager
//

KVDBCappedVisibilityManager::KVDBCappedVisibilityManager(KVDBCappedRecordStore& crs,
                                                         KVDBDurabilityManager& durabilityManager)
    : _crs(crs), _durabilityManager(durabilityManager), _oplog_highestSeen(RecordId()) {
    _durable = _durabilityManager.isDurable();
    _forceLag = static_cast<int64_t>(_durabilityManager.getForceLag()) << 32;
}

KVDBCappedVisibilityManager::~KVDBCappedVisibilityManager() {}

void KVDBCappedVisibilityManager::addUncommittedRecord(OperationContext* opctx,
                                                       const RecordId& record) {
    stdx::lock_guard<stdx::mutex> lk(_uncommittedRecordIdsMutex);

    _addUncommittedRecord_inlock(opctx, record);
}

void KVDBCappedVisibilityManager::_addUncommittedRecord_inlock(OperationContext* opctx,
                                                               const RecordId& record) {
    dassert(_uncommittedRecords.empty() || _uncommittedRecords.back() < record);
    SortedRecordIds::iterator it = _uncommittedRecords.insert(_uncommittedRecords.end(), record);

    opctx->recoveryUnit()->registerChange(new KVDBCappedInsertChange(_crs, *this, it));
    _oplog_highestSeen = record;
}

RecordId KVDBCappedVisibilityManager::getNextAndAddUncommitted(OperationContext* opctx,
                                                               std::function<RecordId()> nextId) {
    stdx::lock_guard<stdx::mutex> lk(_uncommittedRecordIdsMutex);
    RecordId record = nextId();

    _addUncommittedRecord_inlock(opctx, record);

    return record;
}

void KVDBCappedVisibilityManager::durableCallback(int64_t newPersistBoundary) {
    if (newPersistBoundary > _persistBoundary) {
        _uncommittedRecordIdsMutex.lock();
        if ((newPersistBoundary <= _commitBoundary) && (newPersistBoundary > _persistBoundary)) {
            // The oldest record yet to be persisted has moved forward i.e. there may be new oplog
            // records available to be read by waiting cursors (unless oplog records were removed
            // during aborts).
            _persistBoundary = newPersistBoundary;
        }
        _uncommittedRecordIdsMutex.unlock();

        _opsBecameVisibleCV.notify_all();

        // Notify any capped callback waiters (tailable oplog cursors) that there is new
        // data available.
        stdx::lock_guard<stdx::mutex> cappedCallbackLock(_crs._cappedCallbackMutex);
        if (_crs._cappedCallback) {
            _crs._cappedCallback->notifyCappedWaitersIfNeeded();
        }
    }
}

void KVDBCappedVisibilityManager::waitForAllOplogWritesToBeVisible(OperationContext* opctx) const {
    invariantHse(opctx->lockState()->isNoop() || !opctx->lockState()->inAWriteUnitOfWork());

    stdx::unique_lock<stdx::mutex> lk(_uncommittedRecordIdsMutex);
    const auto waitingFor = _oplog_highestSeen;

    opctx->waitForConditionOrInterrupt(_opsBecameVisibleCV, lk, [&] {
        return (_uncommittedRecords.empty() && (_commitBoundary == _persistBoundary)) ||
            (RecordId(_persistBoundary) > waitingFor);
    });
}

void KVDBCappedVisibilityManager::dealtWithCappedRecord(SortedRecordIds::iterator it) {
    // At the time of a transaction commit or abort, remove capped records
    // that were mutated by this transaction. They may not be durable.
    // commitBoundary tracks the smallest outstanding record (for oplog records).

    bool notify = false;

    _uncommittedRecordIdsMutex.lock();

    int64_t newBound;
    _uncommittedRecords.erase(it);
    if (!_uncommittedRecords.empty()) {
        newBound = _uncommittedRecords.front().repr();
    } else {
        newBound = _oplog_highestSeen.repr() + 1;
    }

    dassert(_commitBoundary <= newBound);
    if (_commitBoundary < newBound) {
        _commitBoundary = newBound;
        // If journaling is disabled, the journalFlusher thread doesn't run.
        // Move the _persistBoundary forward, if necessary.
        if (_crs.isOplog() && !_durable) {
            dassert(newBound > _persistBoundary);
            _persistBoundary = newBound;
            notify = true;
        }
    }

    _uncommittedRecordIdsMutex.unlock();

    if (notify) {
        _opsBecameVisibleCV.notify_all();

        // Notify any capped callback waiters (tailable oplog cursors) that there is new
        // data available.
        stdx::lock_guard<stdx::mutex> cappedCallbackLock(_crs._cappedCallbackMutex);

        if (_crs._cappedCallback)
            _crs._cappedCallback->notifyCappedWaitersIfNeeded();
    }
}

int64_t KVDBCappedVisibilityManager::getCommitBoundary() {
    return _commitBoundary;
}

int64_t KVDBCappedVisibilityManager::getPersistBoundary() {
    stdx::lock_guard<stdx::mutex> lk(_uncommittedRecordIdsMutex);

    int64_t bound;

    if (_uncommittedRecords.empty() && (_commitBoundary == _persistBoundary))
        bound = _oplog_highestSeen.repr() + 1;
    else
        bound = _persistBoundary;

    if (bound <= _forceLag)
        return 0;

    return (bound - _forceLag);
}

bool KVDBCappedVisibilityManager::isCappedHidden(const RecordId& record) const {
    // This is used only for non oplog collections.
    stdx::lock_guard<stdx::mutex> lk(_uncommittedRecordIdsMutex);

    if (_uncommittedRecords.empty())
        return false;

    return _uncommittedRecords.front() <= record;
}

void KVDBCappedVisibilityManager::updateHighestSeen(const RecordId& record) {
    if (record > _oplog_highestSeen) {
        stdx::lock_guard<stdx::mutex> lk(_uncommittedRecordIdsMutex);

        if (record > _oplog_highestSeen)
            _oplog_highestSeen = record;
    }
}

void KVDBCappedVisibilityManager::setHighestSeen(const RecordId& record) {
    // This is called during truncates to rollback oplog records.
    _uncommittedRecordIdsMutex.lock();
    _oplog_highestSeen = record;
    _commitBoundary = _persistBoundary = record.repr() + 1;
    _uncommittedRecordIdsMutex.unlock();
}

RecordId KVDBCappedVisibilityManager::getHighestSeen() {
    stdx::lock_guard<stdx::mutex> lk(_uncommittedRecordIdsMutex);

    return _oplog_highestSeen;
}

//
// End Implementation of KVDBCappedVisibilityManager
//

//
// Start Implementation of KVDBCappedInsertChange
//

KVDBCappedInsertChange::KVDBCappedInsertChange(KVDBCappedRecordStore& crs,
                                               KVDBCappedVisibilityManager& cappedVisibilityManager,
                                               SortedRecordIds::iterator it)
    : _crs(crs), _cappedVisMgr(cappedVisibilityManager), _it(it) {}

void KVDBCappedInsertChange::commit() {
    _cappedVisMgr.dealtWithCappedRecord(_it);
}

void KVDBCappedInsertChange::rollback() {
    _cappedVisMgr.dealtWithCappedRecord(_it);
    stdx::lock_guard<stdx::mutex> lk(_crs._cappedCallbackMutex);
    if (_crs._cappedCallback) {
        _crs._cappedCallback->notifyCappedWaitersIfNeeded();
    }
}

//
// End Implementation of KVDBCappedInsertChange
//
}
