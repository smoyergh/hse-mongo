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

using hse::KVDB_prefix;
using hse::KVDBData;
using hse::KVDBRecordStoreKey;

using hse::_cursorRead;
using hse::_getNumChunks;
using hse::_getValueLength;
using hse::_getValueOffset;
using hse::arrayToHexStr;
using hse::DEFAULT_PFX_LEN;
using hse::VALUE_META_SIZE;
using hse::VALUE_META_THRESHOLD_LEN;

using hse_stat::_hseAppBytesReadCounter;
using hse_stat::_hseAppBytesWrittenCounter;
using hse_stat::_hseOplogCursorCreateCounter;
using hse_stat::_hseOplogCursorReadRate;

using mongo::BSONElement;
using mongo::BSONObjBuilder;
using mongo::BSONType;
using mongo::ErrorCodes;

namespace mongo {

namespace {
static const int RS_RETRIES_ON_CANCELED = 5;

bool _getKey(OperationContext* opctx,
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

    // read compressed first chunk
    KRSK_SET_SUFFIX(*key, loc.repr());
    KVDBData compatKey{key->data, KRSK_KEY_LEN(*key)};

    st = ru->getMCo(baseKvs, compatKey, value, found, use_txn);
    invariantHseSt(st);

    if (!found)
        return false;

    val_len = _getValueLength(value);

    if (val_len > VALUE_META_THRESHOLD_LEN) {
        // The value spans multiple chunks so we will read it all into a large buffer
        // If compressed, largevalue will contain the 4bytes length +
        // algo byte + leb128 bytes + compressed user value.
        hse::Status st;
        KVDBData largeValue{};

        // Allocate space and copy the first chunk just read into the larger buffer.
        largeValue.createOwned(val_len + VALUE_META_SIZE);
        st = largeValue.copy(value.data(), HSE_KVS_VALUE_LEN_MAX);
        invariantHse(st.ok());
        invariantHse(largeValue.len() == HSE_KVS_VALUE_LEN_MAX);

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
        invariantHse(_getNumChunks(val_len) == chunk);

        value = largeValue;
    }

    return true;
}
}  // namespace

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
                                 KVSHandle& metaKvs,
                                 KVSHandle& colKvs,
                                 KVSHandle& largeKvs,
                                 uint32_t prefix,
                                 KVDBDurabilityManager& durabilityManager,
                                 KVDBCounterManager& counterManager)
    : RecordStore(ns),
      _db(db),
      _metaKvs(metaKvs),
      _colKvs(colKvs),
      _largeKvs(largeKvs),
      _prefixVal(prefix),
      _durabilityManager(durabilityManager),
      _counterManager(counterManager),
      _ident(id.toString()),
      _dataSizeKeyKvs(KVDB_prefix + "datasize-" + _ident),
      _storageSizeKeyKvs(KVDB_prefix + "storagesize-" + _ident),
      _numRecordsKeyKvs(KVDB_prefix + "numrecords-" + _ident) {

    _prefixValBE = htobe32(_prefixVal);

    LOG(1) << "opening collection " << ns;

    _dataSizeKeyID = KVDBCounterMapUniqID.fetch_add(1);
    _storageSizeKeyID = KVDBCounterMapUniqID.fetch_add(1);
    _numRecordsKeyID = KVDBCounterMapUniqID.fetch_add(1);

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

    _shuttingDown = true;
}

// KVDBRecordStore - Metadata Methods

void KVDBRecordStore::_readAndDecodeCounter(const std::string& keyString,
                                            std::atomic<long long>& counter) {
    bool found;

    KVDBData key{keyString};
    KVDBData val{};
    val.createOwned(sizeof(int64_t));

    auto st = _db.kvs_get(_metaKvs, 0, key, val, found);
    invariantHseSt(st);
    if (!found) {
        counter.store(0);
    } else {
        counter.store(endian::bigToNative(*(uint64_t*)val.data()));
    }
}

void KVDBRecordStore::loadCounters() {
    _readAndDecodeCounter(_numRecordsKeyKvs, _numRecords);
    _readAndDecodeCounter(_dataSizeKeyKvs, _dataSize);
    _readAndDecodeCounter(_storageSizeKeyKvs, _storageSize);
}


void KVDBRecordStore::_encodeAndWriteCounter(const std::string& keyString,
                                             std::atomic<long long>& counter) {
    uint64_t bigCtr = endian::nativeToBig(counter.load());
    string valString = std::string(reinterpret_cast<const char*>(&bigCtr), sizeof(bigCtr));
    KVDBData key{keyString};
    KVDBData val = KVDBData{valString};

    auto st = _db.kvs_sub_txn_put(_metaKvs, key, val);
    invariantHseSt(st);
}

void KVDBRecordStore::updateCounters() {
    _encodeAndWriteCounter(_numRecordsKeyKvs, _numRecords);
    _encodeAndWriteCounter(_dataSizeKeyKvs, _dataSize);
    _encodeAndWriteCounter(_storageSizeKeyKvs, _storageSize);
}

const char* KVDBRecordStore::name() const {
    return "HSE";
};

long long KVDBRecordStore::dataSize(OperationContext* opctx) const {
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    return _dataSize.load(std::memory_order::memory_order_relaxed) +
        ru->getDeltaCounter(_dataSizeKeyID);
}

long long KVDBRecordStore::numRecords(OperationContext* opctx) const {
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    return _numRecords.load(std::memory_order::memory_order_relaxed) +
        ru->getDeltaCounter(_numRecordsKeyID);
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

    found = _getKey(opctx, key, _colKvs, _largeKvs, loc, val, true);

    if (!found)
        return false;

    offset = _getValueOffset(val);
    uint64_t dataLen = val.len() - offset;

    // [HSE_REVISIT] The value is copied from KVDBData to RecordData.
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
    unsigned long _lenBytes = VALUE_META_SIZE;

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

    int val_len = _getValueLength(oldValue);
    int chunk, num_chunks = _getNumChunks(val_len);
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
    _increaseDataStorageSizes(opctx, -val_len, -val_len);
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

    hse::Status st = _putKey(opctx, key, loc, data, len, &num_chunks);
    if (st.ok()) {
        _changeNumRecords(opctx, 1);
        _increaseDataStorageSizes(opctx, len, len);
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
    unsigned long _lenBytes = VALUE_META_SIZE;

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

    oldLen = _getValueLength(oldValue);

    if (noLenChange && (len != oldLen)) {
        *lenChangeFailure = true;
        return hse::Status{EINVAL};
    }
    old_nchunks = _getNumChunks(oldLen);

    st = _putKey(opctx, key, loc, data, len, &new_nchunks);
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

    _increaseDataStorageSizes(opctx, len - oldLen, len - oldLen);

    // HSE_REVISIT - updateRecord currently treated as a whole app write for accounting.
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
        opctx, _db, _colKvs, _largeKvs, _prefixVal, forward);
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
    _numRecords.store(numRecords);
    _dataSize.store(dataSize);
    updateCounters();
}


// KVDBRecordStore - Protected Methods

void KVDBRecordStore::_changeNumRecords(OperationContext* opctx, int64_t amount) {
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    ru->incrementCounter(_numRecordsKeyID, &_numRecords, amount);
}

void KVDBRecordStore::_increaseDataStorageSizes(OperationContext* opctx,
                                                int64_t damount,
                                                int64_t samount) {
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    ru->incrementCounter(_dataSizeKeyID, &_dataSize, damount);
    ru->incrementCounter(_storageSizeKeyID, &_storageSize, samount);
}

void KVDBRecordStore::_resetNumRecords(OperationContext* opctx) {
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    ru->resetCounter(_numRecordsKeyID, &_numRecords);
}

void KVDBRecordStore::_resetDataStorageSizes(OperationContext* opctx) {
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    ru->resetCounter(_dataSizeKeyID, &_dataSize);
    ru->resetCounter(_storageSizeKeyID, &_storageSize);
}

hse::Status KVDBRecordStore::_putKey(OperationContext* opctx,
                                     struct KVDBRecordStoreKey* key,
                                     const RecordId& loc,
                                     const char* data,
                                     const int len,
                                     unsigned int* num_chunks) {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey chunkKey;
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    KRSK_SET_SUFFIX(*key, loc.repr());

    KVDBData compatKey{key->data, KRSK_KEY_LEN(*key)};

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
        if (chunk_len > HSE_KVS_VALUE_LEN_MAX)
            chunk_len = HSE_KVS_VALUE_LEN_MAX;

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
    KvsCursor* cursor = new KvsCursor(_colKvs, compatKey, false, 0);

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
                                             KVSHandle& metaKvs,
                                             KVSHandle& colKvs,
                                             KVSHandle& largeKvs,
                                             uint32_t prefix,
                                             KVDBDurabilityManager& durabilityManager,
                                             KVDBCounterManager& counterManager,
                                             int64_t cappedMaxSize,
                                             int64_t cappedMaxDocs)
    : KVDBRecordStore(
          ctx, ns, id, db, metaKvs, colKvs, largeKvs, prefix, durabilityManager, counterManager),
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
        opctx, _db, _colKvs, _largeKvs, _prefixVal, forward, *_cappedVisMgr.get());
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
    dataSizeDelta = ru->getDeltaCounter(_dataSizeKeyID);
    numRecordsDelta = ru->getDeltaCounter(_numRecordsKeyID);

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

    int oldValLen = _getValueLength(oldValue);
    if (_getNumChunks(oldValLen)) {
        // Read all chunks.

        KRSK_CLEAR(key);
        _setPrefix(&key, newestOld);

        bool found = _getKey(opctx, &key, _colKvs, _largeKvs, newestOld, oldValue, true);
        invariantHse(found);
    }

    offset = _getValueOffset(oldValue);

    uassertStatusOK(_cappedCallback->aboutToDeleteCapped(
        opctx,
        newestOld,
        RecordData(reinterpret_cast<const char*>(oldValue.data() + offset),
                   oldValue.len() - offset)));

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

    int64_t dataSize = _dataSize.load() + realRecoveryUnit->getDeltaCounter(_dataSizeKeyID);
    int64_t numRecords = _numRecords.load() + realRecoveryUnit->getDeltaCounter(_numRecordsKeyID);

    int64_t sizeOverCap = (dataSize > _cappedMaxSize) ? dataSize - _cappedMaxSize : 0;
    int64_t sizeSaved = 0;
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

        // [HSE_REVISIT] - Why is this a map? We only ever iterate over it. It even has a capped
        //                size of 20000 elements - shouldn't it just be a vector.
        //
        //                Even worse, we're making a heap-allocated copy of every key that we are
        //                going to delete in the second loop.
        map<KVDBData, unsigned int> keysToDelete;
        KVDBData prefixKey{(uint8_t*)&_prefixValBE, sizeof(_prefixValBE)};

        st = ru->beginScan(_colKvs, prefixKey, true, &cursor);
        invariantHseSt(st);

        while ((sizeSaved < sizeOverCap || docsRemoved < docsOverCap) && (docsRemoved < 20000)) {
            KVDBData elKey{};
            KVDBData elVal{};
            bool eof = false;
            st = cursor->read(elKey, elVal, eof);
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

            sizeSaved += _getValueLength(elVal);
            _cappedDeleteCallbackHelper(opctx, oldValue, newestOld);
            keysToDelete[elKey.clone()] = _getNumChunks(_getValueLength(elVal));
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
            _increaseDataStorageSizes(opctx, -sizeSaved, -sizeSaved);
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
                               KVSHandle& metaKvs,
                               KVSHandle& colKvs,
                               KVSHandle& largeKvs,
                               uint32_t prefix,
                               KVDBDurabilityManager& durabilityManager,
                               KVDBCounterManager& counterManager,
                               int64_t cappedMaxSize)
    : KVDBCappedRecordStore(opctx,
                            ns,
                            id,
                            db,
                            metaKvs,
                            colKvs,
                            largeKvs,
                            prefix,
                            durabilityManager,
                            counterManager,
                            cappedMaxSize,
                            -1) {

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

    // [HSE_REVISIT] - I suspect there might be a race condition here. The oplog background thread
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
    return stdx::make_unique<KVDBOplogStoreCursor>(
        opctx, _db, _colKvs, _largeKvs, _prefixVal, forward, *_cappedVisMgr.get(), _opBlkMgr);
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

    // [HSE_REVISIT] Should this cursor be able to see records that haven't persisted?

    auto ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    // Find the oplog block and do a scan on it
    uint32_t opBlk = _opBlkMgr->getBlockId(startingPosition);
    uint64_t olScanKey;

    KRSK_SET_OL_SCAN_KEY(olScanKey, _prefixVal, opBlk);

    KVDBData compatKey{(const uint8_t*)&olScanKey, sizeof(olScanKey)};
    KvsCursor* cursor = 0;
    st = ru->beginScan(_colKvs, compatKey, true, &cursor);
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
                                             bool forward)
    : _opctx(opctx),
      _db(db),
      _colKvs(colKvs),
      _largeKvs(largeKvs),
      _prefixVal(prefix),
      _forward(forward) {
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

    return _curr(true);
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

    found = _getKey(_opctx, &key, _colKvs, _largeKvs, id, _seekVal, true);
    if (!found)
        return {};

    offset = _getValueOffset(_seekVal);
    unsigned int dataLen = _seekVal.len() - offset;

    _eof = false;
    _lastPos = id;
    _needSeek = true;

    KVDBStatCounterRollup(_hseAppBytesReadCounter, dataLen, 8);

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

boost::optional<Record> KVDBRecordStoreCursor::_curr(bool use_txn) {
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
    int valLen = _getValueLength(elVal);
    if (_getNumChunks(valLen)) {
        // The value is "large", so we switch to the get interface to read its contents
        KRSK_CLEAR(key);
        _krskSetPrefixFromKey(key, elKey);
        found = _getKey(_opctx, &key, _colKvs, _largeKvs, loc, _largeVal, use_txn);
        invariantHse(found);
        elVal = _largeVal;
    }

    offset = _getValueOffset(elVal);

    int dataLen = elVal.len() - offset;

    invariantHse(_getValueLength(elVal) == static_cast<unsigned int>(dataLen));

    _hseAppBytesReadCounter.add(dataLen);

    return {{loc, {(const char*)elVal.data() + offset, dataLen}}};
}

KvsCursor* KVDBRecordStoreCursor::_getMCursor() {
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(_opctx);
    hse::Status st;

    if (!_cursorValid) {
        KVDBData compatKey{(uint8_t*)&_prefixValBE, sizeof(_prefixValBE)};

        st = ru->beginScan(_colKvs, compatKey, _forward, &_mCursor);
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
                                                         KVDBCappedVisibilityManager& cappedVisMgr)
    : KVDBRecordStoreCursor(opctx, db, colKvs, largeKvs, prefix, forward),
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

// Note that oplog store cursors behave differently from other record store cursors.
// In particular, their view isn't identical to that of a transaction in its opctx.
// They must be able to see all records persisted < _oplogReadUntil.
// 1) Oplog cursors are unbound cursors that can see all records persisted till the
//    point in time when they were created or last updated.
// 2) All read operations in the context of an oplog cursor must be unbound.
//    A transaction in the same opctx may not be able to see keys that the cursor should.
// 3) Forward cursors are subject to visibility rules such that they can read records
//    (keys are oplog timestamps) that are known to have been committed and persisted
//    with no holes in between. Read _oplogReadUntil *before* updating a cursor's view.
// 4) From 3), an oplog store cursor can't see a transaction's own in flight mutations.
// 5) Note that WT uses oplog cursors bound to the active transaction in its recovery unit.
//    To ensure that it can see all commits in its cursor read snapshot, it throws a
//    WCE if there is an active txn and it's not the only thread writing (if it is the
//    only thread writing, it can be assured its snapshot isn't missing commits from other
//    writing threads).
// In our implementation, the cursor runs unbound and is decoupled from an opctx's transaction.
KVDBOplogStoreCursor::KVDBOplogStoreCursor(OperationContext* opctx,
                                           KVDB& db,
                                           KVSHandle& colKvs,
                                           KVSHandle& largeKvs,
                                           uint32_t prefix,
                                           bool forward,
                                           KVDBCappedVisibilityManager& cappedVisMgr,
                                           shared_ptr<KVDBOplogBlockManager> opBlkMgr)
    : KVDBCappedRecordStoreCursor(opctx, db, colKvs, largeKvs, prefix, forward, cappedVisMgr),
      _readUntilForOplog(RecordId()),
      _opBlkMgr{opBlkMgr} {
    _hseOplogCursorCreateCounter.add();
}

KVDBOplogStoreCursor::~KVDBOplogStoreCursor() {}

bool KVDBOplogStoreCursor::restore() {
    // The cursor (should one exist) needs to be updated to the latest read snapshot.
    _needUpdate = true;

    // An oplog cursor must be able to see everything committed so far. Use an unbound get.
    // There may already be an active txn in this recovery unit. Do not bind to it.
    // Check whether the key we seeked to last is still present.
    if (_lastPos.isNormal()) {
        if (!seekExact(_lastPos))
            return false;
    }

    return true;
}

boost::optional<Record> KVDBOplogStoreCursor::seekExact(const RecordId& id) {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey key;
    hse::Status st;

    KRSK_CLEAR(key);
    KRSK_SET_PREFIX(key, KRSK_OL_PREFIX(_prefixVal, _opBlkMgr->getBlockId(id)));

    bool found = false;
    unsigned int offset;

    // An oplog cursor must be able to see everything committed so far. Use an unbound get.
    // There may already be an active txn in this recovery unit. Do not bind to it.
    found = _getKey(_opctx, &key, _colKvs, _largeKvs, id, _seekVal, false);
    if (!found)
        return {};

    offset = _getValueOffset(_seekVal);
    invariantHse(_getValueLength(_seekVal) == _seekVal.len() - offset);

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

boost::optional<Record> KVDBOplogStoreCursor::next() {
    if (_eof)
        return {};

    // [HSE_REVISIT] Note that oplog cursor creation is deferred until next().
    // This may mean that an optime returned by seekExact (unbound get) is no
    // longer present in the newly created cursor read snapshot. Later optimes
    // may also have been deleted and we could end up reading past them. We
    // shouldn't be deferring oplog cursor creation to next() and should use
    // the same unbound cursor for all seekExact and next() operations.
    // At the moment, that causes an OOM failure in HSE cursor create.
    // Alternatively, we could seek to _lastPos here, confirm _lastPos still exists
    // in the cursor snapshot before returning the next record.
    _getMCursor();
    if (_needSeek) {
        if (_forward)
            _reallySeek(RecordId(_lastPos.repr() + 1));
        else
            _reallySeek(RecordId(_lastPos.repr() - 1));
    }

    // Note that this cursor may use the get interface to read large values.
    // An oplog cursor must be able to see everything committed so far.
    // It must set use_txn to false and use unbound gets in order for it to be able to
    // see all the values committed so far in time. There may already be an active txn
    // in this recovery unit. Do not bind to it. We don't know what the txn can see.
    return _curr(false);
}

KvsCursor* KVDBOplogStoreCursor::_getMCursor() {
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(_opctx);
    hse::Status st;

    if (!_cursorValid) {
        KVDBData compatKey{(uint8_t*)&_prefixValBE, sizeof(_prefixValBE)};
        _updateReadUntil();

        // We must update _oplogReadUntil before we create or update a cursor.
        // An oplog cursor must be unbound in order to see everything committed so far.
        st = ru->beginOplogScan(_colKvs, compatKey, _forward, &_mCursor);
        invariantHseSt(st);
        _cursorValid = true;
        _needSeek =
            (_forward && (_lastPos != RecordId())) || (!_forward && (_lastPos != RecordId::max()));
    } else if (_needUpdate) {
        _updateReadUntil();

        // We must update _oplogReadUntil before we create or update a cursor.
        // An oplog cursor must be unbound in order to see everything committed so far.
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

    // Query the oldest record whose persist state is unknown.
    // This function must only be called before either creating or
    // updating an unbound cursor. That ensures the newly created or updated
    // unbound cursor can see everything persisted so far in time.
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
}  // namespace mongo
