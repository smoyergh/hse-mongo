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

#include <cstdlib>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "mongo/base/checked_cast.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/storage/index_entry_comparison.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

#include "hse_impl.h"
#include "hse_index.h"


namespace mongo {
using std::string;
using std::stringstream;
using std::vector;

namespace {
static const int kKeyStringV0Version = 0;
static const int kKeyStringV1Version = 1;
static const int kMinimumIndexVersion = kKeyStringV0Version;
static const int kMaximumIndexVersion = kKeyStringV1Version;

/**
 * Strips the field names from a BSON object
 */
BSONObj stripFieldNames(const BSONObj& obj, int& count) {
    BSONObjBuilder b;
    BSONObjIterator i(obj);

    int num_fields = 0;
    while (i.more()) {
        BSONElement e = i.next();
        b.appendAs(e, "");
        num_fields++;
    }

    count = num_fields;
    return b.obj();
}

string dupKeyError(const BSONObj& key) {
    stringstream ss;
    ss << "E11000 duplicate key error ";
    ss << "dup key: " << key.toString();
    return ss.str();
}

const int kTempKeyMaxSize = 1024;  // Do the same as the heap implementation

Status checkKeySize(const BSONObj& key) {
    if (key.objsize() >= kTempKeyMaxSize) {
        string msg = mongoutils::str::stream()
            << "hse_index::insert: key too large to index, failing " << ' ' << key.objsize() << ' '
            << key;
        return Status(ErrorCodes::KeyTooLong, msg);
    }
    return Status::OK();
}

string stripPrefix(KVDBData& key, const string& prefix) {
    string fullKey((const char*)key.data(), key.len());

    return fullKey.substr(prefix.size(), string::npos);
}

string makePrefixedKey(const string& prefix, const KeyString& encodedKey) {
    string key(prefix);
    key.append(encodedKey.getBuffer(), encodedKey.getSize());
    return key;
}
}

/* Start KVDBIdxCursorBase */
KVDBIdxCursorBase::KVDBIdxCursorBase(OperationContext* opctx,
                                     KVSHandle& idxKvs,
                                     std::string prefix,
                                     bool forward,
                                     Ordering order,
                                     KeyString::Version keyStringVersion,
                                     int numFields)
    : _idxKvs(idxKvs),
      _prefix(prefix),
      _forward(forward),
      _order(order),
      _keyStringVersion(keyStringVersion),
      _key(keyStringVersion),
      _typeBits(keyStringVersion),
      _query(keyStringVersion),
      _seekPosIncl(keyStringVersion),
      _endPosIncl(keyStringVersion),
      _numFields(numFields),
      _opctx(opctx) {}

boost::optional<IndexKeyEntry> KVDBIdxCursorBase::next(RequestedInfo parts) {
    // Advance on a cursor at the end is a no-op
    if (_eof) {
        return {};
    }

    // If the last seek resolved to a point get, check whether this is still a
    // point query and if we need a cursor on an update.
    if (_lastPointGet && (_seekPosIncl == _endPosIncl) &&
        !(_needUpdate && _needCursorAfterUpdate())) {
        _eof = true;
        return {};
    }

    _lastPointGet = false;
    _advanceCursor();
    _updatePosition();

    return _curr(parts);
}

void KVDBIdxCursorBase::setEndPosition(const BSONObj& key, bool inclusive) {
    if (key.isEmpty()) {
        // This means scan to end of index.
        _endPosition.reset();
        _endPosIncl.resetToEmpty();
        return;
    }

    int cnt = 0;
    const BSONObj newkey = stripFieldNames(key, cnt);

    // NOTE: this uses the opposite rules as a normal seek because a forward scan should
    // end after the key if inclusive and before if exclusive.
    const auto discriminator =
        _forward == inclusive ? KeyString::kExclusiveAfter : KeyString::kExclusiveBefore;
    _endPosition = stdx::make_unique<KeyString>(_keyStringVersion);
    _endPosition->resetToKey(newkey, _order, discriminator);

    // Cache the keyString using the standard inclusive discriminator.
    // This is used to compare against the last sought to position,
    // to determine whether we can use point gets (is seek pos equal to end pos?)
    // Since _endPosition uses the exact opposite discriminator byte as that used by
    // _query, a direct comparison between the two always reports they are not equal.
    // So we store _seekPosIncl and _endPosIncl versions which use the inclusive
    // discriminator byte and compare _seekPosIncl with _endPosIncl.
    _endPosIncl.resetToKey(newkey, _order);
}

boost::optional<IndexKeyEntry> KVDBIdxCursorBase::_seek(const BSONObj& key,
                                                        int nfields,
                                                        bool inclusive,
                                                        RequestedInfo parts) {
    _eof = false;

    // Cache the keyString using the standard inclusive discriminator.
    // This is used to compare against the end position,
    // to determine whether we can use point gets (is seek pos equal to end pos?)
    // Since _endPosition uses the exact opposite discriminator byte as that used by
    // _query, a direct comparison between the two always reports they are not equal.
    // So we store _seekPosIncl and _endPosIncl versions which use the inclusive
    // discriminator byte and compare _seekPosIncl with _endPosIncl.
    _seekPosIncl.resetToKey(key, _order);

    bool needCursor = !inclusive || (_seekPosIncl != _endPosIncl);

    if (!needCursor) {
        // Compute the number of fields. Note that this iterates over the BSONObj.
        if (nfields == 0)
            nfields = key.nFields();

        // We may get a prefix index query over <field1, field2> when the index is
        // configured on <field1, field2, field3>. Can't substitute a point get then.
        // If this BSONObj includes all fields configured in the index, use a point get.
        // _numFields is 0 only for unit tests.
        if (nfields == _numFields || !_numFields) {
            auto result = _pointGet(key, parts, needCursor);
            if (!needCursor) {
                _needSeek = true;
                _lastPointGet = true;
                return result;
            }
        }
    }

    // Must use a cursor.
    invariantHse(needCursor);
    _lastPointGet = false;

    const auto discriminator =
        _forward == inclusive ? KeyString::kExclusiveBefore : KeyString::kExclusiveAfter;

    // By using a discriminator other than kInclusive, there is no need to distinguish
    // unique vs non-unique key formats since both start with the key.
    _query.resetToKey(key, _order, discriminator);

    _ensureCursor();
    _seekCursor(_query);
    _updatePosition();

    return _curr(parts);
}

boost::optional<IndexKeyEntry> KVDBIdxCursorBase::seek(const BSONObj& key,
                                                       bool inclusive,
                                                       RequestedInfo parts) {
    int cnt = 0;
    const BSONObj newkey = stripFieldNames(key, cnt);

    return _seek(newkey, cnt, inclusive, parts);
}

boost::optional<IndexKeyEntry> KVDBIdxCursorBase::seek(const IndexSeekPoint& seekPoint,
                                                       RequestedInfo parts) {
    // make a key representing the location to which we want to advance.
    return _seek(IndexEntryComparison::makeQueryObject(seekPoint, _forward), 0, true, parts);
}

void KVDBIdxCursorBase::save() {}

void KVDBIdxCursorBase::saveUnpositioned() {
    save();
}

void KVDBIdxCursorBase::restore() {
    _needUpdate = true;
}

void KVDBIdxCursorBase::detachFromOperationContext() {
    _destroyMCursor();
    _opctx = nullptr;
}

void KVDBIdxCursorBase::reattachToOperationContext(OperationContext* opctx) {
    _opctx = opctx;
    // iterator recreated in restore()
}

void KVDBIdxCursorBase::_ensureCursor() {
    auto ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(_opctx);

    if (!_cursorValid) {
        KVDBData pKey{(const uint8_t*)_prefix.c_str(), _prefix.size()};
        auto hseSt = ru->beginScan(_idxKvs, pKey, _forward, &_cursor);
        invariantHseSt(hseSt);
        _cursorValid = true;
        _needSeek = true;
        _eof = false;
    } else if (_needUpdate) {
        auto hseSt = ru->cursorUpdate(_cursor);
        invariantHseSt(hseSt);
    }
    _needUpdate = false;
}

void KVDBIdxBase::loadCounter() {
    bool found = false;
    KVDBData key{_indexSizeKeyKvs};
    KVDBData val{};
    val.createOwned(sizeof(int64_t));

    auto st = _db.kvs_get(_idxKvs, 0, key, val, found);
    invariantHseSt(st);

    if (!found) {
        _indexSize.store(0);
    } else {
        _indexSize.store(endian::bigToNative(*(uint64_t*)val.data()));
    }
}

void KVDBIdxBase::updateCounter() {
    uint64_t bigCtr = endian::nativeToBig(_indexSize.load());
    string valString = std::string(reinterpret_cast<const char*>(&bigCtr), sizeof(bigCtr));
    KVDBData key{_indexSizeKeyKvs};
    KVDBData val = KVDBData{valString};

    auto st = _db.kvs_sub_txn_put(_idxKvs, key, val);
    invariantHseSt(st);
}

void KVDBIdxBase::incrementCounter(KVDBRecoveryUnit* ru, int size) {
    ru->incrementCounter(_indexSizeKeyID, &_indexSize, size);
}

void KVDBIdxCursorBase::_destroyMCursor() {
    if (_cursorValid) {
        auto ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(_opctx);
        auto hseSt = ru->endScan(_cursor);
        invariantHseSt(hseSt);
        _cursorValid = false;
    }
}

void KVDBIdxCursorBase::_advanceCursor() {
    if (_eof) {
        return;
    }

    _ensureCursor();

    auto ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(_opctx);
    bool eof = false;

    if (_needSeek) {
        // last operation was a point get, so reposition to what comes next
        // _key is the last thing we sought, so use that here.
        // It includes loc for standard index.
        // Then read and discard the value (already returned by point get).
        // The next read gets the "next" value.

        KVDBData k, v;  // local, will be discarded
        KVDBData found;

        string prefixedQuery = makePrefixedKey(_prefix, _key);
        KVDBData pQry{(const uint8_t*)prefixedQuery.c_str(), prefixedQuery.size()};
        auto hseSt = ru->cursorSeek(_cursor, pQry, &found);
        invariantHseSt(hseSt);

        if (found == pQry) {
            auto hseSt = ru->cursorRead(_cursor, k, v, eof);
            invariantHseSt(hseSt);
            invariantHse(k == pQry);
        }

        _needSeek = false;
    }

    auto hseSt = ru->cursorRead(_cursor, _mKey, _mVal, eof);
    invariantHseSt(hseSt);
    if (eof) {
        _eof = true;
        hseSt = hse::Status{};
    }
}

void KVDBIdxCursorBase::_updatePosition() {
    if (_eof) {
        _loc = RecordId();
        return;
    }

    auto key = stripPrefix(_mKey, _prefix);
    _key.resetFromBuffer(key.data(), key.size());

    // _endPosition doesn't contain a loc.
    if (_endPosition) {
        int cmp = _key.compare(*_endPosition);
        if (_forward ? cmp > 0 : cmp < 0) {
            _eof = true;
            return;
        }
    }

    _updateLocAndTypeBits();
}


boost::optional<IndexKeyEntry> KVDBIdxCursorBase::_curr(RequestedInfo parts) const {
    if (_eof) {
        return {};
    }

    BSONObj bson;
    if (parts & kWantKey) {
        // KeyString::toBson only reads upto kEnd and ignores loc bytes.
        bson = KeyString::toBson(_key.getBuffer(), _key.getSize(), _order, _typeBits);
    }

    return {{std::move(bson), _loc}};
}

void KVDBIdxCursorBase::_seekCursor(const KeyString& query) {
    auto ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(_opctx);

    string prefixedQuery = makePrefixedKey(_prefix, query);

    KVDBData pQry{(uint8_t*)prefixedQuery.c_str(), prefixedQuery.size()};

    auto hseSt = ru->cursorSeek(_cursor, pQry, nullptr);
    invariantHseSt(hseSt);

    bool eof = false;
    _eof = false;
    hseSt = ru->cursorRead(_cursor, _mKey, _mVal, eof);
    invariantHseSt(hseSt);
    if (eof) {
        _eof = true;
    }

    _needSeek = false;
}


KVDBIdxCursorBase::~KVDBIdxCursorBase() {
    _destroyMCursor();
}

/* End KVDBIdxCursorBase */

/* Start KVDBIdxStdCursor */

KVDBIdxStdCursor::KVDBIdxStdCursor(OperationContext* opctx,
                                   KVSHandle& idxKvs,
                                   std::string prefix,
                                   bool forward,
                                   Ordering order,
                                   KeyString::Version keyStringVersion,
                                   int numFields)
    : KVDBIdxCursorBase(opctx, idxKvs, prefix, forward, order, keyStringVersion, numFields) {}

KVDBIdxStdCursor::~KVDBIdxStdCursor() {}

void KVDBIdxStdCursor::_updateLocAndTypeBits() {
    const char* buffer = _key.getBuffer();
    const uint8_t* first = (uint8_t*)(buffer + _key.getSize() - sizeof(int64_t));
    uint64_t repr = 0;
    for (int i = 0; i < 8; i++) {
        repr = (repr << 8) | *(first + i);
    }

    _loc = RecordId(repr);
    dassert(_loc.isNormal());
    BufReader br(_mVal.data(), _mVal.len());
    _typeBits.resetFromBuffer(&br);
}

boost::optional<IndexKeyEntry> KVDBIdxStdCursor::_pointGet(const BSONObj& key,
                                                           RequestedInfo parts,
                                                           bool& needCursor) {
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(_opctx);
    hse_kvs_pfx_probe_cnt found;
    std::string pkey(_prefix);

    _query.resetToKey(key, _order);
    pkey.append(_query.getBuffer(), _query.getSize());
    KVDBData pfx{(const uint8_t*)pkey.c_str(), pkey.size()};

    _mKey.createOwned(HSE_KVS_KLEN_MAX);
    _mVal.createOwned(KeyString::TypeBits::kMaxBytesNeeded + 1);

    auto st = ru->prefixGet(_idxKvs, pfx, _mKey, _mVal, found);
    invariantHseSt(st);

    if (found == HSE_KVS_PFX_FOUND_ZERO) {
        needCursor = false;
        _eof = true;
        _updatePosition();
        return boost::none;
    } else if (found == HSE_KVS_PFX_FOUND_ONE) {
        needCursor = false;
        _updatePosition();
        return _curr(parts);
    } else {
        invariantHse(found == HSE_KVS_PFX_FOUND_MUL);
        needCursor = true;
        return boost::none;
    }
}

bool KVDBIdxStdCursor::_needCursorAfterUpdate() {
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(_opctx);
    hse_kvs_pfx_probe_cnt found;
    std::string pkey(_prefix);
    pkey.append(_query.getBuffer(), _query.getSize());

    KVDBData pfx{(const uint8_t*)pkey.c_str(), pkey.size()};
    KVDBData k, v;
    k.createOwned(HSE_KVS_KLEN_MAX);

    auto st = ru->prefixGet(_idxKvs, pfx, k, v, found);
    invariantHseSt(st);

    if (found == HSE_KVS_PFX_FOUND_ZERO) {
        // After an update, we don't have any matches. We're at eof.
        return false;
    } else if (found == HSE_KVS_PFX_FOUND_ONE) {
        // After an update, check whether this is the same loc as last time.
        // If it is, we're at eof. If not, need to create a cursor.
        return !(k == _mKey);
    } else {
        // After an update, there are multiple locs. Need a cursor.
        invariantHse(found == HSE_KVS_PFX_FOUND_MUL);
        return true;
    }
}

/* End KVDBIdxStdCursor */

/* Start KVDBIdxUniqCursor */

KVDBIdxUniqCursor::KVDBIdxUniqCursor(OperationContext* opctx,
                                     KVSHandle& idxKvs,
                                     std::string prefix,
                                     bool forward,
                                     Ordering order,
                                     KeyString::Version keyStringVersion,
                                     int numFields)
    : KVDBIdxCursorBase(opctx, idxKvs, prefix, forward, order, keyStringVersion, numFields) {}

KVDBIdxUniqCursor::~KVDBIdxUniqCursor() {}

boost::optional<IndexKeyEntry> KVDBIdxUniqCursor::_pointGet(const BSONObj& key,
                                                            RequestedInfo parts,
                                                            bool& needCursor) {
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(_opctx);
    bool found;
    std::string pkey(_prefix);

    needCursor = false;

    _query.resetToKey(key, _order);
    pkey.append(_query.getBuffer(), _query.getSize());
    _mKey = KVDBData(pkey).clone();

    auto st = ru->getMCo(_idxKvs, _mKey, _mVal, found);
    invariantHseSt(st);

    if (!found) {
        _eof = true;
        _updatePosition();
        return boost::none;
    }

    // _mKey + _mVal now have allocated memory
    _updatePosition();
    return _curr(parts);
}

bool KVDBIdxUniqCursor::_needCursorAfterUpdate() {
    return false;
}

boost::optional<IndexKeyEntry> KVDBIdxUniqCursor::seekExact(const BSONObj& key,
                                                            RequestedInfo parts) {
    Status s = checkKeySize(key);
    if (!s.isOK()) {
        return boost::none;
    }

    bool needCursor;
    int cnt = 0;
    auto finalKey = stripFieldNames(key, cnt);

    _eof = false;
    _lastPointGet = true;
    _needSeek = true;

    // Cache the keyString using the standard inclusive discriminator.
    // This is used to compare against the end position,
    // to determine whether we can use point gets (is seek pos equal to end pos?)
    // Since _endPosition uses the exact opposite discriminator byte as that used by
    // _query, a direct comparison between the two always reports they are not equal.
    // So we store _seekPosIncl and _endPosIncl versions which use the inclusive
    // discriminator byte and compare _seekPosIncl with _endPosIncl.
    _seekPosIncl.resetToKey(finalKey, _order);

    return _pointGet(finalKey, parts, needCursor);
}

void KVDBIdxUniqCursor::_updateLocAndTypeBits() {
    // We assume that cursors can only ever see unique indexes in their "pristine"
    // state,
    // where no duplicates are possible. The cases where dups are allowed should hold
    // sufficient locks to ensure that no cursor ever sees them.
    BufReader br(_mVal.data(), _mVal.len());
    _loc = KeyString::decodeRecordId(&br);
    _typeBits.resetFromBuffer(&br);

    if (!br.atEof()) {
        severe() << "Unique index cursor seeing multiple records for key " << _curr(kWantKey)->key;
        fassertFailed(40385);
    }
}

/* End KVDBIdxUniqCursor */

/* KVDBIdxBase */
KVDBIdxBase::KVDBIdxBase(KVDB& db,
                         KVSHandle& idxKvs,
                         KVDBCounterManager& counterManager,
                         std::string prefix,
                         std::string ident,
                         Ordering order,
                         const BSONObj& config,
                         int numFields,
                         const string indexKey)
    : _db(db),
      _idxKvs(idxKvs),
      _counterManager(counterManager),
      _prefix(prefix),
      _ident(ident),
      _order(order),
      _numFields(numFields),
      _indexSizeKeyKvs(indexKey) {
    int indexFormatVersion = 0;  // default

    _indexSizeKeyID = KVDBCounterMapUniqID.fetch_add(1);

    if (config.hasField("index_format_version")) {
        indexFormatVersion = config.getField("index_format_version").numberInt();
    }

    if (indexFormatVersion < kMinimumIndexVersion || indexFormatVersion > kMaximumIndexVersion) {
        Status indexVersionStatus(ErrorCodes::UnsupportedFormat,
                                  "Unrecognized index format -- you might want to upgrade MongoDB");
        fassertFailedWithStatusNoTrace(40384, indexVersionStatus);
    }

    _keyStringVersion =
        indexFormatVersion >= kKeyStringV1Version ? KeyString::Version::V1 : KeyString::Version::V0;
    loadCounter();
    _counterManager.registerIndex(this);
}

void KVDBIdxBase::fullValidate(OperationContext* opctx,
                               long long* numKeysOut,
                               ValidateResults* fullResults) const {
    if (numKeysOut) {
        std::unique_ptr<SortedDataInterface::Cursor> cursor(newCursor(opctx, 1));

        *numKeysOut = 0;
        const auto requestedInfo = Cursor::kJustExistance;
        for (auto entry = cursor->seek(BSONObj(), true, requestedInfo); entry;
             entry = cursor->next(requestedInfo)) {
            (*numKeysOut)++;
        }
    }
}

long long KVDBIdxBase::getSpaceUsedBytes(OperationContext* opctx) const {
    return static_cast<int64_t>(_indexSize.load());
}

bool KVDBIdxBase::isEmpty(OperationContext* opctx) {
    std::unique_ptr<SortedDataInterface::Cursor> cursor(newCursor(opctx, 1));
    const auto requestedInfo = Cursor::kJustExistance;

    auto entry = cursor->seek(BSONObj(), true, requestedInfo);
    if (entry == boost::none) {
        return true;
    }

    return false;
}

void KVDBIdxBase::generateConfig(BSONObjBuilder* configBuilder,
                                 int formatVersion,
                                 IndexDescriptor::IndexVersion descVersion) {
    if (formatVersion >= 0 && descVersion >= IndexDescriptor::IndexVersion::kV2) {
        configBuilder->append("index_format_version", static_cast<int32_t>(kMaximumIndexVersion));
    } else {
        // keep it backwards compatible
        configBuilder->append("index_format_version", static_cast<int32_t>(kMinimumIndexVersion));
    }
}

KVDBIdxBase::~KVDBIdxBase() {
    updateCounter();
    _counterManager.deregisterIndex(this);
}
/* End KVDBIdxBase */

/* Start KVDBUniqIdx */
KVDBUniqIdx::KVDBUniqIdx(KVDB& db,
                         KVSHandle& idxKvs,
                         KVDBCounterManager& counterManager,
                         std::string prefix,
                         std::string ident,
                         Ordering order,
                         const BSONObj& config,
                         bool partial,
                         int numFields,
                         const string indexString)
    : KVDBIdxBase(
          db, idxKvs, counterManager, prefix, ident, order, config, numFields, indexString) {
    _partial = partial;
}

Status KVDBUniqIdx::insert(OperationContext* opctx,
                           const BSONObj& key,
                           const RecordId& loc,
                           bool dupsAllowed) {
    Status s = checkKeySize(key);
    if (!s.isOK()) {
        return s;
    }

    KeyString encodedKey(_keyStringVersion, key, _order);
    std::string prefixedKey(makePrefixedKey(_prefix, encodedKey));

    auto ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    KVDBData pKey{(uint8_t*)prefixedKey.c_str(), prefixedKey.size()};
    KVDBData iVal{};
    bool found = false;

    // Do a quick check if key already exists
    auto hseSt = ru->probeKey(_idxKvs, pKey, found);
    if (!hseSt.ok()) {
        return hseToMongoStatus(hseSt);
    } else if (!found) {
        // nothing here. just insert the value
        KeyString value(_keyStringVersion, loc);
        if (!encodedKey.getTypeBits().isAllZeros()) {
            value.appendTypeBits(encodedKey.getTypeBits());
        }
        iVal = KVDBData{(uint8_t*)value.getBuffer(), value.getSize()};
        hseSt = ru->put(_idxKvs, pKey, iVal);

        if (hseSt.ok()) {
            incrementCounter(ru, prefixedKey.size());
        }
        return hseToMongoStatus(hseSt);
    }

    // we are in a weird state where there might be multiple values for a key
    // we put them all in the "list"
    // Note that we can't omit AllZeros when there are multiple locs for a
    // value. When we remove
    // down to a single value, it will be cleaned up.


    // need to read the value first
    hseSt = ru->getMCo(_idxKvs, pKey, iVal, found);
    if (!hseSt.ok()) {
        return hseToMongoStatus(hseSt);
    }
    invariantHse(found);

    bool insertedLoc = false;
    KeyString valueVector(_keyStringVersion);
    BufReader br(iVal.data(), iVal.len());
    while (br.remaining()) {
        RecordId locInIndex = KeyString::decodeRecordId(&br);
        if (loc == locInIndex) {
            return Status::OK();  // already in index
        }

        if (!insertedLoc && loc < locInIndex) {
            valueVector.appendRecordId(loc);
            valueVector.appendTypeBits(encodedKey.getTypeBits());
            insertedLoc = true;
        }

        // Copy from old to new value
        valueVector.appendRecordId(locInIndex);
        valueVector.appendTypeBits(KeyString::TypeBits::fromBuffer(_keyStringVersion, &br));
    }

    if (!dupsAllowed) {
        return Status(ErrorCodes::DuplicateKey, dupKeyError(key));
    }

    if (!insertedLoc) {
        // This loc is higher than all currently in the index for this key
        valueVector.appendRecordId(loc);
        valueVector.appendTypeBits(encodedKey.getTypeBits());
    }

    iVal = KVDBData((uint8_t*)valueVector.getBuffer(), valueVector.getSize());
    hseSt = ru->put(_idxKvs, pKey, iVal);
    return hseToMongoStatus(hseSt);
}

void KVDBUniqIdx::unindex(OperationContext* opctx,
                          const BSONObj& key,
                          const RecordId& loc,
                          bool dupsAllowed) {
    // When DB parameter failIndexKeyTooLong is set to false,
    // this method may be called for non-existing
    // keys with the length exceeding the maximum allowed.
    // Since such keys cannot be in the storage in any case,
    // executing the following code results in:
    // - corruption of index storage size value, and
    // - an attempt to single-delete non-existing key which may
    //   potentially lead to consecutive single-deletion of the key.
    // Filter out long keys to prevent the problems described.
    if (!checkKeySize(key).isOK()) {
        return;
    }

    KeyString encodedKey(_keyStringVersion, key, _order);
    std::string prefixedKey(makePrefixedKey(_prefix, encodedKey));
    KVDBData pKey{(uint8_t*)prefixedKey.c_str(), prefixedKey.size()};

    auto ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    hse::Status hseSt{};
    // We do blind unindexing of records for efficiency. However, when duplicates
    // are allowed in unique indexes, confirm that the recordid matches the element
    // we are removing.
    if (!dupsAllowed && !_partial) {
        hseSt = ru->del(_idxKvs, pKey);
        invariantHseSt(hseSt);
        incrementCounter(ru, -prefixedKey.size());
        return;
    }

    bool found = false;
    KVDBData iVal{};

    if (!dupsAllowed && _partial) {
        // Check that the record id matches. We may be called to unindex records that are not
        // present in the index due to the partial filter expression.
        hseSt = ru->getMCo(_idxKvs, pKey, iVal, found);
        invariantHseSt(hseSt);
        if (found) {
            BufReader br(iVal.data(), iVal.len());
            invariantHse(br.remaining());
            RecordId locInIndex = KeyString::decodeRecordId(&br);
            KeyString::TypeBits typeBits = KeyString::TypeBits::fromBuffer(_keyStringVersion, &br);
            invariantHse(!br.remaining());

            if (locInIndex == loc) {
                hseSt = ru->del(_idxKvs, pKey);
                invariantHseSt(hseSt);
                incrementCounter(ru, -prefixedKey.size());
            }
        }
        return;
    }

    // dups are allowed, so we have to deal with a vector of RecordIds.
    hseSt = ru->getMCo(_idxKvs, pKey, iVal, found);
    invariantHseSt(hseSt);
    if (!found) {
        // nothing here. just return
        return;
    }

    bool foundLoc = false;
    std::vector<std::pair<RecordId, KeyString::TypeBits>> records;

    BufReader br(iVal.data(), iVal.len());
    while (br.remaining()) {
        RecordId locInIndex = KeyString::decodeRecordId(&br);
        KeyString::TypeBits typeBits = KeyString::TypeBits::fromBuffer(_keyStringVersion, &br);

        if (loc == locInIndex) {
            if (records.empty() && !br.remaining()) {
                // This is the common case: we are removing the only loc for this
                // key.
                // Remove the whole entry.
                hseSt = ru->del(_idxKvs, pKey);
                invariantHseSt(hseSt);
                incrementCounter(ru, -prefixedKey.size());
                return;
            }

            foundLoc = true;
            continue;
        }

        records.push_back(std::make_pair(locInIndex, typeBits));
    }

    if (!foundLoc) {
        warning().stream() << loc << " not found in the index for key " << key;
        return;  // nothing to do
    }

    // Put other locs for this key back in the index.
    KeyString newValue(_keyStringVersion);
    invariantHse(!records.empty());
    for (size_t i = 0; i < records.size(); i++) {
        newValue.appendRecordId(records[i].first);
        // When there is only one record, we can omit AllZeros TypeBits.
        // Otherwise they need
        // to be included.
        if (!(records[i].second.isAllZeros() && records.size() == 1)) {
            newValue.appendTypeBits(records[i].second);
        }
    }

    iVal = KVDBData{(uint8_t*)newValue.getBuffer(), newValue.getSize()};
    hseSt = ru->put(_idxKvs, pKey, iVal);
    invariantHseSt(hseSt);
}

Status KVDBUniqIdx::dupKeyCheck(OperationContext* opctx, const BSONObj& key, const RecordId& loc) {
    KeyString encodedKey(_keyStringVersion, key, _order);
    std::string prefixedKey(makePrefixedKey(_prefix, encodedKey));
    KVDBData pKey{(uint8_t*)prefixedKey.c_str(), prefixedKey.size()};

    auto ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);
    KVDBData iVal{};
    bool found = false;
    auto hseSt = ru->getMCo(_idxKvs, pKey, iVal, found);
    if (!hseSt.ok()) {
        return hseToMongoStatus(hseSt);
    } else if (!found) {
        return Status::OK();
    }

    // If the key exists, check if we already have this loc at this key. If so,
    // we don't
    // consider that to be a dup.
    BufReader br(iVal.data(), iVal.len());
    while (br.remaining()) {
        if (KeyString::decodeRecordId(&br) == loc) {
            return Status::OK();
        }

        KeyString::TypeBits::fromBuffer(_keyStringVersion,
                                        &br);  // Just calling this to advance reader.
    }

    return Status(ErrorCodes::DuplicateKey, dupKeyError(key));
}

std::unique_ptr<SortedDataInterface::Cursor> KVDBUniqIdx::newCursor(OperationContext* opctx,
                                                                    bool forward) const {
    return stdx::make_unique<KVDBIdxUniqCursor>(
        opctx, _idxKvs, _prefix, forward, _order, _keyStringVersion, _numFields);
}

SortedDataBuilderInterface* KVDBUniqIdx::getBulkBuilder(OperationContext* opctx, bool dupsAllowed) {
    return new KVDBUniqBulkBuilder(
        *this, _idxKvs, _prefix, _order, _keyStringVersion, opctx, dupsAllowed);
}

/* End KVDBUniqIdx */

/* Start KVDBStdIndex */
KVDBStdIdx::KVDBStdIdx(KVDB& db,
                       KVSHandle& idxKvs,
                       KVDBCounterManager& counterManager,
                       std::string prefix,
                       std::string ident,
                       Ordering order,
                       const BSONObj& config,
                       int numFields,
                       const string indexKey)
    : KVDBIdxBase(db, idxKvs, counterManager, prefix, ident, order, config, numFields, indexKey) {}

Status KVDBStdIdx::insert(OperationContext* opctx,
                          const BSONObj& key,
                          const RecordId& loc,
                          bool dupsAllowed) {
    invariantHse(dupsAllowed);
    Status s = checkKeySize(key);
    if (!s.isOK()) {
        return s;
    }

    KeyString encodedKey(_keyStringVersion, key, _order);
    std::string prefixedKey(makePrefixedKey(_prefix, encodedKey));

    // Append the 8-byte record ID.
    int64_t bigLoc = endian::nativeToBig(loc.repr());
    prefixedKey.append(reinterpret_cast<const char*>(&bigLoc), sizeof(bigLoc));

    KVDBData pKey{(uint8_t*)prefixedKey.c_str(), prefixedKey.size()};

    auto ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    KVDBData iVal{};
    if (!encodedKey.getTypeBits().isAllZeros()) {
        iVal = KVDBData(reinterpret_cast<const uint8_t*>(encodedKey.getTypeBits().getBuffer()),
                        encodedKey.getTypeBits().getSize());
    }

    auto hseSt = ru->put(_idxKvs, pKey, iVal);
    if (hseSt.ok()) {
        incrementCounter(ru, prefixedKey.size());
    }

    return hseToMongoStatus(hseSt);
}

Status KVDBStdIdx::bulkInsert(OperationContext* opctx, const BSONObj& key, const RecordId& loc) {
    Status s = checkKeySize(key);
    if (!s.isOK()) {
        return s;
    }

    KeyString encodedKey(_keyStringVersion, key, _order);
    std::string prefixedKey(makePrefixedKey(_prefix, encodedKey));

    // Append the 8-byte record ID.
    int64_t bigLoc = endian::nativeToBig(loc.repr());
    prefixedKey.append(reinterpret_cast<const char*>(&bigLoc), sizeof(bigLoc));

    KVDBData pKey{(uint8_t*)prefixedKey.c_str(), prefixedKey.size()};
    auto ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    KVDBData iVal{};
    if (!encodedKey.getTypeBits().isAllZeros()) {
        iVal = KVDBData(reinterpret_cast<const uint8_t*>(encodedKey.getTypeBits().getBuffer()),
                        encodedKey.getTypeBits().getSize());
    }

    auto hseSt = ru->put(_idxKvs, pKey, iVal);
    invariantHseSt(hseSt);

    incrementCounter(ru, prefixedKey.size());

    return hseToMongoStatus(hseSt);
}

void KVDBStdIdx::unindex(OperationContext* opctx,
                         const BSONObj& key,
                         const RecordId& loc,
                         bool dupsAllowed) {
    invariantHse(dupsAllowed);
    // When DB parameter failIndexKeyTooLong is set to false,
    // this method may be called for non-existing
    // keys with the length exceeding the maximum allowed.
    // Since such keys cannot be in the storage in any case,
    // executing the following code results in:
    // - corruption of index storage size value, and
    // - an attempt to single-delete non-existing key which may
    //   potentially lead to consecutive single-deletion of the key.
    // Filter out long keys to prevent the problems described.
    if (!checkKeySize(key).isOK()) {
        return;
    }

    KeyString encodedKey(_keyStringVersion, key, _order);
    std::string prefixedKey(makePrefixedKey(_prefix, encodedKey));

    // Append the 8-byte record ID.
    int64_t bigLoc = endian::nativeToBig(loc.repr());
    prefixedKey.append(reinterpret_cast<const char*>(&bigLoc), sizeof(bigLoc));

    KVDBData pKey{(uint8_t*)prefixedKey.c_str(), prefixedKey.size()};

    auto ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    auto hseSt = ru->del(_idxKvs, pKey);
    invariantHseSt(hseSt);

    incrementCounter(ru, -prefixedKey.size());
}

Status KVDBStdIdx::dupKeyCheck(OperationContext* opctx, const BSONObj& key, const RecordId& loc) {
    // Should not be called for non-unique indexes.
    invariantHse(false);
}

std::unique_ptr<SortedDataInterface::Cursor> KVDBStdIdx::newCursor(OperationContext* opctx,
                                                                   bool forward) const {
    return stdx::make_unique<KVDBIdxStdCursor>(
        opctx, _idxKvs, _prefix, forward, _order, _keyStringVersion, _numFields);
}

SortedDataBuilderInterface* KVDBStdIdx::getBulkBuilder(OperationContext* opctx, bool dupsAllowed) {
    invariantHse(dupsAllowed);
    return new KVDBStdBulkBuilder(*this, opctx);
}

/* End KVDBStdIdx */

/* Start KVDBStdBulkBuilder */
KVDBStdBulkBuilder::KVDBStdBulkBuilder(KVDBStdIdx& index, OperationContext* opctx)
    : _index(index), _opctx(opctx) {}

Status KVDBStdBulkBuilder::addKey(const BSONObj& key, const RecordId& loc) {
    return _index.bulkInsert(_opctx, key, loc);
}

void KVDBStdBulkBuilder::commit(bool mayInterrupt) {
    WriteUnitOfWork uow(_opctx);
    uow.commit();
}

/* End KVDBStdBulkBuilder */

/* Start KVDBUniqBulkBuilder */

KVDBUniqBulkBuilder::KVDBUniqBulkBuilder(KVDBUniqIdx& index,
                                         KVSHandle& idxKvs,
                                         std::string prefix,
                                         Ordering ordering,
                                         KeyString::Version keyStringVersion,
                                         OperationContext* opctx,
                                         bool dupsAllowed)
    : _index(index),
      _idxKvs(idxKvs),
      _prefix(std::move(prefix)),
      _ordering(ordering),
      _keyStringVersion(keyStringVersion),
      _opctx(opctx),
      _dupsAllowed(dupsAllowed),
      _keyString(keyStringVersion) {}

Status KVDBUniqBulkBuilder::addKey(const BSONObj& newKey, const RecordId& loc) {
    Status s = checkKeySize(newKey);
    if (!s.isOK()) {
        return s;
    }

    const int cmp = newKey.woCompare(_key, _ordering);
    if (cmp != 0) {
        if (!_key.isEmpty()) {      // _key.isEmpty() is only true on the first call to addKey().
            invariantHse(cmp > 0);  // newKey must be > the last key
            // We are done with dups of the last key so we can insert it now.
            _doInsert();
        }
        invariantHse(_records.empty());
    } else {
        // Dup found!
        if (!_dupsAllowed) {
            return Status(ErrorCodes::DuplicateKey, dupKeyError(newKey));
        }

        // If we get here, we are in the weird mode where dups are allowed on a unique
        // index, so add ourselves to the list of duplicate locs. This also replaces the
        // _key which is correct since any dups seen later are likely to be newer.
    }

    _key = newKey.getOwned();
    _keyString.resetToKey(_key, _ordering);
    _records.push_back(std::make_pair(loc, _keyString.getTypeBits()));

    return Status::OK();
}
void KVDBUniqBulkBuilder::commit(bool mayInterrupt) {
    WriteUnitOfWork uow(_opctx);
    if (!_records.empty()) {
        // This handles inserting the last unique key.
        _doInsert();
    }
    uow.commit();
}

void KVDBUniqBulkBuilder::_doInsert() {
    invariantHse(!_records.empty());

    KeyString value(_keyStringVersion);
    for (size_t i = 0; i < _records.size(); i++) {
        value.appendRecordId(_records[i].first);
        // When there is only one record, we can omit AllZeros TypeBits. Otherwise they need
        // to be included.
        if (!(_records[i].second.isAllZeros() && _records.size() == 1)) {
            value.appendTypeBits(_records[i].second);
        }
    }

    std::string prefixedKey(makePrefixedKey(_prefix, _keyString));
    KVDBData iKey{(uint8_t*)prefixedKey.c_str(), prefixedKey.size()};
    KVDBData iVal{(uint8_t*)value.getBuffer(), value.getSize()};

    auto ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(_opctx);
    auto hseSt = ru->put(_idxKvs, iKey, iVal);
    invariantHseSt(hseSt);

    _index.incrementCounter(ru, prefixedKey.size());

    _records.clear();
}

/* End KVDBUniqBulkBuilder */
}  // namespace mongo
