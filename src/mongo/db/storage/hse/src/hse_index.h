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

#include "mongo/platform/basic.h"

#include <atomic>
#include <memory>
#include <string>

#include "mongo/base/checked_cast.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/ordering.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/storage/key_string.h"
#include "mongo/db/storage/sorted_data_interface.h"

#include "hse.h"
#include "hse_counter_manager.h"
#include "hse_exceptions.h"
#include "hse_recovery_unit.h"
#include "hse_util.h"

using hse::KVSHandle;

namespace mongo {

class KVDBIdxCursorBase : public SortedDataInterface::Cursor {
public:
    KVDBIdxCursorBase(OperationContext* opctx,
                      KVSHandle& idxKvs,
                      std::string prefix,
                      bool forward,
                      Ordering order,
                      KeyString::Version keyStringVersion,
                      int numFields);
    virtual ~KVDBIdxCursorBase();

    virtual void setEndPosition(const BSONObj& key, bool inclusive) override;

    virtual boost::optional<IndexKeyEntry> next(RequestedInfo parts = kKeyAndLoc) override;
    virtual boost::optional<IndexKeyEntry> seek(const BSONObj& key,
                                                bool inclusive,
                                                RequestedInfo parts = kKeyAndLoc) override;
    virtual boost::optional<IndexKeyEntry> seek(const IndexSeekPoint& seekPoint,
                                                RequestedInfo parts = kKeyAndLoc) override;

    virtual void save() override;

    virtual void saveUnpositioned() override;

    virtual void restore() override;

    virtual void detachFromOperationContext() final;

    virtual void reattachToOperationContext(OperationContext* opCtx) final;

protected:
    void _ensureCursor();
    void _destroyMCursor();

    void _advanceCursor();
    void _updatePosition();
    boost::optional<IndexKeyEntry> _curr(RequestedInfo parts) const;
    void _seekCursor(const KeyString& query);
    boost::optional<IndexKeyEntry> _seek(const BSONObj& key,
                                         int cnt,
                                         bool inclusive,
                                         RequestedInfo parts = kKeyAndLoc);
    virtual boost::optional<IndexKeyEntry> _pointGet(const BSONObj& key,
                                                     RequestedInfo parts,
                                                     bool& needCursor) = 0;
    virtual bool _needCursorAfterUpdate() = 0;
    virtual void _updateLocAndTypeBits() = 0;

    KVSHandle& _idxKvs;  // not owned
    std::string _prefix;
    KvsCursor* _cursor;
    bool _cursorValid = false;
    const bool _forward;
    bool _needSeek = false;
    bool _needUpdate = false;
    Ordering _order;

    // These are for storing savePosition/restorePosition state
    bool _savedEOF = false;
    // RecordId _savedRecordId;
    // rocksdb::SequenceNumber _currentSequenceNumber;

    KeyString::Version _keyStringVersion;
    KeyString _key;
    KeyString::TypeBits _typeBits;
    RecordId _loc;

    KeyString _query;
    KeyString _seekPosIncl;
    KeyString _endPosIncl;

    std::unique_ptr<KeyString> _endPosition;

    int _numFields;

    bool _lastPointGet = false;
    bool _eof = false;
    OperationContext* _opctx;

    // stores the value associated with the latest call to seekExact()
    // std::string _value;
    bool _enodata = false;

    KVDBData _mKey{};
    KVDBData _mVal{};
};

class KVDBIdxStdCursor : public KVDBIdxCursorBase {

public:
    KVDBIdxStdCursor(OperationContext* opctx,
                     KVSHandle& idxKvs,
                     std::string prefix,
                     bool forward,
                     Ordering order,
                     KeyString::Version keyStringVersion,
                     int numFields);
    virtual ~KVDBIdxStdCursor();

protected:
    virtual boost::optional<IndexKeyEntry> _pointGet(const BSONObj& key,
                                                     RequestedInfo parts,
                                                     bool& needCursor);
    virtual void _updateLocAndTypeBits();
    virtual bool _needCursorAfterUpdate();
};

class KVDBIdxUniqCursor : public KVDBIdxCursorBase {

public:
    KVDBIdxUniqCursor(OperationContext* opctx,
                      KVSHandle& idxKvs,
                      std::string prefix,
                      bool forward,
                      Ordering order,
                      KeyString::Version keyStringVersion,
                      int numFields);
    virtual ~KVDBIdxUniqCursor();

    boost::optional<IndexKeyEntry> seekExact(const BSONObj& key, RequestedInfo parts) override;

protected:
    virtual boost::optional<IndexKeyEntry> _pointGet(const BSONObj& key,
                                                     RequestedInfo parts,
                                                     bool& needCursor);
    virtual void _updateLocAndTypeBits();
    virtual bool _needCursorAfterUpdate();
};


class KVDBIdxBase : public SortedDataInterface {
    MONGO_DISALLOW_COPYING(KVDBIdxBase);

public:
    KVDBIdxBase(KVDB& db,
                KVSHandle& idxKvs,
                KVDBCounterManager& counterManager,
                std::string prefix,
                std::string ident,
                Ordering order,
                const BSONObj& config,
                int numFields,
                const string indexSizeKey);
    virtual ~KVDBIdxBase();

    virtual SortedDataBuilderInterface* getBulkBuilder(OperationContext* opctx,
                                                       bool dupsAllowed) = 0;

    virtual void fullValidate(OperationContext* opctx,
                              long long* numKeysOut,
                              ValidateResults* fullResults) const;

    virtual bool appendCustomStats(OperationContext* opctx,
                                   BSONObjBuilder* output,
                                   double scale) const {
        // nothing to say here, really
        return false;
    }

    virtual bool isEmpty(OperationContext* opctx);

    virtual long long getSpaceUsedBytes(OperationContext* opctx) const;

    virtual Status initAsEmpty(OperationContext* opctx) {
        // Nothing to do here
        return Status::OK();
    }

    static void generateConfig(BSONObjBuilder* configBuilder,
                               int formatVersion,
                               IndexDescriptor::IndexVersion descVersion);

    void loadCounter();
    void updateCounter();
    void incrementCounter(KVDBRecoveryUnit* ru, int size);

protected:
    KVDB& _db;
    KVSHandle& _idxKvs;                   // not owned
    KVDBCounterManager& _counterManager;  // not owned

    // Each key in the index is prefixed with _prefix
    std::string _prefix;
    std::string _ident;

    // used to construct RocksCursors
    const Ordering _order;
    KeyString::Version _keyStringVersion;
    int _numFields;
    const std::string _indexSizeKeyKvs;
    unsigned long _indexSizeKeyID;

    char _pad[128];

    std::atomic<long long> _indexSize;
    char _indexSizePad[128 - sizeof(_indexSize)];
};

class KVDBUniqIdx : public KVDBIdxBase {
public:
    KVDBUniqIdx(KVDB& db,
                KVSHandle& idxKvs,
                KVDBCounterManager& counterManager,
                std::string prefix,
                std::string ident,
                Ordering order,
                const BSONObj& config,
                bool partial,
                int numFields,
                const string indexSizeKey);

    virtual Status insert(OperationContext* opctx,
                          const BSONObj& key,
                          const RecordId& loc,
                          bool dupsAllowed);

    virtual void unindex(OperationContext* opctx,
                         const BSONObj& key,
                         const RecordId& loc,
                         bool dupsAllowed);

    virtual Status dupKeyCheck(OperationContext* opctx, const BSONObj& key, const RecordId& loc);

    virtual std::unique_ptr<SortedDataInterface::Cursor> newCursor(OperationContext* opctx,
                                                                   bool forward) const;

    virtual SortedDataBuilderInterface* getBulkBuilder(OperationContext* opctx,
                                                       bool dupsAllowed) override;

private:
    bool _partial;
};

class KVDBStdIdx : public KVDBIdxBase {
public:
    KVDBStdIdx(KVDB& db,
               KVSHandle& idxKvs,
               KVDBCounterManager& counterManager,
               std::string prefix,
               std::string ident,
               Ordering order,
               const BSONObj& config,
               int numFields,
               const string indexSizeKey);

    virtual Status insert(OperationContext* opctx,
                          const BSONObj& key,
                          const RecordId& loc,
                          bool dupsAllowed);

    virtual void unindex(OperationContext* opctx,
                         const BSONObj& key,
                         const RecordId& loc,
                         bool dupsAllowed);

    virtual Status bulkInsert(OperationContext* opctx, const BSONObj& key, const RecordId& loc);

    virtual Status dupKeyCheck(OperationContext* opctx, const BSONObj& key, const RecordId& loc);

    virtual std::unique_ptr<SortedDataInterface::Cursor> newCursor(OperationContext* opctx,
                                                                   bool forward) const;

    virtual SortedDataBuilderInterface* getBulkBuilder(OperationContext* opctx,
                                                       bool dupsAllowed) override;
};

/**
 * Bulk builds a non-unique index.
 */
class KVDBStdBulkBuilder : public SortedDataBuilderInterface {
public:
    KVDBStdBulkBuilder(KVDBStdIdx& index, OperationContext* opctx);
    Status addKey(const BSONObj& key, const RecordId& loc);

    void commit(bool mayInterrupt);

private:
    KVDBStdIdx& _index;
    OperationContext* _opctx;
};


/**
 * Bulk builds a unique index.
 *
 * In order to support unique indexes in dupsAllowed mode this class only does an actual insert
 * after it sees a key after the one we are trying to insert. This allows us to gather up all
 * duplicate locs and insert them all together. This is necessary since bulk cursors can only
 * append data.
 */
class KVDBUniqBulkBuilder : public SortedDataBuilderInterface {
public:
    KVDBUniqBulkBuilder(KVDBUniqIdx& index,
                        KVSHandle& idxKvs,
                        std::string prefix,
                        Ordering ordering,
                        KeyString::Version keyStringVersion,
                        OperationContext* opctx,
                        bool dupsAllowed);

    Status addKey(const BSONObj& newKey, const RecordId& loc);
    void commit(bool mayInterrupt);

private:
    void _doInsert();

    KVDBUniqIdx& _index;
    KVSHandle& _idxKvs;
    std::string _prefix;
    Ordering _ordering;
    const KeyString::Version _keyStringVersion;
    OperationContext* _opctx;
    const bool _dupsAllowed;
    BSONObj _key;
    KeyString _keyString;
    std::vector<std::pair<RecordId, KeyString::TypeBits>> _records;
};
}  // namespace mongo
