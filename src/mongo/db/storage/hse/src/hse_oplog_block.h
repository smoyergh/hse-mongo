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

#include "mongo/db/storage/record_store.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/platform/basic.h"

#include "hse_recovery_unit.h"
#include "hse_util.h"

#include <deque>
#include <string>

using std::string;
using std::deque;

using hse::OPLOG_START_BLK;

namespace mongo {

//
// The KVDBOplogBlockManager abstraction maintains a collection KVDBOplogBlock's, each of
// which describes a set of entries. There are two lifecycles to be aware of: (1) that of
// a KVDBOplogBlockManager instance, and (2) that of KVDBOplogBlock instances within the
// context of a KVDBOplogBlockManager instance. A KVDBOplogBlock is effectively metadata
// related to contiguous collection of oplog entries.
//
// (1) KVDBOplogBlockManager Instance Lifecycle
//
//     When an oplog is opened, an instance of a KVDBOplogBlockManager is created. During
//     construction, the current collection of KVDBOplogBlock's is determined. One of the
//     key tasks for a KVDBOplogBlockManager is to track the identity of the last oplog
//     block that was deleted through reclamation. As the manager operates and reclaims
//     oplog blocks, it updates this data transactionally with the deletion itself. At
//     creation, the KVDBOplogBlockManager constructor reads the index of the last deleted
//     oplog block. If that value is missing, then nothing was ever reclaimed and the first
//     block to start with is the earliest oplog block index (OPLOG_START_BLK). If it is
//     present, then the first active block has an index one greater than that of the
//     last block deleted.
//
//     Starting from the first active oplog block index, each oplog block is read in turn
//     creating the list of active KVDBOplogBlock's.
//
// (2) KVDBOplogBlock Instance Management
//
//     Metadata about a collection of oplog entries (i.e., a KVDBOplogBlock) is kept in
//     one of two places: (1) the oplog regular KVS (see (A) below), or the (2) the
//     oplog large KVS. The latter is also used as something like a metadata store to
//     keep track of the last deleted KVDBOplogBlock ID.
//
//     Those KVDBOplogBlock's that are not the current block are kept in the regular
//     oplog KVS and correspond to those in the KVDBOplogBlockManager's _blockList
//     member variable.
//
//
// Outstanding Problems:
// --------------------------------
//
// The scope of the issues with this code is too large to clean up in the current round
// of rework. Instead, it will be deferred to a subsequent effort. Items that come
// immediately to mind:
//
//   (A) The names "kvs" and "largeKvs" should have no meaning in the context of the
//       KVDBOplogManager. The KVDBOplogStore uses the KVDBOplogManager primarily to
//       access/mutate metadata about the oplog itself. In particular, it inserts
//       and updates individual oplog entries itself. However, in the case of oplog
//       reclamation the KVDBOplogBlockManager "knows" that individual oplog entries
//       are in the same KVS that it stores its own "marker" information. A side
//       effective is that prefix deletes in "truncate" (almost assuredly) aren't
//       doing what they appear to do.
//
//   (B) No non-static member function may be called by another non-static member
//       function passing as arguments non-static member variables. Further, every
//       single argument to a function must be needed by that function unless the
//       calling signature is dictated by inheritance from a subclass. This code
//       violates this in a pervasive fashion. It was only in trying to fix this
//       that the problem described in (A) was discovered.
//

class KVDBOplogBlockManager;

struct KVDBOplogBlock {
    static const int SERLEN = 4 + 8 + 8 + 8;  // Serialized length

    uint32_t blockId{OPLOG_START_BLK};
    RecordId highestRec{};
    AtomicInt64 sizeInBytes{0};
    AtomicInt64 numRecs{0};

    KVDBOplogBlock(){};
    ~KVDBOplogBlock(){};

    KVDBOplogBlock(const KVDBOplogBlock& n) {
        this->blockId = n.blockId;
        this->highestRec = n.highestRec;
        this->sizeInBytes.store(n.sizeInBytes.load());
        this->numRecs.store(n.numRecs.load());
    }

    KVDBOplogBlock& operator=(const KVDBOplogBlock& rhs) {
        this->blockId = rhs.blockId;
        this->highestRec = rhs.highestRec;
        this->sizeInBytes.store(rhs.sizeInBytes.load());
        this->numRecs.store(rhs.numRecs.load());

        return *this;
    }

    static bool cmpWithBlkId(const KVDBOplogBlock& lhs, const KVDBOplogBlock& rhs) {
        if (lhs.blockId < rhs.blockId)
            return true;

        return false;
    }

    static string blockIdToStr(uint32_t id) {
        uint32_t blkIdBig = endian::nativeToBig(id);

        return std::string(reinterpret_cast<const char*>(&blkIdBig), sizeof(uint32_t));
    }

    static string blockToBuf(const KVDBOplogBlock& block) {
        string retStr;

        retStr.reserve(SERLEN);

        uint32_t blkIdBig = endian::nativeToBig(block.blockId);
        string blkIdStr = std::string(reinterpret_cast<const char*>(&blkIdBig), sizeof(uint32_t));
        retStr.append(blkIdStr);

        int64_t hiRecIdBig = endian::nativeToBig(block.highestRec.repr());
        string hiRecIdStr =
            std::string(reinterpret_cast<const char*>(&hiRecIdBig), sizeof(int64_t));
        retStr.append(hiRecIdStr);

        int64_t szBig = endian::nativeToBig(block.sizeInBytes.load());
        string szStr = std::string(reinterpret_cast<const char*>(&szBig), sizeof(int64_t));
        retStr.append(szStr);


        int64_t nRecsBig = endian::nativeToBig(block.numRecs.load());
        string nRecsStr = std::string(reinterpret_cast<const char*>(&nRecsBig), sizeof(int64_t));
        retStr.append(nRecsStr);

        return retStr;
    }

    static KVDBOplogBlock bufToBlock(const uint8_t* buf, const int64_t len) {
        KVDBOplogBlock retBlk{};

        invariantHse(SERLEN == len);

        retBlk.blockId = endian::bigToNative(*((uint32_t*)buf));
        buf += sizeof(uint32_t);

        retBlk.highestRec = RecordId{endian::bigToNative(*((int64_t*)buf))};
        buf += sizeof(int64_t);

        retBlk.sizeInBytes.store(endian::bigToNative(*((int64_t*)buf)));
        buf += sizeof(int64_t);

        retBlk.numRecs.store(endian::bigToNative(*((int64_t*)buf)));
        buf += sizeof(int64_t);

        return retBlk;
    }
};

// comparison for lower_bound
static bool operator<(const KVDBOplogBlock& lhs, const KVDBOplogBlock& rhs) {
    if (lhs.highestRec < rhs.highestRec)
        return true;

    return false;
}


static bool operator==(const KVDBOplogBlock& lhs, const KVDBOplogBlock& rhs) {
    if (lhs.highestRec == rhs.highestRec)
        return true;

    return false;
}

class KVDBOplogBlockManager {
public:
    KVDBOplogBlockManager(OperationContext* opctx,
                          KVDB& db,
                          KVSHandle& blockKvs,
                          KVSHandle& metadataKvs,
                          uint32_t prefix,
                          int64_t cappedMaxSize);
    ~KVDBOplogBlockManager();

    string getCurrentBlockId();
    uint32_t getBlockId(const RecordId& loc);

    Status truncate(OperationContext* opctx, const KVSHandle& kvs, const KVSHandle& largeKvs);
    uint32_t getBlockIdToInsert(const RecordId& loc);
    uint32_t getBlockIdToInsertAndGrow(const RecordId& loc, int64_t nRecs, int64_t size);
    Status cappedTruncateAfter(OperationContext* opctx,
                               KVSHandle& kvs,
                               KVSHandle& largeKvs,
                               const RecordId& end,
                               bool inclusive,
                               RecordId& lastKeptId,
                               int64_t& numRecsDel,
                               int64_t& sizeDel);
    static hse::Status cursorRead(
        KVDBRecoveryUnit* ru, KvsCursor* cursor, KVDBData& key, KVDBData& val, bool& eof);
    void awaitHasExcessBlocksOrDead();
    void stop();
    bool isDead();
    boost::optional<KVDBOplogBlock> getOldestBlockIfExcess();
    void removeOldestBlock();
    static hse::Status deleteBlock(KVDBRecoveryUnit* ru,
                                   KVSHandle& kvs,
                                   KVSHandle& largeKvs,
                                   bool usePdel,
                                   uint32_t prefix,
                                   const KVDBOplogBlock& block);
    hse::Status updateLastBlkDeleted(KVDBRecoveryUnit* ru,
                                     KVSHandle& kvs,
                                     KVSHandle& largeKvs,
                                     uint32_t blockId);
    static void importBlocks(OperationContext* opctx,
                             KVSHandle& kvs,
                             KVSHandle& largeKvs,
                             uint32_t prefix,
                             deque<KVDBOplogBlock>& blockList,
                             KVDBOplogBlock& currBlock);
    static void dropAllBlocks(OperationContext* opctx,
                              KVSHandle& kvs,
                              KVSHandle& largeKvs,
                              uint32_t prefix);
    RecordId getHighestFromPrevBlk(OperationContext* opctx, uint32_t blkId);
    RecordId getHighestSeenLoc();

    //
    // The following methods are public only for use in tests.
    //
    size_t numBlocks() const;
    int64_t currentBytes() const;
    int64_t currentRecords() const;
    void setMinBytesPerBlock(int64_t size);
    void setMaxBlocksToKeep(size_t numStones);


private:
    static string _computeLastBlockDeletedKey(uint32_t prefix);

    static string _computeCurrentBlockKey(uint32_t prefix);

    static hse::Status _readLastDeletedBlockId(KVDBRecoveryUnit* ru,
                                               KVSHandle& kvs,
                                               KVSHandle& largeKvs,
                                               string key,
                                               uint32_t& lastBlockId,
                                               bool& found);
    KVDBOplogBlock& _getCurrBlock();
    uint32_t _getNextBlockId(uint32_t prevId);
    string _getCurrentBlockId();
    hse::Status _findLastKeptIdInclusive(KVDBRecoveryUnit* ru,
                                         KVSHandle& kvs,
                                         KVSHandle& largeKvs,
                                         RecordId& lastKeptId,
                                         const RecordId& end,
                                         KVDBOplogBlock& block);
    hse::Status _deleteBlockByScan(KVDBRecoveryUnit* ru,
                                   KVSHandle& kvs,
                                   KVSHandle& largeKvs,
                                   KVDBOplogBlock& block,
                                   const RecordId& start,
                                   bool inclusive);
    hse::Status _delKeyHelper(
        KVDBRecoveryUnit* ru, KVSHandle& kvs, KVSHandle& largeKvs, KVDBData& key, uint32_t valLen);
    hse::Status _writeMarker(const KVDBOplogBlock& block);
    hse::Status _deleteMarker(uint32_t blockId);
    static hse::Status _readMarker(OperationContext* opctx,
                                   KVSHandle& kvs,
                                   KVSHandle& largeKvs,
                                   uint32_t prefix,
                                   uint32_t blockId,
                                   KVDBOplogBlock& block,
                                   bool& found);

    bool _hasExcessBlocks();
    void _pokeReclaimThreadIfNeeded();
    hse::Status _writeCurrentBlkMarker();
    hse::Status _eraseCurrentBlkMarker();

    static hse::Status _readCurrBlockKey(KVDBRecoveryUnit* ru,
                                         KVSHandle& kvs,
                                         KVSHandle& largeKvs,
                                         uint32_t prefix,
                                         KVDBOplogBlock& currBlock,
                                         bool& found);

    static hse::Status _importCurrBlockByScan(KVDBRecoveryUnit* ru,
                                              KVSHandle& kvs,
                                              KVSHandle& largeKvs,
                                              uint32_t prefix,
                                              KVDBOplogBlock& currBlock,
                                              uint32_t blkId);

    void _reset(OperationContext* opctx, KVSHandle& kvs, KVSHandle& largeKvs);

    KVDB& _db;
    KVSHandle& _kvs;
    KVSHandle& _largeKvs;
    uint32_t _prefixVal;

    string _lastDeletedBlockKey;
    string _currentBlockKey;

    int64_t _cappedMaxSize;

    mutable mutex _mutex{};
    deque<KVDBOplogBlock> _blockList{};

    KVDBOplogBlock _currBlock{};

    // parameters and defaults
    uint64_t _maxBlocksToKeep = 100;
    int64_t _minBytesPerBlock = 16 * 1024 * 1024;

    mutex _reclaimMutex{};
    condition_variable _reclaimCv{};
    bool _isDead{false};
};
}
