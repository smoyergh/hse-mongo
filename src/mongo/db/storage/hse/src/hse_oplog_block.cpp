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
#include "mongo/util/log.h"

#include "hse_oplog_block.h"
#include "hse_util.h"


using namespace std;

using hse::OPLOG_PFX_LEN;
using hse::OPLOG_START_BLK;
using hse::OPLOG_META_BLK;
using hse::OPLOG_LAST_BLK_DEL_KEY;
using hse::OPLOG_CURR_BLK_KEY;

using hse::KVDBRecordStoreKey;
using hse::KVDBOplogBlockKey;
using hse::_getNumChunks;
using hse::_getValueLength;
using hse::_makeChunkKey;
using hse::CompParms;

namespace mongo {

KVDBOplogBlockManager::KVDBOplogBlockManager(OperationContext* opctx,
                                             KVDB& db,
                                             KVSHandle& kvs,
                                             KVSHandle& largeKvs,
                                             uint32_t prefix,
                                             int64_t cappedMaxSize)
    : _db{db}, _kvs{kvs}, _largeKvs(largeKvs), _prefixVal{prefix}, _cappedMaxSize{cappedMaxSize} {

    _lastDeletedBlockKey = _computeLastBlockDeletedKey(_prefixVal);
    _currentBlockKey = _computeCurrentBlockKey(_prefixVal);

    const unsigned long long kMinBlocksToKeep = 10ULL;
    const unsigned long long kMaxBlocksToKeep = 100ULL;

    unsigned long long numBlocks = _cappedMaxSize / BSONObjMaxInternalSize;
    _maxBlocksToKeep = std::min(kMaxBlocksToKeep, std::max(kMinBlocksToKeep, numBlocks));
    _minBytesPerBlock = _cappedMaxSize / _maxBlocksToKeep;
    invariantHse(_minBytesPerBlock > 0);

    LOG(1) << "OPDBG: cappedMaxSize = " << cappedMaxSize;
    LOG(1) << "OPDBG: _maxBlocksToKeep = " << _maxBlocksToKeep;
    LOG(1) << "OPDBG: _minBytesPerBlock = " << _minBytesPerBlock;

    // this also sets the current block values
    importBlocks(opctx, prefix, _blockList, _currBlock);

    // erase the current block marker
    hse::Status st = _eraseCurrentBlkMarker();
    invariantHseSt(st);

    _pokeReclaimThreadIfNeeded();  // Reclaim stones if over the limit.
}

KVDBOplogBlockManager::~KVDBOplogBlockManager() {
    // record current block for imports
    hse::Status st;

    st = _writeCurrentBlkMarker();
    invariantHseSt(st);
}

string KVDBOplogBlockManager::getCurrentBlockId() {
    lock_guard<mutex> lk{_mutex};

    return _getCurrentBlockId();
}

RecordId KVDBOplogBlockManager::getHighestSeenLoc() {
    lock_guard<mutex> lk{_mutex};

    return _currBlock.highestRec;
}

uint32_t KVDBOplogBlockManager::getBlockId(const RecordId& loc) {
    // find the lower bound
    lock_guard<mutex> lk{_mutex};

    KVDBOplogBlock cmpBlock;
    cmpBlock.highestRec = loc;

    auto bIter = std::lower_bound(_blockList.begin(), _blockList.end(), cmpBlock);

    if (bIter != _blockList.end()) {
        return bIter->blockId;
    } else {
        return _currBlock.blockId;
    }
}

uint32_t KVDBOplogBlockManager::getBlockIdToInsert(const RecordId& loc) {
    // find the lower bound
    lock_guard<mutex> lk{_mutex};

    if (!_blockList.empty() && loc <= _blockList.back().highestRec) {
        KVDBOplogBlock cmpBlock;
        cmpBlock.highestRec = loc;

        auto bIter = std::lower_bound(_blockList.begin(), _blockList.end(), cmpBlock);

        invariantHse(bIter != _blockList.end());

        return bIter->blockId;
    }

    return _currBlock.blockId;
}

uint32_t KVDBOplogBlockManager::getBlockIdToInsertAndGrow(const RecordId& loc,
                                                          int64_t nRecs,
                                                          int64_t size) {
    // find the lower bound
    lock_guard<mutex> lk{_mutex};

    if (!_blockList.empty() && loc <= _blockList.back().highestRec) {
        KVDBOplogBlock cmpBlock;
        cmpBlock.highestRec = loc;

        auto bIter = std::lower_bound(_blockList.begin(), _blockList.end(), cmpBlock);

        invariantHse(bIter != _blockList.end());

        bIter->sizeInBytes.addAndFetch(size);
        bIter->numRecs.addAndFetch(nRecs);

        return bIter->blockId;
    }

    // update current block

    // update last rec to max seen.
    if (loc > _currBlock.highestRec) {
        _currBlock.highestRec = loc;
    }
    _currBlock.sizeInBytes.addAndFetch(size);
    _currBlock.numRecs.addAndFetch(nRecs);

    uint32_t retBlk = _currBlock.blockId;

    // Grow block list if needed.
    if (_currBlock.sizeInBytes.load() >= _minBytesPerBlock) {
        hse::Status st;
        KVDBOplogBlock nBlk{};

        nBlk.blockId = _getNextBlockId(_currBlock.blockId);

        // write marker for current block
        st = _writeMarker(_currBlock);
        invariantHseSt(st);

        _blockList.push_back(_currBlock);
        _currBlock = nBlk;

        _pokeReclaimThreadIfNeeded();
    }

    return retBlk;
}

Status KVDBOplogBlockManager::truncate(OperationContext* opctx) {
    __attribute__((aligned(16))) struct KVDBOplogBlockKey blockKey;
    hse::Status st;
    lock_guard<mutex> lk{_mutex};
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    for (auto& block : _blockList) {
        KOBK_SET(blockKey, _prefixVal, block.blockId);
        KVDBData prefixKey{blockKey.data, KOBK_LEN(blockKey)};

        st = ru->prefixDelete(_kvs, prefixKey);
        invariantHseSt(st);
        st = ru->prefixDelete(_largeKvs, prefixKey);
        invariantHseSt(st);
    }

    // delete current block
    KOBK_SET(blockKey, _prefixVal, _currBlock.blockId);
    KVDBData prefixKey{blockKey.data, KOBK_LEN(blockKey)};

    st = ru->prefixDelete(_kvs, prefixKey);
    invariantHseSt(st);
    st = ru->prefixDelete(_largeKvs, prefixKey);
    invariantHseSt(st);

    // reset blockList
    _reset(opctx);

    return Status::OK();
}

void KVDBOplogBlockManager::_reset(OperationContext* opctx) {
    _blockList.clear();
    _currBlock = KVDBOplogBlock{};
}

Status KVDBOplogBlockManager::cappedTruncateAfter(OperationContext* opctx,
                                                  const RecordId& end,
                                                  bool inclusive,
                                                  RecordId& lastKeptId,
                                                  int64_t& numRecsDel,
                                                  int64_t& sizeDel) {
    hse::Status st;

    // lock
    lock_guard<mutex> lk{_mutex};

    // Find if in the full block list
    bool inFullList = false;
    KVDBOplogBlock cmpBlock{};

    cmpBlock.highestRec = end;
    auto bIter = std::lower_bound(_blockList.begin(), _blockList.end(), cmpBlock);

    if (bIter != _blockList.end() && *bIter == cmpBlock && !inclusive)
        ++bIter;

    if (bIter != _blockList.end())
        inFullList = true;

    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);
    KVDBOplogBlock* firstBlock = nullptr;

    if (inFullList) {
        firstBlock = &(*bIter);
    } else {
        firstBlock = &_currBlock;
    }

    // find lastKeptId
    if (inclusive) {
        auto st = _findLastKeptIdInclusive(ru, lastKeptId, end, *firstBlock);
        invariantHseSt(st);
    } else {
        lastKeptId = end;
    }

    // add the whole block, later we subtract the updated values to reflect the size/recs deleted
    numRecsDel += firstBlock->numRecs.load();
    sizeDel += firstBlock->sizeInBytes.load();

    st = _deleteBlockByScan(ru, *firstBlock, end, inclusive);
    invariantHseSt(st);

    numRecsDel -= firstBlock->numRecs.load();
    sizeDel -= firstBlock->sizeInBytes.load();

    if (inFullList) {
        _blockList.erase(bIter);
        auto savedIter = bIter;
        for (; _blockList.end() != bIter; bIter++) {
            st = deleteBlock(ru, false, _prefixVal, *bIter);
            invariantHseSt(st);
            numRecsDel += bIter->numRecs.load();
            sizeDel += bIter->sizeInBytes.load();
        }

        // delete the entries
        _blockList.erase(savedIter, _blockList.end());

        // delete current block
        st = deleteBlock(ru, false, _prefixVal, _currBlock);
        invariantHseSt(st);
        numRecsDel += _currBlock.numRecs.load();
        sizeDel += _currBlock.sizeInBytes.load();
    }

    // update current Block
    _currBlock = *firstBlock;
    _currBlock.highestRec = lastKeptId;

    // if in full list - then erase the marker for that block to demote it to current.
    if (inFullList) {
        st = _deleteMarker(_currBlock.blockId);
        invariantHseSt(st);
    }

    return Status::OK();
}

// static
hse::Status KVDBOplogBlockManager::cursorRead(
    KVDBRecoveryUnit* ru, KvsCursor* cursor, KVDBData& key, KVDBData& val, bool& eof) {
    hse::Status st;

    // do a read
    eof = false;
    st = ru->cursorRead(cursor, key, val, eof);
    if (!st.ok() || eof)
        return st;

    // silently skip markers
    while (key.len() == OPLOG_PFX_LEN) {
        st = ru->cursorRead(cursor, key, val, eof);
        if (!st.ok() || eof)
            return st;
    }

    return st;
}

void KVDBOplogBlockManager::awaitHasExcessBlocksOrDead() {
    // Wait until stop() is called or there are too many oplog blocks.
    unique_lock<mutex> lk(_reclaimMutex);
    while (!_isDead && !_hasExcessBlocks())
        _reclaimCv.wait(lk);
}

boost::optional<KVDBOplogBlock> KVDBOplogBlockManager::getOldestBlockIfExcess() {
    lock_guard<mutex> lk{_mutex};

    if (!_hasExcessBlocks())
        return {};

    return _blockList.front();
}

void KVDBOplogBlockManager::stop() {
    lock_guard<mutex> lk{_reclaimMutex};
    _isDead = true;
}

bool KVDBOplogBlockManager::isDead() {
    lock_guard<mutex> lk{_reclaimMutex};

    return _isDead;
}

void KVDBOplogBlockManager::removeOldestBlock() {
    lock_guard<mutex> lk{_mutex};
    _blockList.pop_front();
}

// static
hse::Status KVDBOplogBlockManager::deleteBlock(KVDBRecoveryUnit* ru,
                                               bool usePdel,
                                               uint32_t prefix,
                                               const KVDBOplogBlock& block) {
    __attribute__((aligned(16))) struct KVDBOplogBlockKey blockKey;
    hse::Status st;
    KOBK_SET(blockKey, prefix, block.blockId);
    KVDBData pfxToDel{blockKey.data, KOBK_LEN(blockKey)};

    st = usePdel ? ru->prefixDelete(_kvs, pfxToDel) : ru->iterDelete(_kvs, pfxToDel);
    if (!st.ok())
        return st;

    st = usePdel ? ru->prefixDelete(_largeKvs, pfxToDel) : ru->iterDelete(_largeKvs, pfxToDel);
    if (!st.ok())
        return st;

    return hse::Status{};
}

hse::Status KVDBOplogBlockManager::updateLastBlkDeleted(KVDBRecoveryUnit* ru, uint32_t blockId) {
    KVDBData key{_lastDeletedBlockKey};
    uint32_t beBlkId = endian::nativeToBig(blockId);
    KVDBData val{(const uint8_t*)&beBlkId, sizeof(uint32_t)};

    return ru->put(_largeKvs, key, val);
}

// static
void KVDBOplogBlockManager::importBlocks(OperationContext* opctx,
                                         uint32_t prefix,
                                         deque<KVDBOplogBlock>& blockList,
                                         KVDBOplogBlock& currBlock) {
    hse::Status st;

    // if last id key present then
    //    first block is lastid + 1
    // else
    //    first block = start block
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);
    uint32_t lastDelBlk = 0;
    uint32_t firstBlkId = OPLOG_START_BLK;  // default
    bool found = false;

    string lastDeletedBlockKey = _computeLastBlockDeletedKey(prefix);

    st = _readLastDeletedBlockId(ru, lastDeletedBlockKey, lastDelBlk, found);
    invariantHseSt(st);

    if (found)
        firstBlkId = lastDelBlk + 1;

    blockList.clear();

    // read all marker blocks
    uint32_t blkToRead = firstBlkId;
    found = true;
    while (true) {
        KVDBOplogBlock blockRead{};
        st = _readMarker(opctx, prefix, blkToRead, blockRead, found);
        invariantHseSt(st);

        if (!found)
            break;

        blockList.push_back(blockRead);
        blkToRead++;
    }

    // if current block id is present then
    //    import current block
    // else
    //    scan current block by assuming
    //    current block = last read marker block + 1
    found = false;
    st = _readCurrBlockKey(ru, prefix, currBlock, found);
    invariantHseSt(st);

    if (!found) {
        st = _importCurrBlockByScan(ru, prefix, currBlock, blkToRead);
        invariantHseSt(st);
    }
}

// static
void KVDBOplogBlockManager::dropAllBlocks(OperationContext* opctx, uint32_t prefix) {
    __attribute__((aligned(16))) struct KVDBOplogBlockKey blockKey;
    deque<KVDBOplogBlock> blockList;
    KVDBOplogBlock currBlock{};
    hse::Status st;

    // discover blocks
    importBlocks(opctx, prefix, blockList, currBlock);

    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    // delete all blocks
    for (auto& block : blockList) {
        KOBK_SET(blockKey, prefix, block.blockId);
        KVDBData delPfx{blockKey.data, KOBK_LEN(blockKey)};

        st = ru->prefixDelete(_kvs, delPfx);
        invariantHseSt(st);
        st = ru->prefixDelete(_largeKvs, delPfx);
        invariantHseSt(st);
    }

    if (currBlock.numRecs.load()) {
        KOBK_SET(blockKey, prefix, currBlock.blockId);
        KVDBData delPfx{blockKey.data, KOBK_LEN(blockKey)};

        st = ru->prefixDelete(_kvs, delPfx);
        invariantHseSt(st);
        st = ru->prefixDelete(_largeKvs, delPfx);
        invariantHseSt(st);
    }
}

size_t KVDBOplogBlockManager::numBlocks() const {
    lock_guard<mutex> lk{_mutex};

    return (_blockList.size());
}

int64_t KVDBOplogBlockManager::currentBytes() const {

    return _currBlock.sizeInBytes.load();
}

int64_t KVDBOplogBlockManager::currentRecords() const {

    return _currBlock.numRecs.load();
}


void KVDBOplogBlockManager::setMinBytesPerBlock(int64_t size) {
    invariantHse(size > 0);
    lock_guard<mutex> lk{_mutex};

    // Only allow changing the minimum bytes per stone if no data has been inserted.
    invariantHse(_blockList.size() == 0 && _currBlock.numRecs.load() == 0);
    _minBytesPerBlock = size;
}

void KVDBOplogBlockManager::setMaxBlocksToKeep(size_t numBlocks) {
    invariantHse(numBlocks > 0);
    lock_guard<mutex> lk{_mutex};

    // Only allow changing the number of stones to keep if no data has been inserted.
    invariantHse(_blockList.size() == 0 && _currBlock.numRecs.load() == 0);
    _maxBlocksToKeep = numBlocks;
}

RecordId KVDBOplogBlockManager::getHighestFromPrevBlk(OperationContext* opctx, uint32_t blkId) {
    __attribute__((aligned(16))) struct KVDBOplogBlockKey blockKey;
    hse::Status st;

    // we can assume its in the block list since it is for a previous Blk
    if (_blockList.empty()) {
        return RecordId{};
    } else if (blkId == _blockList.front().blockId) {
        return RecordId{};
    }

    uint32_t prevBlkId = blkId - 1;

    KVDBOplogBlock cmpBlock;
    cmpBlock.blockId = prevBlkId;

    auto bIter = std::lower_bound(
        _blockList.begin(), _blockList.end(), cmpBlock, KVDBOplogBlock::cmpWithBlkId);

    if (bIter != _blockList.end()) {
        // Iterate over the block
        KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);
        KOBK_SET(blockKey, _prefixVal, bIter->blockId);
        KVDBData pfxKey{blockKey.data, KOBK_LEN(blockKey)};
        KvsCursor* cursor = 0;

        // forward
        const struct hse::CompParms compparms {};  // no compression for oplog
        st = ru->beginScan(_kvs, pfxKey, true, &cursor, compparms);
        invariantHseSt(st);

        bool eof = false;
        RecordId lastRec{};
        KVDBData elKey{};
        KVDBData elVal{};
        while (!eof) {
            st = cursorRead(ru, cursor, elKey, elVal, eof);
            invariantHseSt(st);

            if (eof)
                break;

            lastRec = _recordIdFromKey(elKey);
        }

        ru->endScan(cursor);

        invariantHse(lastRec.isNormal());

        return lastRec;
    } else {
        // should never be here
        invariantHse(false);
    }
}


// -- begin private

// static
string KVDBOplogBlockManager::_computeLastBlockDeletedKey(uint32_t prefix) {
    uint32_t bePrefix = endian::nativeToBig(prefix);
    string result;

    result.reserve(40);
    result.append(std::string(reinterpret_cast<const char*>(&bePrefix), sizeof(uint32_t)));
    result.append(KVDBOplogBlock::blockIdToStr(OPLOG_META_BLK));
    result.append(OPLOG_LAST_BLK_DEL_KEY);

    return result;
}

// static
string KVDBOplogBlockManager::_computeCurrentBlockKey(uint32_t prefix) {
    uint32_t bePrefix = endian::nativeToBig(prefix);
    string result;

    result.reserve(40);
    result.append(std::string(reinterpret_cast<const char*>(&bePrefix), sizeof(uint32_t)));
    result.append(KVDBOplogBlock::blockIdToStr(OPLOG_META_BLK));
    result.append(OPLOG_CURR_BLK_KEY);

    return result;
}

// static
hse::Status KVDBOplogBlockManager::_readLastDeletedBlockId(KVDBRecoveryUnit* ru,
                                                           string key,
                                                           uint32_t& lastBlockId,
                                                           bool& found) {
    KVDBData compatKey{key};
    KVDBData val{};
    hse::Status st;

    st = ru->getMCo(_largeKvs, compatKey, val, found);
    if (!st.ok() || !found)
        return st;

    invariantHse(val.len() == sizeof(uint32_t));
    lastBlockId = endian::bigToNative(*(uint32_t*)val.data());

    return {};
}

uint32_t KVDBOplogBlockManager::_getNextBlockId(uint32_t prevId) {
    return ++prevId;
}

string KVDBOplogBlockManager::_getCurrentBlockId() {
    return KVDBOplogBlock::blockIdToStr(_currBlock.blockId);
}

hse::Status KVDBOplogBlockManager::_findLastKeptIdInclusive(KVDBRecoveryUnit* ru,
                                                            RecordId& lastKeptId,
                                                            const RecordId& end,
                                                            KVDBOplogBlock& block) {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey seekKey;
    hse::Status st;
    KvsCursor* cursor = 0;
    uint64_t olScanKey;

    KRSK_SET_OL_SCAN_KEY(olScanKey, _prefixVal, block.blockId);

    KVDBData pfxKey{(const uint8_t*)&olScanKey, sizeof(olScanKey)};

    // reverse
    const struct CompParms compparms = {};  // no compression for oplog
    st = ru->beginScan(_kvs, pfxKey, false, &cursor, compparms);
    if (!st.ok())
        return st;

    // seek to key
    KRSK_CLEAR(seekKey);
    KRSK_SET_PREFIX(seekKey, KRSK_OL_PREFIX(_prefixVal, block.blockId));
    KRSK_SET_SUFFIX(seekKey, end.repr());
    KVDBData compatKey{seekKey.data, KRSK_KEY_LEN(seekKey)};
    KVDBData foundKey{};
    KVDBData elKey{};
    KVDBData elVal{};
    bool eof = false;

    st = ru->cursorSeek(cursor, compatKey, &foundKey);
    if (!st.ok()) {
        ru->endScan(cursor);
        return st;
    }
    invariantHse(foundKey == compatKey);

    // consume read
    st = cursorRead(ru, cursor, elKey, elVal, eof);
    if (!st.ok()) {
        ru->endScan(cursor);
        return st;
    }
    invariantHse(!eof);
    invariantHse(elKey == compatKey);

    st = cursorRead(ru, cursor, elKey, elVal, eof);
    if (!st.ok()) {
        ru->endScan(cursor);
        return st;
    }

    lastKeptId = eof ? RecordId{} : _recordIdFromKey(elKey);

    ru->endScan(cursor);

    return hse::Status{};
}

hse::Status KVDBOplogBlockManager::_deleteBlockByScan(KVDBRecoveryUnit* ru,
                                                      KVDBOplogBlock& block,
                                                      const RecordId& start,
                                                      bool inclusive) {
    __attribute__((aligned(16))) struct hse::KVDBRecordStoreKey seekKey;
    hse::Status st;
    uint64_t olScanKey;

    KRSK_SET_OL_SCAN_KEY(olScanKey, _prefixVal, block.blockId);

    KVDBData pfxKey{(const uint8_t*)&olScanKey, sizeof(olScanKey)};
    KvsCursor* cursor = 0;
    const struct CompParms compparms = {};  // no compression for oplog

    // forward
    st = ru->beginScan(_kvs, pfxKey, true, &cursor, compparms);
    if (!st.ok())
        return st;

    // seek to start key
    KRSK_CLEAR(seekKey);
    KRSK_SET_PREFIX(seekKey, KRSK_OL_PREFIX(_prefixVal, block.blockId));
    KRSK_SET_SUFFIX(seekKey, start.repr());
    KVDBData compatKey{seekKey.data, KRSK_KEY_LEN(seekKey)};
    KVDBData foundKey{};
    KVDBData elKey{};
    KVDBData elVal{};
    bool eof = false;
    int64_t sizeDel = 0;
    int64_t recsDel = 0;

    st = ru->cursorSeek(cursor, compatKey, &foundKey);
    if (!st.ok()) {
        ru->endScan(cursor);
        return st;
    }

    if (foundKey == compatKey) {
        st = cursorRead(ru, cursor, elKey, elVal, eof);
        if (!st.ok()) {
            ru->endScan(cursor);
            return st;
        }
    }

    if (inclusive) {
        invariantHse(!eof);
        st = _delKeyHelper(ru, elKey, elVal.getNumChunks());
        if (!st.ok()) {
            ru->endScan(cursor);
            return st;
        }

        sizeDel += elVal.getTotalLen();
        recsDel++;
    }

    while (!eof) {
        st = cursorRead(ru, cursor, elKey, elVal, eof);
        if (!st.ok()) {
            ru->endScan(cursor);
            return st;
        }

        if (eof)
            break;

        st = _delKeyHelper(ru, elKey, elVal.getNumChunks());
        if (!st.ok()) {
            ru->endScan(cursor);
            return st;
        }

        sizeDel += elVal.getTotalLen();
        recsDel++;
    }

    // Adjust block size and recs
    if (block.sizeInBytes.subtractAndFetch(sizeDel) < 0)
        block.sizeInBytes.store(0);

    if (block.numRecs.subtractAndFetch(recsDel) < 0)
        block.numRecs.store(0);

    ru->endScan(cursor);

    return hse::Status{};
}

hse::Status KVDBOplogBlockManager::_delKeyHelper(KVDBRecoveryUnit* ru,
                                                 KVDBData& key,
                                                 uint32_t num_chunks) {
    __attribute__((aligned(16))) struct KVDBRecordStoreKey chunkKey;
    hse::Status st;

    st = ru->del(_kvs, key);
    if (!st.ok())
        return st;

    if (num_chunks == 0)
        return st;

    // Delete the remaining chunks.
    RecordId id = _recordIdFromKey(key);
    KRSK_CLEAR(chunkKey);
    KRSK_SET_PREFIX(chunkKey, KRSK_OL_PREFIX(_prefixVal, getBlockId(id)));
    KRSK_SET_SUFFIX(chunkKey, id.repr());
    KRSK_SET_CHUNKED(chunkKey);

    for (uint32_t chunk = 0; chunk < num_chunks; ++chunk) {
        KRSK_SET_CHUNK(chunkKey, chunk);
        st = ru->del(_largeKvs, KVDBData{chunkKey.data, KRSK_KEY_LEN(chunkKey)});
        if (!st.ok())
            return st;
    }

    return st;
}

hse::Status KVDBOplogBlockManager::_writeMarker(const KVDBOplogBlock& block) {
    __attribute__((aligned(16))) struct KVDBOplogBlockKey blockKey;

    KOBK_SET(blockKey, _prefixVal, block.blockId);
    KVDBData key{blockKey.data, KOBK_LEN(blockKey)};

    string valStr = KVDBOplogBlock::blockToBuf(block);
    KVDBData val{valStr};

    // insert marker for new block
    return _db.kvs_sub_txn_put(_kvs, key, val);
}

hse::Status KVDBOplogBlockManager::_deleteMarker(uint32_t blockId) {
    __attribute__((aligned(16))) struct KVDBOplogBlockKey blockKey;

    KOBK_SET(blockKey, _prefixVal, blockId);
    KVDBData key{blockKey.data, KOBK_LEN(blockKey)};

    // delete marker for block
    return _db.kvs_sub_txn_delete(_kvs, key);
}

// static
hse::Status KVDBOplogBlockManager::_readMarker(
    OperationContext* opctx, uint32_t prefix, uint32_t blkId, KVDBOplogBlock& block, bool& found) {
    __attribute__((aligned(16))) struct KVDBOplogBlockKey blockKey;
    hse::Status st;
    KVDBRecoveryUnit* ru = KVDBRecoveryUnit::getKVDBRecoveryUnit(opctx);

    KOBK_SET(blockKey, prefix, blkId);

    KVDBData key{blockKey.data, KOBK_LEN(blockKey)};
    KVDBData val{};

    found = false;
    st = ru->getMCo(_kvs, key, val, found);
    if (!st.ok() || !found)
        return st;

    block = KVDBOplogBlock::bufToBlock(val.data(), val.len());

    return {};
}

bool KVDBOplogBlockManager::_hasExcessBlocks() {
    return _blockList.size() > _maxBlocksToKeep;
}

void KVDBOplogBlockManager::_pokeReclaimThreadIfNeeded() {
    if (_hasExcessBlocks())
        _reclaimCv.notify_one();
}

hse::Status KVDBOplogBlockManager::_writeCurrentBlkMarker() {
    KVDBData compatKey{_currentBlockKey};
    string valStr;

    valStr = KVDBOplogBlock::blockToBuf(_currBlock);
    KVDBData val{valStr};

    return _db.kvs_sub_txn_put(_largeKvs, compatKey, val);
}

hse::Status KVDBOplogBlockManager::_eraseCurrentBlkMarker() {
    KVDBData compatKey{_currentBlockKey};

    return _db.kvs_sub_txn_delete(_largeKvs, compatKey);
}

// static
hse::Status KVDBOplogBlockManager::_readCurrBlockKey(KVDBRecoveryUnit* ru,
                                                     uint32_t prefix,
                                                     KVDBOplogBlock& currBlock,
                                                     bool& found) {
    hse::Status st;
    string tmpStr = _computeCurrentBlockKey(prefix);
    KVDBData compatKey{tmpStr};
    KVDBData val{};

    found = false;
    st = ru->getMCo(_largeKvs, compatKey, val, found);
    if (!st.ok() || !found)
        return st;

    currBlock = KVDBOplogBlock::bufToBlock(val.data(), val.len());

    return st;
}

hse::Status KVDBOplogBlockManager::_importCurrBlockByScan(KVDBRecoveryUnit* ru,
                                                          uint32_t prefix,
                                                          KVDBOplogBlock& currBlock,
                                                          uint32_t blkId) {

    __attribute__((aligned(16))) struct KVDBOplogBlockKey blockKey;
    hse::Status st;
    KOBK_SET(blockKey, prefix, blkId);
    KVDBData pfxKey{blockKey.data, KOBK_LEN(blockKey)};
    KvsCursor* cursor = 0;

    const struct CompParms compparms = {};  // no compression for oplog
    st = ru->beginScan(_kvs, pfxKey, true, &cursor, compparms);
    if (!st.ok())
        return st;

    currBlock.blockId = blkId;

    KVDBData elKey{};
    KVDBData elVal{};
    bool eof = false;
    int64_t numRecs = 0;
    int64_t sizeBytes = 0;

    RecordId loc{};

    while (true) {
        st = KVDBOplogBlockManager::cursorRead(ru, cursor, elKey, elVal, eof);
        if (!st.ok()) {
            ru->endScan(cursor);
            return st;
        }

        if (eof)
            break;

        loc = _recordIdFromKey(elKey);
        if (currBlock.highestRec < loc)
            currBlock.highestRec = loc;

        invariantHse(elKey.len() > pfxKey.len());

        numRecs++;
        sizeBytes += elVal.len();
    }

    st = ru->endScan(cursor);

    currBlock.numRecs.store(numRecs);
    currBlock.sizeInBytes.store(sizeBytes);

    return st;
}
}
