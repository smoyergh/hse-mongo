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

#include <chrono>
#include <iomanip>
#include <string>
#include <thread>

#include "hse.h"
#include "mongo/db/record_id.h"
#include "mongo/platform/basic.h"
#include "mongo/util/time_support.h"

using mongo::RecordId;

namespace mongo {
class KVDBOplogBlockManager;
class KVDBRecoveryUnit;
}

namespace hse {

class KvsCursor;

static const int VALUE_META_SIZE = 4;
static const int VALUE_META_THRESHOLD_LEN = HSE_KVS_VLEN_MAX - VALUE_META_SIZE;

static const std::string KVDB_prefix = string("\0\0\0\0", 4);

static const int DEFAULT_PFX_LEN = 4;
static const int OPLOG_PFX_LEN = 8;
static const int DEFAULT_SFX_LEN = 0;
static const int STDIDX_SFX_LEN = 8;
static const int RS_LOC_LEN = 8;
static const int DUR_LAG = 100;  // 100 ms durability

static const int OPLOG_FANOUT = 4;

static const uint32_t OPLOG_START_BLK = 5;
static const uint32_t OPLOG_META_BLK = 0xFFFFFFFF;

static const string OPLOG_LAST_BLK_DEL_KEY{"last_blk_del"};
static const string OPLOG_CURR_BLK_KEY{"current_blk"};

static const int INVARIANT_SLEEP_MS = 4000;

static const int SUB_TXN_MAX_RETRIES = 200;

//
// Key Generation and Manipulation
// ------------------------------------------------------------------------------
//
// Macros and definitions to deal with record store and oplog keys
//

struct KVDBRecordStoreKey {
    union {
        unsigned char data[20];
        struct {
            uint32_t prefix;
            uint64_t suffix;
            unsigned char pad[8];
        } __attribute__((__packed__)) k12;
        struct {
            uint64_t prefix;
            uint64_t suffix;
            unsigned char pad[4];
        } __attribute__((__packed__)) k16;
    };
} __attribute__((__packed__));

#define KRSK_TYPE_INDEX (19)
#define KRSK_CHUNK_FLAG_INDEX (18)
#define KRSK_TYPE_RS (1U)
#define KRSK_TYPE_OL (2U)

#define KRSK_TYPE(key) ((key).data[KRSK_TYPE_INDEX])

#define KRSK_CHUNK_SZ_INC(key) (((key).data[KRSK_CHUNK_FLAG_INDEX] == 0U) ? 0U : 1U)

#define KRSK_KEY_LEN(key)                                                  \
    ((KRSK_TYPE((key)) == KRSK_TYPE_RS) ? (12U + KRSK_CHUNK_SZ_INC((key))) \
                                        : (16U + KRSK_CHUNK_SZ_INC((key))))

#define KRSK_RS_PREFIX(pfx) (((uint64_t)(pfx)) << 32UL)

#define KRSK_OL_PREFIX(pfx, blk) (((uint64_t)(pfx)) << 32UL | ((uint64_t)blk))

#define KRSK_CLEAR(key) \
    { memset(&((key)), 0, sizeof(struct KVDBRecordStoreKey)); }

#define KRSK_SET_OL_SCAN_KEY(key, pfx, blk)                        \
    {                                                              \
        const uint32_t pfxbe = htobe32(((uint32_t)(pfx)));         \
        const uint32_t blkbe = htobe32(((uint32_t)(blk)));         \
        (key) = (((uint64_t)(blkbe)) << 32UL | ((uint64_t)pfxbe)); \
    }

#define KRSK_SET_PREFIX(key, pfx)                                             \
    {                                                                         \
        if (!((pfx)&0xffffffffUL)) {                                          \
            (key).k12.prefix = htobe32((uint32_t)(((uint64_t)(pfx)) >> 32U)); \
            KRSK_TYPE((key)) = KRSK_TYPE_RS;                                  \
        } else {                                                              \
            (key).k16.prefix = htobe64((pfx));                                \
            KRSK_TYPE((key)) = KRSK_TYPE_OL;                                  \
        }                                                                     \
    }

#define KRSK_SET_SUFFIX(key, sfx)               \
    {                                           \
        if (KRSK_TYPE((key)) == KRSK_TYPE_RS) { \
            (key).k12.suffix = htobe64((sfx));  \
        } else {                                \
            (key).k16.suffix = htobe64((sfx));  \
        }                                       \
    }

#define KRSK_GET_SUFFIX(key) \
    ((KRSK_TYPE((key)) == KRSK_TYPE_RS) ? be64toh((key).k12.suffix) : be64toh((key).k16.suffix))

#define KRSK_SET_CHUNKED(key) \
    { (key).data[KRSK_CHUNK_FLAG_INDEX] = 1; }

#define KRSK_CHUNK_COPY_MASTER(srcKey, dstKey)         \
    {                                                  \
        (dstKey).data[KRSK_CHUNK_FLAG_INDEX] = 1;      \
        if (KRSK_TYPE((srcKey)) == KRSK_TYPE_RS) {     \
            KRSK_TYPE((dstKey)) = KRSK_TYPE_RS;        \
            (dstKey).k12.prefix = (srcKey).k12.prefix; \
            (dstKey).k12.suffix = (srcKey).k12.suffix; \
        } else {                                       \
            KRSK_TYPE((dstKey)) = KRSK_TYPE_OL;        \
            (dstKey).k16.prefix = (srcKey).k16.prefix; \
            (dstKey).k16.suffix = (srcKey).k16.suffix; \
        }                                              \
    }

#define KRSK_SET_CHUNK(key, chunk)              \
    {                                           \
        if (KRSK_TYPE((key)) == KRSK_TYPE_RS) { \
            (key).data[12] = (chunk);           \
        } else {                                \
            (key).data[16] = (chunk);           \
        }                                       \
    }

//
struct KVDBOplogBlockKey {
    union {
        unsigned char data[8];
        struct {
            uint32_t prefix;
            uint32_t blkId;
        } __attribute__((__packed__)) k8;
    };
} __attribute__((__packed__));

#define KOBK_LEN(key) (8)

#define KOBK_SET(key, pfx, blk)                            \
    {                                                      \
        const uint32_t pfxbe = htobe32(((uint32_t)(pfx))); \
        const uint32_t blkbe = htobe32(((uint32_t)(blk))); \
        (key).k8.prefix = pfxbe;                           \
        (key).k8.blkId = blkbe;                            \
    }

//
// ------------------------------------------------------------------------------
//

inline string arrayToHexStr(const char* ar, int len) {
    std::stringstream ss;
    ss << std::hex;
    for (int i = 0; i < len; ++i)
        ss << std::setw(2) << std::setfill('0') << (int)ar[i];
    return ss.str();
}

mongo::Status hseToMongoStatus_slow(const Status& status, const char* prefix);

/**
 * converts HSE status to mongodb status
 */
inline mongo::Status hseToMongoStatus(const Status& status, const char* prefix = NULL) {
    if (MONGO_likely(status.ok())) {
        return mongo::Status::OK();
    }
    return hseToMongoStatus_slow(status, prefix);
}

#define invariantHseSt(expression)                                                          \
    do {                                                                                    \
        auto _invariantHseSt_status = expression;                                           \
        if (MONGO_unlikely(!_invariantHseSt_status.ok())) {                                 \
            mongo::sleepmillis(hse::INVARIANT_SLEEP_MS);                                    \
            mongo::invariantOKFailed(                                                       \
                #expression, hseToMongoStatus(_invariantHseSt_status), __FILE__, __LINE__); \
        }                                                                                   \
    } while (false)

#define invariantHse(expression)                         \
    do {                                                 \
        if (MONGO_unlikely(!(expression))) {             \
            mongo::sleepmillis(hse::INVARIANT_SLEEP_MS); \
            invariant(expression);                       \
        }                                                \
    } while (false)


#define SUB_TXN_OP_RETRY_LOOP_BEGIN               \
    do {                                          \
        struct hse_kvdb_opspec opspec;            \
        ClientTxn cTxn{_handle};                  \
        HSE_KVDB_OPSPEC_INIT(&opspec);            \
        int retries = 0;                          \
        while (retries < SUB_TXN_MAX_RETRIES) {   \
            cTxn.begin();                         \
            opspec.kop_txn = cTxn.get_kvdb_txn(); \
            do


#define SUB_TXN_OP_RETRY_LOOP_END(rval)                       \
    while (false)                                             \
        ;                                                     \
    if ((rval).ok()) {                                        \
        cTxn.commit();                                        \
    } else {                                                  \
        cTxn.abort();                                         \
    }                                                         \
    if ((rval).getErrno() == ECANCELED) {                     \
        if (retries < 4) {                                    \
        } else if (retries < 10) {                            \
            this_thread::sleep_for(chrono::milliseconds(1));  \
        } else if (retries < 100) {                           \
            this_thread::sleep_for(chrono::milliseconds(5));  \
        } else {                                              \
            this_thread::sleep_for(chrono::milliseconds(10)); \
        }                                                     \
        retries++;                                            \
        continue;                                             \
    }                                                         \
    break;                                                    \
    }                                                         \
    }                                                         \
    while (false)                                             \
        ;

static inline RecordId _recordIdFromKey(const KVDBData& key) {
    const uint32_t len = key.len();
    const unsigned char* data = (unsigned char*)key.data();
    uint64_t locVal;

    if (len == (DEFAULT_PFX_LEN + RS_LOC_LEN)) {
        memcpy(&locVal, data + DEFAULT_PFX_LEN, RS_LOC_LEN);
        return RecordId(mongo::endian::bigToNative(locVal));
    } else {
        memcpy(&locVal, data + OPLOG_PFX_LEN, RS_LOC_LEN);
        return RecordId(mongo::endian::bigToNative(locVal));
    }
}

// "inkey" is the key obtained from kvs. It is encoded big endian (omf).
static inline void _krskSetPrefixFromKey(struct KVDBRecordStoreKey& key, const KVDBData& inkey) {
    const uint8_t* data = (uint8_t*)inkey.data();

    if (inkey.len() == (DEFAULT_PFX_LEN + RS_LOC_LEN)) {
        // 4 bytes prefix
        memcpy(&key.k12.prefix, data, sizeof(key.k12.prefix));
        KRSK_TYPE(key) = KRSK_TYPE_RS;
    } else {
        // 8 bytes prefix
        memcpy(&key.k16.prefix, data, sizeof(key.k16.prefix));
        KRSK_TYPE(key) = KRSK_TYPE_OL;
    }
}


static inline RecordId _maxRecordId() {
    return RecordId::max();
}

static inline KVDBData _makeKey(const RecordId& loc, int64_t* storage) {
    *storage = mongo::endian::nativeToBig(loc.repr());
    return KVDBData(reinterpret_cast<const uint8_t*>(storage), sizeof(*storage));
}

static inline string _makeChunkKey(const string& prefix, const RecordId& loc, uint8_t chunk) {
    int64_t bigLoc = mongo::endian::nativeToBig(loc.repr());
    return prefix + std::string(reinterpret_cast<const char*>(&chunk), sizeof(uint8_t)) +
        std::string(reinterpret_cast<const char*>(&bigLoc), sizeof(int64_t));
}

static inline unsigned int _getValueOffset(const KVDBData& value) {
    return value.len() <= VALUE_META_THRESHOLD_LEN ? 0 : VALUE_META_SIZE;
}

static inline unsigned int _getValueLength(const KVDBData& value) {
    if (value.len() <= VALUE_META_THRESHOLD_LEN) {
        return value.len();
    } else {
        // First four bytes are metadata containing the value length.
        return mongo::endian::bigToNative(*(uint32_t*)value.data());
    }
}

static inline unsigned int _getNumChunks(const int len) {
    return (len + VALUE_META_SIZE - 1) / HSE_KVS_VLEN_MAX;
}

hse::Status _cursorRead(mongo::KVDBRecoveryUnit* ru,
                        shared_ptr<mongo::KVDBOplogBlockManager> opBlkMgr,
                        KvsCursor* cursor,
                        KVDBData& key,
                        KVDBData& val,
                        bool& eof);

class CStyleStrVec {
public:
    CStyleStrVec(const vector<string>& strVec) {
        _count = strVec.size();
        _srcV = new const char*[_count];
        for (int i = 0; i < _count; i++) {
            _srcV[i] = strVec[i].c_str();
        }
    }

    CStyleStrVec(const CStyleStrVec&) = delete;
    CStyleStrVec& operator=(const CStyleStrVec& rhs) = delete;
    CStyleStrVec(CStyleStrVec&&) = delete;
    CStyleStrVec& operator=(CStyleStrVec&& rhs) = delete;


    ~CStyleStrVec() {
        delete[] _srcV;
    }

    const char** getCVec() {
        return _srcV;
    }

    int getCount() {
        return _count;
    }

private:
    const char** _srcV{nullptr};
    int _count{0};
};

}  // namespace hse
