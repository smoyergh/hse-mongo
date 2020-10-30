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

#include "hse_clienttxn.h"
#include "hse_util.h"

#include <mutex>
#include <set>

using namespace std;

#ifdef __cplusplus
extern "C" {
#endif

struct hse_kvs_cursor;

#ifdef __cplusplus
}
#endif


// KVDB interface
namespace hse {

class KvsCursor;

KvsCursor* create_cursor(KVSHandle kvs,
                         KVDBData& prefix,
                         bool forward,
                         const struct CompParms& compparms,
                         ClientTxn* lnkd_txn = 0);

class KvsCursor {
public:
    KvsCursor(KVSHandle kvs,
              KVDBData& prefix,
              bool forward,
              ClientTxn* lnkd_txn,
              const struct CompParms& compparms);

    virtual ~KvsCursor();

    virtual Status update(ClientTxn* lnkd_txn = 0);

    virtual Status seek(const KVDBData& key, const KVDBData* kmax, KVDBData* posKey);

    virtual Status read(KVDBData& key, KVDBData& val, bool& eof);

    virtual Status save();

    virtual Status restore();

protected:
    void _kvs_cursor_create(ClientTxn* lnkd_txn);
    int _read_kvs(bool& eof);

    struct hse_kvs* _kvs;  // not owned
    KVDBData _pfx;
    bool _forward{true};

    struct hse_kvs_cursor* _cursor;
    int _start;
    int _end;
    int _curr;

    // Last read key. If null, this cursor was just created.
    const void* _kvs_key;
    size_t _kvs_klen;

    // If last operation was a seek, this will store the key we landed on.
    // Used when recreating a cursor in update.
    // Using a separate variable avoids the issue with a stale key in _kvs_key if
    // _kvs_key is overloaded for this use as well.
    const void* _kvs_seek_key;
    size_t _kvs_seek_klen;

    const void* _kvs_val;
    // Because we are in the context of a cursor if the value is multi chunks,
    // _kvs_val applies to the first chunk only.
    //
    // If the value was not compressed, first byte of the first chunk stored in kvs,
    // may be the 4 bytes length header if multichunk or the first byte user data if one
    // chunk. _kvs_vlen is the length of the first chunk.
    //
    // If the value was compressed and was multichunks, points on the first byte of
    // the first chunk stored in kvs (aka points 4 bytes length header followed by
    // compression headers and then compressed value.
    // kvs_len is the length of that first chunk (containing headers and
    // compressed value first chunk).
    //
    // If the value was compressed and < 1 chunk, _kvs_val and _kvs_vlen do NOT
    // reflect the value as stored in kvs. _kvs_val points on the first
    // byte of user data (decompressed). _kvs_vlen is the length of the value
    // uncompressed.

    size_t _kvs_vlen;
    unique_ptr<unsigned char[]> _uncompressed_val{};
    // Used to free automatically the buffer allocated to hold the de-compression output.
    // If the compression is not used, it doesn't point on any buffer.
    // If compression is used, it points on the same buffer as _kvs_val point to.

    bool _kvs_eof;
    unsigned int _kvs_num_chunks;
    unsigned int
        _offset;  // Offset of the first byte of the user value [un compressed] in the final
                  // buffer passed to mongo.
    unsigned long
        _total_len;  // Always the length of the whole user value [un compressed]. If the value
                     // was chunked, that includes all the chunks.

    unsigned long _kvs_total_len_comp;
    // Length of the compressed whole value. It is also the length of the value as stored
    // in kvs. Does not contain the 4 bytes length header but contains the compression
    // headers. If compression is off, it is the same as _total_len.

    struct CompParms _compparms;
};
}
