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

KvsCursor* create_cursor(KVSHandle kvs, KVDBData& prefix, bool forward, ClientTxn* lnkd_txn = 0);

class KvsCursor {
public:
    KvsCursor(KVSHandle kvs, KVDBData& prefix, bool forward, ClientTxn* lnkd_txn);

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
    size_t _kvs_vlen;
};
}
