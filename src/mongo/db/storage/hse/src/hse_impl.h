/**
 *    SPDX-License-Identifier: AGPL-3.0-only
 *
 *    Copyright (C) 2017-2021 Micron Technology, Inc.
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

#include "hse.h"
#include "hse_clienttxn.h"

#include <vector>

using namespace std;

// KVDB interface
namespace hse {

Status init();

Status fini();

// KVDB Implementation
class KVDBImpl : public KVDB {
public:
    virtual Status kvdb_make(const char* kvdb_home, const vector<string>& params);

    virtual Status kvdb_open(const char* kvdb_home, const vector<string>& params);

    virtual Status kvdb_kvs_open(const char* kvs_name,
                                 const vector<string>& params,
                                 KVSHandle& kvs_out);

    virtual Status kvdb_kvs_close(KVSHandle handle);

    virtual struct hse_kvdb* kvdb_handle() {
        return _handle;
    }

    virtual Status kvdb_get_names(unsigned int* count, char*** kvs_list);

    virtual Status kvdb_free_names(char** kvsv);

    virtual Status kvdb_kvs_make(const char* kvs_name, const vector<string>& params);

    virtual Status kvdb_kvs_drop(const char* kvs_name);

    virtual Status kvdb_close();

    virtual Status kvs_put(KVSHandle handle,
                           ClientTxn* txn,
                           const KVDBData& key,
                           const KVDBData& val);

    virtual Status kvs_sub_txn_put(KVSHandle handle, const KVDBData& key, const KVDBData& val);

    virtual Status kvs_put(KVSHandle handle, const KVDBData& key, const KVDBData& val);

    virtual Status kvs_get(
        KVSHandle handle, ClientTxn* txn, const KVDBData& key, KVDBData& val, bool& found);

    virtual Status kvs_probe_key(KVSHandle handle,
                                 ClientTxn* txn,
                                 const KVDBData& key,
                                 bool& found);

    virtual Status kvs_delete(KVSHandle handle, ClientTxn* txn, const KVDBData& key);

    virtual Status kvs_sub_txn_delete(KVSHandle handle, const KVDBData& key);

    virtual Status kvs_prefix_probe(KVSHandle handle,
                                    ClientTxn* txn,
                                    const KVDBData& prefix,
                                    KVDBData& key,
                                    KVDBData& val,
                                    hse_kvs_pfx_probe_cnt& found);

    virtual Status kvs_probe_len(
        KVSHandle handle, ClientTxn* txn, const KVDBData& key, KVDBData& val, bool& found);

    virtual Status kvs_prefix_delete(KVSHandle handle, ClientTxn* txn, const KVDBData& prefix);

    virtual Status kvs_sub_txn_prefix_delete(KVSHandle handle, const KVDBData& prefix);

    virtual Status kvs_iter_delete(KVSHandle handle, ClientTxn* txn, const KVDBData& prefix);

    virtual Status kvdb_sync();

private:
    struct hse_kvdb* _handle = nullptr;
};
}  // namespace hse
