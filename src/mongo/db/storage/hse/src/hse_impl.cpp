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

#include "hse_clienttxn.h"
#include "hse_impl.h"
#include "hse_stats.h"
#include "hse_util.h"

using namespace std;
using mongo::warning;

// stats
using hse_stat::_hseKvsGetLatency;
using hse_stat::_hseKvsGetCounter;
using hse_stat::_hseKvsCursorCreateLatency;
using hse_stat::_hseKvsCursorCreateCounter;
using hse_stat::_hseKvsCursorReadLatency;
using hse_stat::_hseKvsCursorReadCounter;
using hse_stat::_hseKvsCursorDestroyLatency;
using hse_stat::_hseKvsCursorDestroyCounter;
using hse_stat::_hseKvsPutLatency;
using hse_stat::_hseKvsPutCounter;
using hse_stat::_hseKvsProbeLatency;
using hse_stat::_hseKvsProbeCounter;
using hse_stat::_hseKvdbSyncLatency;
using hse_stat::_hseKvdbSyncCounter;
using hse_stat::_hseKvsDeleteLatency;
using hse_stat::_hseKvsDeleteCounter;
using hse_stat::_hseKvsPrefixDeleteLatency;
using hse_stat::_hseKvsPrefixDeleteCounter;


// KVDB interface
namespace hse {

struct hse_kvdb_txn;

// KVDB Implementation
Status KVDBImpl::kvdb_init() {
    return Status(::hse_kvdb_init());
}

Status KVDBImpl::kvdb_fini() {
    ::hse_kvdb_fini();
    return Status();
}

Status KVDBImpl::kvdb_make(const char* mp_name, const char* kvdb_name, struct hse_params* params) {
    return Status(::hse_kvdb_make(mp_name, params));
}

Status KVDBImpl::kvdb_open(const char* mp_name, const char* kvdb_name, struct hse_params* params) {
    auto st = ::hse_kvdb_open(mp_name, params, &_handle);
    return Status(st);
}

Status KVDBImpl::kvdb_kvs_open(const char* kvs_name,
                               struct hse_params* params,
                               KVSHandle& kvs_out) {
    struct hse_kvs* kvsH = nullptr;
    auto st = ::hse_kvdb_kvs_open(_handle, kvs_name, params, &kvsH);
    kvs_out = (KVSHandle)kvsH;
    return st;
}

Status KVDBImpl::kvdb_kvs_close(KVSHandle handle) {
    struct hse_kvs* kvsH = (struct hse_kvs*)handle;
    return Status(::hse_kvdb_kvs_close(kvsH));
}

Status KVDBImpl::kvdb_get_names(unsigned int* count, char*** kvs_list) {
    return Status(::hse_kvdb_get_names(_handle, count, kvs_list));
}

Status KVDBImpl::kvdb_free_names(char** kvsv) {
    ::hse_kvdb_free_names(_handle, kvsv);
    return Status();
}

Status KVDBImpl::kvdb_kvs_make(const char* kvs_name, struct hse_params* params) {
    return Status(::hse_kvdb_kvs_make(_handle, kvs_name, params));
}

Status KVDBImpl::kvdb_kvs_drop(const char* kvs_name) {
    return Status(::hse_kvdb_kvs_drop(_handle, kvs_name));
}

Status KVDBImpl::kvdb_close() {
    int ret = ::hse_kvdb_close(_handle);
    _handle = nullptr;
    return Status(ret);
}

Status KVDBImpl::kvs_put(KVSHandle handle,
                         ClientTxn* txn,
                         const KVDBData& key,
                         const KVDBData& val) {
    struct hse_kvs* kvs = (struct hse_kvs*)handle;
    struct hse_kvdb_opspec opspec;

    HSE_KVDB_OPSPEC_INIT(&opspec);
    opspec.kop_txn = txn ? txn->get_kvdb_txn() : 0;

    _hseKvsPutCounter.add();
    auto lt = _hseKvsPutLatency.begin();
    Status ret{::hse_kvs_put(kvs, &opspec, key.data(), key.len(), val.data(), val.len())};
    _hseKvsPutLatency.end(lt);
    return ret;
}

Status KVDBImpl::kvs_put(KVSHandle handle, const KVDBData& key, const KVDBData& val) {
    struct hse_kvs* kvs = (struct hse_kvs*)handle;
    struct hse_kvdb_opspec opspec;

    HSE_KVDB_OPSPEC_INIT(&opspec);
    opspec.kop_flags = HSE_KVDB_KOP_FLAG_PRIORITY;

    _hseKvsPutCounter.add();
    auto lt = _hseKvsPutLatency.begin();
    Status ret{::hse_kvs_put(kvs, &opspec, key.data(), key.len(), val.data(), val.len())};
    _hseKvsPutLatency.end(lt);
    return ret;
}

Status KVDBImpl::kvs_get(
    KVSHandle handle, ClientTxn* txn, const KVDBData& key, KVDBData& val, bool& found) {
    struct hse_kvs* kvs = (struct hse_kvs*)handle;
    struct hse_kvdb_opspec opspec;
    size_t flen;

    HSE_KVDB_OPSPEC_INIT(&opspec);
    opspec.kop_txn = txn ? txn->get_kvdb_txn() : 0;

    _hseKvsGetCounter.add();
    auto lt = _hseKvsGetLatency.begin();
    Status ret{::hse_kvs_get(kvs,
                             &opspec,
                             (const void*)key.data(),
                             key.len(),
                             &found,
                             val.data() + val.len(),
                             val.getAllocLen() - val.len(),
                             &flen)};
    _hseKvsGetLatency.end(lt);
    val.adjustLen(std::min(val.getAllocLen() - val.len(), flen));
    return ret;
}

Status KVDBImpl::kvs_probe_len(
    KVSHandle handle, ClientTxn* txn, const KVDBData& key, KVDBData& val, bool& found) {
    struct hse_kvs* kvs = (struct hse_kvs*)handle;
    struct hse_kvdb_opspec opspec;
    size_t flen;

    HSE_KVDB_OPSPEC_INIT(&opspec);
    opspec.kop_txn = txn ? txn->get_kvdb_txn() : 0;

    _hseKvsGetCounter.add();
    auto lt = _hseKvsGetLatency.begin();
    Status ret{::hse_kvs_get(kvs,
                             &opspec,
                             (const void*)key.data(),
                             key.len(),
                             &found,
                             val.data(),
                             val.getAllocLen(),
                             &flen)};
    _hseKvsGetLatency.end(lt);
    val.adjustLen(flen);
    return ret;
}

Status KVDBImpl::kvs_prefix_probe(KVSHandle handle,
                                  ClientTxn* txn,
                                  const KVDBData& prefix,
                                  KVDBData& key,
                                  KVDBData& val,
                                  hse_kvs_pfx_probe_cnt& found) {
    struct hse_kvs* kvs = (struct hse_kvs*)handle;
    struct hse_kvdb_opspec opspec;

    HSE_KVDB_OPSPEC_INIT(&opspec);
    opspec.kop_txn = txn ? txn->get_kvdb_txn() : 0;

    size_t klen, vlen;
    int ret = ::hse_kvs_prefix_probe_exp(kvs,
                                         &opspec,
                                         (const void*)prefix.data(),
                                         prefix.len(),
                                         &found,
                                         key.data(),
                                         key.getAllocLen(),
                                         &klen,
                                         val.data(),
                                         val.getAllocLen(),
                                         &vlen);

    if (found == HSE_KVS_PFX_FOUND_ONE) {
        invariantHse(klen <= key.getAllocLen());
        invariantHse(vlen <= val.getAllocLen() || !val.getAllocLen());
        key.adjustLen(klen);
        val.adjustLen(std::min(val.getAllocLen(), vlen));
    }

    return Status(ret);
}

Status KVDBImpl::kvs_probe_key(KVSHandle handle, ClientTxn* txn, const KVDBData& key, bool& found) {

    struct hse_kvs* kvs = (struct hse_kvs*)handle;
    struct hse_kvdb_opspec opspec;
    size_t valLen = 0;

    HSE_KVDB_OPSPEC_INIT(&opspec);
    opspec.kop_txn = txn ? txn->get_kvdb_txn() : 0;

    // treating this kvs_get as a probe wrt metrics
    _hseKvsProbeCounter.add();
    auto lt = _hseKvsProbeLatency.begin();
    int ret =
        ::hse_kvs_get(kvs, &opspec, (const void*)key.data(), key.len(), &found, 0, 0, &valLen);
    _hseKvsProbeLatency.end(lt);
    if (EMSGSIZE == ret) {
        ret = 0;
    }

    return Status(ret);
}

Status KVDBImpl::kvs_delete(KVSHandle handle, ClientTxn* txn, const KVDBData& key) {
    struct hse_kvs* kvs = (struct hse_kvs*)handle;
    struct hse_kvdb_opspec opspec;

    HSE_KVDB_OPSPEC_INIT(&opspec);
    opspec.kop_txn = txn ? txn->get_kvdb_txn() : 0;

    _hseKvsDeleteCounter.add();
    auto lt = _hseKvsDeleteLatency.begin();
    Status ret{::hse_kvs_delete(kvs, &opspec, key.data(), key.len())};
    _hseKvsDeleteLatency.end(lt);

    return ret;
}

Status KVDBImpl::kvs_prefix_delete(KVSHandle handle, ClientTxn* txn, const KVDBData& prefix) {
    struct hse_kvs* kvs = (struct hse_kvs*)handle;
    struct hse_kvdb_opspec opspec;

    HSE_KVDB_OPSPEC_INIT(&opspec);
    opspec.kop_txn = txn ? txn->get_kvdb_txn() : 0;

    _hseKvsPrefixDeleteCounter.add();
    auto lt = _hseKvsPrefixDeleteLatency.begin();
    Status ret{::hse_kvs_prefix_delete(kvs, &opspec, prefix.data(), prefix.len(), nullptr)};
    _hseKvsPrefixDeleteLatency.end(lt);

    return ret;
}

Status KVDBImpl::kvs_iter_delete(KVSHandle handle, ClientTxn* txn, const KVDBData& prefix) {
    struct hse_kvs* kvs = (struct hse_kvs*)handle;
    struct hse_kvs_cursor* lCursor = nullptr;
    struct hse_kvdb_opspec opspec;

    HSE_KVDB_OPSPEC_INIT(&opspec);
    opspec.kop_txn = txn ? txn->get_kvdb_txn() : 0;

    _hseKvsCursorCreateCounter.add();
    auto lt = _hseKvsCursorCreateLatency.begin();
    unsigned long ret =
        ::hse_kvs_cursor_create(kvs, &opspec, prefix.data(), prefix.len(), &lCursor);
    _hseKvsCursorCreateLatency.end(lt);
    if (ret) {
        return Status{ret};
    }

    bool eof = false;

    const void* lKey = nullptr;
    size_t lKeyLen = 0;
    const void* lVal = nullptr;
    size_t lValLen = 0;

    while (true) {
        _hseKvsCursorReadCounter.add();
        auto lt = _hseKvsCursorReadLatency.begin();
        ret = ::hse_kvs_cursor_read(lCursor, 0, &lKey, &lKeyLen, &lVal, &lValLen, &eof);
        _hseKvsCursorReadLatency.end(lt);
        if (ret || eof) {
            break;
        }

        ret = ::hse_kvs_delete(kvs, &opspec, lKey, lKeyLen);
        if (ret) {
            break;
        }
    }

    _hseKvsCursorDestroyCounter.add();
    lt = _hseKvsCursorDestroyLatency.begin();
    ::hse_kvs_cursor_destroy(lCursor);
    _hseKvsCursorDestroyLatency.end(lt);

    return Status{ret};
}

Status KVDBImpl::kvdb_sync() {
    unsigned long ret = 0;

    if (_handle) {
        _hseKvdbSyncCounter.add();
        auto lt = _hseKvdbSyncLatency.begin();
        ret = ::hse_kvdb_sync(_handle);
        _hseKvdbSyncLatency.end(lt);
    }

    return Status{ret};
}

Status KVDBImpl::kvdb_params_from_file(struct hse_params* params, const string& filePath) {
    return Status(::hse_params_from_file(params, filePath.c_str()));
}

Status KVDBImpl::kvdb_params_set(struct hse_params* params, const string& key, const string& val) {
    return Status(::hse_params_set(params, key.c_str(), val.c_str()));
};

// The sub_txn ops below are used in lieu of not-txnal ops where snapshot isolation is not
// required. This is so since we use only transaction enabled KVSes now.
Status KVDBImpl::kvs_sub_txn_put(KVSHandle handle, const KVDBData& key, const KVDBData& val) {
    struct hse_kvs* kvs = (struct hse_kvs*)handle;
    struct hse_kvdb_opspec opspec;
    Status ret{};

    SUB_TXN_OP_RETRY_LOOP_BEGIN {
        _hseKvsPutCounter.add();
        auto lt = _hseKvsPutLatency.begin();
        ret = Status{::hse_kvs_put(kvs, &opspec, key.data(), key.len(), val.data(), val.len())};
        _hseKvsPutLatency.end(lt);
    }
    SUB_TXN_OP_RETRY_LOOP_END(ret)

    return ret;
}

Status KVDBImpl::kvs_sub_txn_delete(KVSHandle handle, const KVDBData& key) {
    struct hse_kvs* kvs = (struct hse_kvs*)handle;
    struct hse_kvdb_opspec opspec;
    Status ret{};

    SUB_TXN_OP_RETRY_LOOP_BEGIN {
        _hseKvsDeleteCounter.add();
        auto lt = _hseKvsDeleteLatency.begin();
        ret = Status{::hse_kvs_delete(kvs, &opspec, key.data(), key.len())};
        _hseKvsDeleteLatency.end(lt);
    }
    SUB_TXN_OP_RETRY_LOOP_END(ret)

    return ret;
}


Status KVDBImpl::kvs_sub_txn_prefix_delete(KVSHandle handle, const KVDBData& prefix) {
    struct hse_kvs* kvs = (struct hse_kvs*)handle;
    struct hse_kvdb_opspec opspec;
    Status ret{};

    SUB_TXN_OP_RETRY_LOOP_BEGIN {
        _hseKvsPrefixDeleteCounter.add();
        auto lt = _hseKvsPrefixDeleteLatency.begin();
        ret = Status{::hse_kvs_prefix_delete(kvs, &opspec, prefix.data(), prefix.len(), nullptr)};
        _hseKvsPrefixDeleteLatency.end(lt);
    }
    SUB_TXN_OP_RETRY_LOOP_END(ret)


    return ret;
}
}
