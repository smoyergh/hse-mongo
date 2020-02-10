/**
 * TODO: HSE License
 */
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#ifdef __cplusplus
extern "C" {
#endif

#include <hse_kvdb/hse.h>

#ifdef __cplusplus
}
#endif

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
// TODO: HSE
// Consider protecting handle
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

Status KVDBImpl::kvdb_open(const char* mp_name,
                           const char* kvdb_name,
                           struct hse_params* params,
                           unsigned long snapshot_id) {
    auto st = ::hse_kvdb_open(mp_name, params, snapshot_id, &_handle);
    g_txn_cache->set_kvdb(_handle);
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

Status KVDBImpl::kvdb_kvs_count(unsigned int* count) {
    return Status(::hse_kvdb_kvs_count(_handle, count));
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
    g_txn_cache->release();
    int ret = ::hse_kvdb_close(_handle);
    _handle = nullptr;
    return Status(ret);
}

Status KVDBImpl::kvs_put(KVSHandle handle,
                         ClientTxn* txn,
                         const KVDBData& key,
                         const KVDBData& val) {
    struct hse_kvs* kvs = (struct hse_kvs*)handle;
    struct hse_kvdb_opspec opspec {
        0U, txn ? txn->get_kvdb_txn() : 0
    };

    _hseKvsPutCounter.add();
    auto lt = _hseKvsPutLatency.begin();
    Status ret{::hse_kvs_put(kvs, &opspec, key.data(), key.len(), val.data(), val.len())};
    _hseKvsPutLatency.end(lt);
    return ret;
}

Status KVDBImpl::kvs_put(KVSHandle handle, const KVDBData& key, const KVDBData& val) {
    struct hse_kvs* kvs = (struct hse_kvs*)handle;
    struct hse_kvdb_opspec opspec {
        .kop_flags = HSE_KVDB_KOP_FLAG_PRIORITY,
    };

    _hseKvsPutCounter.add();
    auto lt = _hseKvsPutLatency.begin();
    Status ret{::hse_kvs_put(kvs, &opspec, key.data(), key.len(), val.data(), val.len())};
    _hseKvsPutLatency.end(lt);
    return ret;
}

Status KVDBImpl::kvs_get(
    KVSHandle handle, ClientTxn* txn, const KVDBData& key, KVDBData& val, bool& found) {
    struct hse_kvs* kvs = (struct hse_kvs*)handle;
    struct hse_kvdb_opspec opspec {
        0U, txn ? txn->get_kvdb_txn() : 0
    };
    size_t flen;

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
    struct hse_kvdb_opspec opspec {
        0U, txn ? txn->get_kvdb_txn() : 0
    };
    size_t flen;

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
    struct hse_kvdb_opspec opspec {
        0U, txn ? txn->get_kvdb_txn() : 0
    };

    size_t klen, vlen;
    int ret = ::hse_kvs_prefix_probe(kvs,
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
    struct hse_kvdb_opspec opspec {
        0U, txn ? txn->get_kvdb_txn() : 0
    };
    size_t valLen = 0;

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
    struct hse_kvdb_opspec opspec {
        0U, txn ? txn->get_kvdb_txn() : 0
    };

    _hseKvsDeleteCounter.add();
    auto lt = _hseKvsDeleteLatency.begin();
    Status ret{::hse_kvs_delete(kvs, &opspec, key.data(), key.len())};
    _hseKvsDeleteLatency.end(lt);

    return ret;
}

Status KVDBImpl::kvs_prefix_delete(KVSHandle handle, ClientTxn* txn, const KVDBData& prefix) {
    struct hse_kvs* kvs = (struct hse_kvs*)handle;
    struct hse_kvdb_opspec opspec {
        0U, txn ? txn->get_kvdb_txn() : 0
    };

    _hseKvsPrefixDeleteCounter.add();
    auto lt = _hseKvsPrefixDeleteLatency.begin();
    Status ret{::hse_kvs_prefix_delete(kvs, &opspec, prefix.data(), prefix.len(), nullptr)};
    _hseKvsPrefixDeleteLatency.end(lt);

    return ret;
}

Status KVDBImpl::kvs_iter_delete(KVSHandle handle, ClientTxn* txn, const KVDBData& prefix) {
    struct hse_kvs* kvs = (struct hse_kvs*)handle;
    struct hse_kvs_cursor* lCursor = nullptr;
    struct hse_kvdb_opspec opspec {
        0U, txn ? txn->get_kvdb_txn() : 0
    };

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

Status KVDBImpl::kvdb_cparams_parse(int argc,
                                    char** argv,
                                    struct hse_params* params,
                                    int* next_arg) {
    return Status{(unsigned long)::hse_params_parse(argc, argv, next_arg, 0, params, HSE_KVDB_CP)};
}

Status KVDBImpl::kvdb_rparams_parse(int argc,
                                    char** argv,
                                    struct hse_params* params,
                                    int* next_arg) {
    return Status{(unsigned long)::hse_params_parse(argc, argv, next_arg, 0, params, HSE_KVDB_RP)};
}

Status KVDBImpl::kvs_cparams_parse(int argc,
                                   char** argv,
                                   struct hse_params* params,
                                   int* next_arg) {
    return Status{(unsigned long)::hse_params_parse(argc, argv, next_arg, 0, params, HSE_KVS_CP)};
}

Status KVDBImpl::kvs_rparams_parse(int argc,
                                   char** argv,
                                   struct hse_params* params,
                                   int* next_arg) {
    return Status{(unsigned long)::hse_params_parse(argc, argv, next_arg, 0, params, HSE_KVS_RP)};
}

Status KVDBImpl::kvdb_get_c1_info(struct ikvdb_c1_info* info) {
    return Status(::kvdb_get_c1_info(_handle, info));
}
}
