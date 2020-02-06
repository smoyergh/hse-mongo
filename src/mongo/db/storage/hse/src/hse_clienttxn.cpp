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

using namespace std;
using mongo::warning;

// KVDB interface
namespace hse {

TxnCache::~TxnCache() {
    if (_kvdb)
        release();
}

void TxnCache::set_kvdb(hse_kvdb* kvdb) {
    _kvdb = kvdb;
}

void TxnCache::release() {
    _kvdb = NULL;
}

hse_kvdb_txn* TxnCache::alloc() {
    return ::hse_kvdb_txn_alloc(_kvdb);
}

void TxnCache::free(hse_kvdb_txn* txn) {
    ::hse_kvdb_txn_free(_kvdb, txn);
}

TxnCache _txn_cache;

TxnCache* g_txn_cache = &_txn_cache;

ClientTxn::ClientTxn(struct hse_kvdb* kvdb) : _kvdb(kvdb), _txn(0) {
    _txn = g_txn_cache->alloc();
}

// virtual
ClientTxn::~ClientTxn() {
    if (_txn)
        g_txn_cache->free(_txn);
}

Status ClientTxn::begin() {
    return Status(::hse_kvdb_txn_begin(_kvdb, _txn));
}

Status ClientTxn::commit() {
    return Status(::hse_kvdb_txn_commit(_kvdb, _txn));
}

Status ClientTxn::abort() {
    return Status(::hse_kvdb_txn_abort(_kvdb, _txn));
}
}
