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
