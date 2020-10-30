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

#include "hse_impl.h"
#include "hse_kvscursor.h"
#include "hse_stats.h"
#include "hse_util.h"

#include <chrono>
#include <cstdlib>
#include <thread>

using namespace std;
using mongo::warning;
using hse::Status;

using hse_stat::_hseKvsCursorCreateLatency;
using hse_stat::_hseKvsCursorCreateCounter;
using hse_stat::_hseKvsCursorReadLatency;
using hse_stat::_hseKvsCursorReadCounter;
using hse_stat::_hseKvsCursorUpdateLatency;
using hse_stat::_hseKvsCursorUpdateCounter;
using hse_stat::_hseKvsCursorDestroyLatency;
using hse_stat::_hseKvsCursorDestroyCounter;

namespace {
int RETRY_FIB_SEQ_EAGAIN[] = {1, 2, 3, 5, 8, 13};
int FIB_LEN = 6;
}

// KVDB interface
namespace hse {

KvsCursor* create_cursor(KVSHandle kvs,
                         KVDBData& prefix,
                         bool forward,
                         const struct CompParms& compparms,
                         ClientTxn* lnkd_txn) {
    return new KvsCursor(kvs, prefix, forward, lnkd_txn, compparms);
}

void KvsCursor::_kvs_cursor_create(ClientTxn* lnkd_txn) {
    struct hse_kvdb_opspec opspec;
    int retries = 0;
    unsigned long long sleepTime = 0;

    HSE_KVDB_OPSPEC_INIT(&opspec);

    if (lnkd_txn) {
        // the client is requesting a bound cursor
        opspec.kop_flags |= HSE_KVDB_KOP_FLAG_BIND_TXN;
        opspec.kop_flags |= HSE_KVDB_KOP_FLAG_STATIC_VIEW;
        opspec.kop_txn = lnkd_txn->get_kvdb_txn();
    }

    if (!_forward) {
        opspec.kop_flags |= HSE_KVDB_KOP_FLAG_REVERSE;
    }

    /* [HSE_REVISIT] This loop retries indefinitely on an EAGAIN. */
    while (true) {
        if (retries < FIB_LEN) {
            sleepTime = RETRY_FIB_SEQ_EAGAIN[retries % FIB_LEN];
        } else {
            sleepTime = RETRY_FIB_SEQ_EAGAIN[FIB_LEN - 1];
            if (retries % 20 == 0)
                warning() << "HSE: kvs_cursor_create returning EAGAIN after " << retries
                          << " retries";
        }

        _hseKvsCursorCreateCounter.add();
        auto lt = _hseKvsCursorCreateLatency.begin();
        Status st = Status{
            ::hse_kvs_cursor_create(_kvs, &opspec, (const void*)_pfx.data(), _pfx.len(), &_cursor)};
        _hseKvsCursorCreateLatency.end(lt);
        if (st.ok())
            break;

        if (st.getErrno() != EAGAIN)
            throw KVDBException("non EAGAIN failure from hse_kvs_cursor_create()");

        this_thread::sleep_for(chrono::milliseconds(sleepTime));

        retries++;
    }
}

KvsCursor::KvsCursor(KVSHandle handle,
                     KVDBData& prefix,
                     bool forward,
                     ClientTxn* lnkd_txn,
                     const struct CompParms& compparms)
    : _kvs((struct hse_kvs*)handle),
      _pfx(prefix),
      _forward(forward),
      _cursor(0),
      _start(0),
      _end(0),
      _curr(0),
      _kvs_key(0),
      _kvs_klen(0),
      _kvs_seek_key(0),
      _kvs_seek_klen(0),
      _kvs_val(0),
      _kvs_vlen(0),
      _kvs_num_chunks(0),
      _offset(0),
      _total_len(0),
      _kvs_total_len_comp(0),
      _compparms(compparms) {
    _kvs_cursor_create(lnkd_txn);
}

KvsCursor::~KvsCursor() {
    _hseKvsCursorDestroyCounter.add();
    auto lt = _hseKvsCursorDestroyLatency.begin();
    ::hse_kvs_cursor_destroy(_cursor);
    _hseKvsCursorDestroyLatency.end(lt);
}

Status KvsCursor::update(ClientTxn* lnkd_txn) {
    struct hse_kvdb_opspec opspec;

    HSE_KVDB_OPSPEC_INIT(&opspec);
    if (!_forward) {
        opspec.kop_flags |= HSE_KVDB_KOP_FLAG_REVERSE;
    }

    if (lnkd_txn) {
        opspec.kop_flags |= HSE_KVDB_KOP_FLAG_BIND_TXN;
        opspec.kop_flags |= HSE_KVDB_KOP_FLAG_STATIC_VIEW;
        opspec.kop_txn = lnkd_txn->get_kvdb_txn();
    }

    _hseKvsCursorUpdateCounter.add();
    auto lt = _hseKvsCursorUpdateLatency.begin();
    Status st = Status{::hse_kvs_cursor_update(_cursor, &opspec)};
    _hseKvsCursorUpdateLatency.end(lt);
    if (st.ok())
        return st;

    // Recreating cursor and seeking to last point. Copy out key before destroying the cursor.
    // Skip a key after seek if the last op was a read.
    bool lastOpWasRead = !_kvs_seek_key && _kvs_key;
    const void* skey = _kvs_seek_key ?: _kvs_key;
    size_t sklen = _kvs_seek_klen ?: _kvs_klen;
    auto seekKey = KVDBData((const uint8_t*)skey, (int)sklen, true);

    _hseKvsCursorDestroyCounter.add();
    lt = _hseKvsCursorDestroyLatency.begin();
    ::hse_kvs_cursor_destroy(_cursor);
    _hseKvsCursorDestroyLatency.end(lt);

    _kvs_cursor_create(lnkd_txn);
    st = Status{::hse_kvs_cursor_seek(
        _cursor, 0, seekKey.data(), seekKey.len(), &_kvs_seek_key, &_kvs_seek_klen)};
    if (st.ok() && lastOpWasRead) {
        // Last op was a read, if seek didn't land on the key we had read, it was deleted. Don't
        // skip.
        if (seekKey.len() == _kvs_seek_klen &&
            0 == memcmp(seekKey.data(), _kvs_seek_key, _kvs_seek_klen)) {
            bool eof;

            _read_kvs(eof);
        }
    }

    return st;
}

Status KvsCursor::seek(const KVDBData& key, const KVDBData* kmax, KVDBData* pos) {
    Status st{};

    st = Status{
        ::hse_kvs_cursor_seek(_cursor, 0, key.data(), key.len(), &_kvs_seek_key, &_kvs_seek_klen)};
    if (st.ok()) {
        if (pos)
            *pos = KVDBData((const uint8_t*)_kvs_seek_key, (int)_kvs_seek_klen);
    }

    return st;
}

Status KvsCursor::read(KVDBData& key, KVDBData& val, bool& eof) {
    // We have guaranteed that the only possible error value returned is ECANCELED, which
    // we will return eagerly even if the "next" value might be from the connector itself.
    int ret = _read_kvs(eof);
    if (ret)
        return ret;

    if (!eof) {
        key = KVDBData((const uint8_t*)_kvs_key, (int)_kvs_klen);
        val = KVDBData((const uint8_t*)_kvs_val, (int)_kvs_vlen);
        val.setFraming(_total_len, _kvs_total_len_comp, _kvs_num_chunks, _offset);
    }

    return 0;
}

int KvsCursor::_read_kvs(bool& eof) {
    Status st{};
    bool _eof;

    _kvs_seek_key = 0;
    _kvs_seek_klen = 0;

    _hseKvsCursorReadCounter.add();
    auto lt = _hseKvsCursorReadLatency.begin();
    st = Status{
        ::hse_kvs_cursor_read(_cursor, 0, &_kvs_key, &_kvs_klen, &_kvs_val, &_kvs_vlen, &_eof)};
    _hseKvsCursorReadLatency.end(lt);

    eof = _eof;
    if (st.ok() && !_eof) {
        unsigned int off_comp;

        computeFraming((const uint8_t*)_kvs_val,
                       _kvs_vlen,
                       this->_compparms,
                       &_total_len,
                       &_kvs_total_len_comp,
                       &_kvs_num_chunks,
                       &_offset,
                       &off_comp);

        // If the value is across several chunks, it is not possible to
        // decompress the first chunk only. Because the compression of the
        // whole value is done before the chunking.
        if (this->_compparms.compdoit && !_kvs_num_chunks) {
            void* kvs_val;
            size_t kvs_len;

            st = decompressdata1(
                this->_compparms, _kvs_val, _kvs_vlen, off_comp, &kvs_val, &kvs_len);
            if (st.ok()) {
                _kvs_val = kvs_val;
                _kvs_vlen = kvs_len;
                // Free the old _kvs_val buffer and attach the new one to _uncompressed_val.
                _uncompressed_val.reset(static_cast<unsigned char*>(kvs_val));
            } else {
                return EILSEQ;
            }
        }
    }

    return st.getErrno();
}

Status KvsCursor::save() {
    return 0;
}

Status KvsCursor::restore() {
    return 0;
}
}
