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


#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>

#ifdef __cplusplus
extern "C" {
#endif

#include <hse/hse.h>

#ifdef __cplusplus
}
#endif

#include "hse_exceptions.h"

using namespace std;

// KVDB interface
namespace hse {

typedef void* KVSHandle;

class ClientTxn;

class Status {
public:
    Status(unsigned long e = 0) : _err(e) {}

    bool ok() const {
        return (0 == _err);
    }

    int getErrno() const {
        return hse_err_to_errno(_err);
    }

    string ToString() const {
        stringstream ss{};
        char buf[256];
        ss << "KVDB Error: " << hse_err_to_string(_err, buf, 256) << " - #" << this->getErrno()
           << endl;
        return ss.str();
    }

private:
    unsigned long _err;
};

class KVDBData {
public:
    KVDBData() {}

    KVDBData(uint8_t* cStr) : _data{cStr} {
        _len = strlen((char*)cStr) + 1;
    }

    KVDBData(const uint8_t* cStr) : KVDBData{(char*)cStr} {}

    KVDBData(uint8_t* str, unsigned long l) : _data{str}, _bufLen{l}, _len{l} {}

    KVDBData(const uint8_t* str, unsigned long l) : KVDBData{(uint8_t*)str, l} {}

    KVDBData(const string& s) : _data{(uint8_t*)s.c_str()}, _bufLen{s.size()}, _len(s.size()) {}

    KVDBData(uint8_t* mem, unsigned long len, bool owned) {
        if (owned) {
            createOwned(len);
            memcpy(_ownedData.get(), mem, len);
            _len = len;
        } else {
            _data = mem;
            _bufLen = len;
            _len = len;
        }
    }

    KVDBData(const uint8_t* mem, unsigned long len, bool owned)
        : KVDBData{(uint8_t*)mem, len, owned} {}

    KVDBData(const KVDBData& n) {
        this->_data = n._data;
        this->_bufLen = n._bufLen;
        this->_len = n._len;
        this->_owned = n._owned;
        this->_ownedData = n._ownedData;
        this->_allocLen = n._allocLen;
        this->_num_chunks = n._num_chunks;
        this->_total_len = n._total_len;
        this->_total_len_comp = n._total_len_comp;
        this->_offset = n._offset;
    }

    KVDBData& operator=(const KVDBData& rhs) {
        this->_data = rhs._data;
        this->_bufLen = rhs._bufLen;
        this->_len = rhs._len;
        this->_owned = rhs._owned;
        this->_ownedData = rhs._ownedData;
        this->_allocLen = rhs._allocLen;
        this->_num_chunks = rhs._num_chunks;
        this->_total_len = rhs._total_len;
        this->_total_len_comp = rhs._total_len_comp;
        this->_offset = rhs._offset;

        return *this;
    }

    uint8_t* data() const {
        if (_owned) {
            return _ownedData.get();
        } else {
            return _data;
        }
    }

    unsigned long len() const {
        return _len;
    }

    void adjustLen(unsigned long copied) {
        _len = _len + copied;
    }

    void setFraming(unsigned long totallen,
                    unsigned long totallencomp,
                    unsigned int num_chunks,
                    unsigned int offset) {
        _total_len = totallen;
        _total_len_comp = totallencomp;
        _num_chunks = num_chunks;
        _offset = offset;
    }
    unsigned long getTotalLen() const {
        return _total_len;
    }
    unsigned long getTotalLenComp() const {
        return _total_len_comp;
    }
    unsigned int getNumChunks() const {
        return _num_chunks;
    }
    unsigned int getOffset() const {
        return _offset;
    }

    bool empty() const {
        return (0 == _len);
    }

    KVDBData makeOwned() {
        if (!_owned) {
            uint8_t* nData = new uint8_t[_bufLen];
            memcpy(nData, _data, _len);
            _allocLen = _bufLen;
            _ownedData.reset(nData, [](uint8_t* p) { delete[] p; });
            _owned = true;
        }
        return *this;
    }

    KVDBData createOwned(unsigned long len) {
        uint8_t* nData = new uint8_t[len];
        _allocLen = len;
        _ownedData.reset(nData, [](uint8_t* p) { delete[] p; });
        _owned = true;
        _len = 0;

        return *this;
    }

    // set an external un-owned buffer for reading into.
    void setReadBuf(uint8_t* buf, unsigned long len) {
        _data = buf;
        _bufLen = len;
        _len = 0;
        _owned = false;
        _allocLen = len;
    }

    unsigned long getAllocLen() {
        return _allocLen;
    }

    KVDBData clone() const {
        return KVDBData(*this).makeOwned();
    }

    void destroy() {
        // memory is managed by shared_ptr
        _data = nullptr;
        _ownedData.reset();
        _len = 0;
        _bufLen = 0;
        _owned = false;
        _allocLen = 0;
        _num_chunks = 0;
        _total_len = 0;
        _total_len_comp = 0;
        _offset = 0;
    }

    uint8_t* getDataCopy() {
        uint8_t* ret = new uint8_t[_len];
        if (_owned) {
            memcpy(ret, _ownedData.get(), _len);
        } else {
            memcpy(ret, _data, _len);
        }

        return ret;
    }

    Status copy(const uint8_t* str, unsigned long len) {
        if (_owned && len <= (_allocLen - _len)) {
            memcpy(_ownedData.get() + _len, str, len);
            adjustLen(len);

            return Status{};
        } else if (!_owned && len <= (_bufLen - _len)) {  // not Owned
            memcpy(_data + _len, str, len);
            adjustLen(len);

            return Status{};
        }

        return Status{EMSGSIZE};
    }

    virtual ~KVDBData(){};

private:
    uint8_t* _data{nullptr};
    unsigned long _bufLen{0};

    unsigned long _len{0};

    bool _owned{false};
    shared_ptr<uint8_t> _ownedData{};
    unsigned long _allocLen{0};

    // The below 3 fields are updated only when this object corresponds
    // to the first chunk of a user value.
    // They are update after the value is read from KVS.
    unsigned int _num_chunks{0};  // (Actual number of chunks) - 1.
                                  // If the value is not chunked, it is 0.
    unsigned int _offset{0};      // first byte of user data in the buffer.
    unsigned long _total_len{0};  // Length of the uncompressed user data
                                  // across all chunks. Does not include the
                                  // length header that may be before "_offset".
    unsigned long _total_len_comp{0};
    // Length of the compressed user data across
    // all chunks. Does not include the length
    // header but include the compression headers.
    // If the collection is not compressed, it is
    // equal to _total_len.
};

// Lexicographic
static bool operator<(const KVDBData& lhs, const KVDBData& rhs) {
    unsigned long lLen = lhs.len();
    unsigned long rLen = rhs.len();

    unsigned long minLen = min(lLen, rLen);

    int res = memcmp(lhs.data(), rhs.data(), minLen);
    if (0 == res) {
        if (lLen < rLen) {
            return true;
        } else {
            return false;
        }
    } else {
        return (0 > res);
    }
}

static inline bool operator==(const KVDBData& lhs, const KVDBData& rhs) {
    return lhs.len() == rhs.len() && memcmp(lhs.data(), rhs.data(), rhs.len()) == 0;
}

// KVDB Interface
class KVDB {
public:
    KVDB() {}

    virtual Status kvdb_init() = 0;

    virtual Status kvdb_fini() = 0;

    virtual Status kvdb_make(const char* mp_name,
                             const char* kvdb_name,
                             struct hse_params* params) = 0;

    virtual Status kvdb_open(const char* mp_name,
                             const char* kvdb_name,
                             struct hse_params* params,
                             unsigned long snapshot_id) = 0;

    virtual Status kvdb_kvs_open(const char* kvs_name,
                                 struct hse_params* params,
                                 KVSHandle& kvs_out) = 0;

    virtual Status kvdb_kvs_close(KVSHandle handle) = 0;

    virtual struct hse_kvdb* kvdb_handle() = 0;

    virtual Status kvdb_kvs_count(unsigned int* count) = 0;

    virtual Status kvdb_get_names(unsigned int* count, char*** kvs_list) = 0;

    virtual Status kvdb_free_names(char** kvsv) = 0;

    virtual Status kvdb_kvs_make(const char* kvs_name, struct hse_params* params) = 0;

    virtual Status kvdb_kvs_drop(const char* kvs_name) = 0;

    virtual Status kvdb_close() = 0;

    virtual Status kvs_put(KVSHandle handle,
                           ClientTxn* txn,
                           const KVDBData& key,
                           const KVDBData& val) = 0;

    virtual Status kvs_put(KVSHandle handle, const KVDBData& key, const KVDBData& val) = 0;

    virtual Status kvs_prefix_probe(KVSHandle handle,
                                    ClientTxn* txn,
                                    const KVDBData& prefix,
                                    KVDBData& key,
                                    KVDBData& val,
                                    hse_kvs_pfx_probe_cnt& found) = 0;

    virtual Status kvs_probe_len(
        KVSHandle handle, ClientTxn* txn, const KVDBData& key, KVDBData& val, bool& found) = 0;

    virtual Status kvs_get(
        KVSHandle handle, ClientTxn* txn, const KVDBData& key, KVDBData& val, bool& found) = 0;

    virtual Status kvs_probe_key(KVSHandle handle,
                                 ClientTxn* txn,
                                 const KVDBData& key,
                                 bool& found) = 0;

    virtual Status kvs_delete(KVSHandle handle, ClientTxn* txn, const KVDBData& key) = 0;

    virtual Status kvs_prefix_delete(KVSHandle handle, ClientTxn* txn, const KVDBData& prefix) = 0;

    virtual Status kvs_iter_delete(KVSHandle handle, ClientTxn* txn, const KVDBData& prefix) = 0;

    virtual Status kvdb_sync() = 0;

    virtual Status kvdb_params_from_file(struct hse_params* params, const string& filePath) = 0;

    virtual Status kvdb_params_set(struct hse_params* params,
                                   const string& key,
                                   const string& val) = 0;

    bool keyStartsWith(KVDBData key, const uint8_t* prefix, unsigned long pLen) {
        if (pLen <= key.len() && 0 == memcmp(key.data(), prefix, pLen)) {
            return true;
        }

        return false;
    }

    virtual ~KVDB() {}
};
}
