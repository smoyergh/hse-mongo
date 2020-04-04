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
#include "hse_ut_common.h"

#include <boost/lexical_cast.hpp>
using namespace std;
using namespace hse;


namespace hse {

KVDBTestSuiteFixture::KVDBTestSuiteFixture() {

    _kvdbPerUt = nullptr != getenv("KVDB_PER_UT") ? true : false;

    hse::Status st = _db.kvdb_init();
    ASSERT_EQUALS(0, st.getErrno());

    hse_params_create(&_params);
    ASSERT_FALSE(nullptr == _params);

    if (_kvdbPerUt) {
        st = _db.kvdb_params_set(_params, string("kvdb.dur_capacity"), std::to_string(16));
        ASSERT_EQUALS(0, st.getErrno());
    }

    int err{0};
    while (true) {
        st = _db.kvdb_make(_mpoolName.c_str(), _kvdbName.c_str(), _params);

        err = st.getErrno();
        if (EAGAIN != err) {
            break;
        }
    }

    if (EEXIST == err) {
        err = 0;
    }

    ASSERT_EQUALS(0, err);

    st = _db.kvdb_open(_mpoolName.c_str(), _kvdbName.c_str(), _params);
    ASSERT_EQUALS(0, st.getErrno());

    _dbClosed = false;
}

void KVDBTestSuiteFixture::reset() {

    if (_dbClosed) {
        hse::Status st = _db.kvdb_init();
        ASSERT_EQUALS(0, st.getErrno());

        st = _db.kvdb_open(_mpoolName.c_str(), _kvdbName.c_str(), _params);
        ASSERT_EQUALS(0, st.getErrno());

        _dbClosed = false;
    }

    // delete all kvses
    char** kvsList = nullptr;
    unsigned int count = 0;

    hse::Status st = _db.kvdb_get_names(&count, &kvsList);
    ASSERT_EQUALS(0, st.getErrno());

    for (unsigned int i = 0; i < count; i++) {
        st = _db.kvdb_kvs_drop(kvsList[i]);
        ASSERT_EQUALS(0, st.getErrno());
    }

    _db.kvdb_free_names(kvsList);


    if (!_kvdbPerUt) {
        // do nothing
        return;
    }

    // drop the kvdb
    st = _db.kvdb_close();
    ASSERT_EQUALS(0, st.getErrno());

    st = _db.kvdb_open(_mpoolName.c_str(), _kvdbName.c_str(), _params);
    ASSERT_EQUALS(0, st.getErrno());
}

KVDBTestSuiteFixture::~KVDBTestSuiteFixture() {
    if (_dbClosed) {
        hse::Status st = _db.kvdb_init();
        ASSERT_EQUALS(0, st.getErrno());

        st = _db.kvdb_open(_mpoolName.c_str(), _kvdbName.c_str(), _params);
        ASSERT_EQUALS(0, st.getErrno());

        _dbClosed = false;
    }

    hse::Status st = _db.kvdb_close();
    ASSERT_EQUALS(0, st.getErrno());

    hse_params_destroy(_params);

    _db.kvdb_fini();
    _dbClosed = true;
}

KVDB& KVDBTestSuiteFixture::getDb() {
    return _db;
}

string KVDBTestSuiteFixture::getDbName() {
    return _kvdbName;
}

void KVDBTestSuiteFixture::closeDb() {

    if (!_dbClosed) {
        hse::Status st = _db.kvdb_close();
        ASSERT_EQUALS(0, st.getErrno());

        _db.kvdb_fini();
        _dbClosed = true;
    }
}

KVDBTestSuiteFixture& KVDBTestSuiteFixture::getFixture() {
    // Static here ensures that we have only one KVDB per suite of
    // tests. Also set in a method ensures that this is initialized after
    // other static dependencies like the txn_cache, i.e., only when this method
    // is first called.
    static KVDBTestSuiteFixture fx{};

    return fx;
}
}
