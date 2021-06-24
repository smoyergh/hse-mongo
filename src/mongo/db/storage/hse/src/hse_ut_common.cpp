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
#include "hse_ut_common.h"

#include <boost/lexical_cast.hpp>
using namespace std;
using namespace hse;


namespace hse {

KVDBTestSuiteFixture::KVDBTestSuiteFixture() {

    const char* envStr = getenv("MONGO_UT_KVDB_HOME");
    if (nullptr != envStr) {
        _kvdbHome = envStr;
    }

    hse::Status st = hse::init();
    ASSERT_EQUALS(0, st.getErrno());

    int err{0};
    vector<string> params{};
    while (true) {
        st = _db.kvdb_make(_kvdbHome.c_str(), params);

        err = st.getErrno();
        if (EAGAIN != err) {
            break;
        }
    }

    if (EEXIST == err) {
        err = 0;
    }

    ASSERT_EQUALS(0, err);

    st = _db.kvdb_open(_kvdbHome.c_str(), params);
    ASSERT_EQUALS(0, st.getErrno());

    _dbClosed = false;
}

void KVDBTestSuiteFixture::reset() {
    if (_dbClosed) {
        hse::Status st = hse::init();
        ASSERT_EQUALS(0, st.getErrno());

        vector<string> params{};
        st = _db.kvdb_open(_kvdbHome.c_str(), params);
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
}

KVDBTestSuiteFixture::~KVDBTestSuiteFixture() {
    if (_dbClosed) {
        hse::Status st = hse::init();
        ASSERT_EQUALS(0, st.getErrno());

        vector<string> params{};
        st = _db.kvdb_open(_kvdbHome.c_str(), params);
        ASSERT_EQUALS(0, st.getErrno());

        _dbClosed = false;
    }

    hse::Status st = _db.kvdb_close();
    ASSERT_EQUALS(0, st.getErrno());

    hse::fini();
    _dbClosed = true;
}

KVDB& KVDBTestSuiteFixture::getDb() {
    return _db;
}

string KVDBTestSuiteFixture::getDbHome() {
    return _kvdbHome;
}

void KVDBTestSuiteFixture::closeDb() {

    if (!_dbClosed) {
        hse::Status st = _db.kvdb_close();
        ASSERT_EQUALS(0, st.getErrno());

        hse::fini();
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
}  // namespace hse
