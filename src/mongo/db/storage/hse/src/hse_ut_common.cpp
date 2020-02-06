/**
 * TODO: LICENSE
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

    hse_params_create(&_kvdbCfg);
    if (_kvdbPerUt) {
        hse_params_set(_kvdbCfg, "kvdb.cparams.dur_capacity", std::to_string(16).c_str());
    }

    while (true) {
        st = _db.kvdb_make(_mpoolName.c_str(), _kvdbName.c_str(), _kvdbCfg);

        int err = st.getErrno();
        if (EEXIST != err && EAGAIN != err) {
            break;
        }
    }

    ASSERT_EQUALS(0, st.getErrno());

    hse_params_create(&_kvdbRnCfg);
    st = _db.kvdb_open(_mpoolName.c_str(), _kvdbName.c_str(), _kvdbRnCfg, _snapId);
    ASSERT_EQUALS(0, st.getErrno());

    _dbClosed = false;
}

void KVDBTestSuiteFixture::reset() {

    if (_dbClosed) {
        hse::Status st = _db.kvdb_init();
        ASSERT_EQUALS(0, st.getErrno());

        st = _db.kvdb_open(_mpoolName.c_str(), _kvdbName.c_str(), _kvdbRnCfg, _snapId);
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

    st = _db.kvdb_open(_mpoolName.c_str(), _kvdbName.c_str(), _kvdbRnCfg, _snapId);
    ASSERT_EQUALS(0, st.getErrno());
}

KVDBTestSuiteFixture::~KVDBTestSuiteFixture() {
    if (_dbClosed) {
        hse::Status st = _db.kvdb_init();
        ASSERT_EQUALS(0, st.getErrno());

        st = _db.kvdb_open(_mpoolName.c_str(), _kvdbName.c_str(), _kvdbRnCfg, _snapId);
        ASSERT_EQUALS(0, st.getErrno());

        _dbClosed = false;
    }

    hse::Status st = _db.kvdb_close();
    ASSERT_EQUALS(0, st.getErrno());

    hse_params_destroy(_kvdbCfg);
    hse_params_destroy(_kvdbRnCfg);

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
