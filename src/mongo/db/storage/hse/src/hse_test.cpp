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
#include "mongo/platform/basic.h"

#include "hse_impl.h"
#include "hse_kvscursor.h"
#include "hse_ut_common.h"

#include <iostream>
#include <sstream>

#include "mongo/unittest/unittest.h"

using namespace std;
using namespace hse;

namespace {

int MAX_KEY_SIZE = HSE_KVS_KLEN_MAX;
int MAX_VAL_SIZE = 2048;

int MAX_DB_SIZE = 256;

const unsigned int TEST_KVS_CNT = 2;

const unsigned int GETCO_RETRY_CNT = 2;

const size_t DEF_VAL_SIZE = 1024 * 1024;

typedef map<KVDBData, KVDBData> KVS;

size_t seqlen(const uint8_t* seq) {
    return strlen(reinterpret_cast<const char*>(seq));
}


bool genPrefixInfo(map<KVDBData, int>& pInfo) {

    vector<const uint8_t*> prefixes = {
        (const uint8_t*)"1rq", (const uint8_t*)"2xy", (const uint8_t*)"3ab"};
    vector<int> pLens = {4, 4, 4};

    for (unsigned int i = 0; i < prefixes.size(); i++) {
        KVDBData prefix{prefixes[i], seqlen(prefixes[i]) + 1};
        int pLen = pLens[i];

        pInfo[prefix] = pLen;
    }

    return true;
}

bool createRandStr(const char* prefix, int prefix_len, uint8_t** outStr, int len, long tag) {

    uint8_t* rStr = (uint8_t*)malloc(len + sizeof(long));
    if (!rStr)
        return false;

    *outStr = rStr;

    int numPack = 0;
    if (prefix != nullptr) {
        memcpy(rStr, prefix, prefix_len);
        numPack = (len - prefix_len) / sizeof(int);
        rStr += prefix_len;
    } else {
        numPack = len / sizeof(int);
    }

    for (int i = 0; i < numPack; i++) {
        int genInt = std::rand();
        memcpy(rStr + i * sizeof(int), &(genInt), sizeof(int));
    }

    rStr += numPack * sizeof(int);

    // add tag
    memcpy(rStr, &(tag), sizeof(tag));

    return true;
}

bool genRandKeyVal(map<KVDBData, int>& prefixInfo,
                   map<KVDBData, KVS>& prefixedVals,
                   map<KVDBData, KVDBData>& outKeyVal,
                   int num) {
    int numLeft = num;
    long tag = 0;

    for (auto& pItem : prefixInfo) {
        const char* prefStr = (const char*)pItem.first.data();
        int pLen = pItem.first.len();
        int pCnt = pItem.second;

        prefixedVals[pItem.first] = KVS{};

        for (int i = 0; i < pCnt; i++) {
            int keySize = rand() % MAX_KEY_SIZE;

            int valSize = rand() % MAX_VAL_SIZE;

            uint8_t* keyPtr = NULL;
            uint8_t* valPtr = NULL;

            keySize = std::max(keySize, pLen + 4);

            if (!createRandStr(prefStr, pLen, &keyPtr, keySize, tag)) {
                return false;
            }

            if (!createRandStr(nullptr, 0, &valPtr, valSize, 0)) {
                free(keyPtr);
                return false;
            }

            outKeyVal[KVDBData(keyPtr, keySize)] = KVDBData(valPtr, valSize);
            prefixedVals[pItem.first][KVDBData(keyPtr, keySize)] = KVDBData(valPtr, valSize);
            numLeft--;
            tag++;
        }
    }

    for (int i = 0; i < numLeft; i++) {
        int keySize = rand() % MAX_KEY_SIZE;
        int valSize = rand() % MAX_VAL_SIZE;

        keySize = std::max(keySize, 1);

        uint8_t* keyPtr = NULL;
        uint8_t* valPtr = NULL;

        if (!createRandStr(nullptr, 0, &keyPtr, keySize, tag)) {
            return false;
        }

        if (!createRandStr(nullptr, 0, &valPtr, valSize, 0)) {
            free(keyPtr);
            return false;
        }

        outKeyVal[KVDBData(keyPtr, keySize)] = KVDBData(valPtr, valSize);
        tag++;
    }

    return true;
}

bool genFixedKeyVal(map<KVDBData, int>& prefixInfo,
                    map<KVDBData, KVS>& prefixedVals,
                    map<KVDBData, KVDBData>& outKeyVal,
                    int num) {
    vector<const uint8_t*> keyV = {(const uint8_t*)"1", (const uint8_t*)"2", (const uint8_t*)"3"};
    vector<const uint8_t*> valV = {
        (const uint8_t*)"1val", (const uint8_t*)"2val", (const uint8_t*)"3val"};
    for (unsigned int i = 0; i < keyV.size(); i++) {
        const uint8_t* keyPtr = keyV[i];
        const uint8_t* valPtr = valV[i];
        int keySize = seqlen(keyPtr) + 1;
        int valSize = seqlen(valPtr) + 1;

        outKeyVal[KVDBData(keyPtr, keySize)] = KVDBData(valPtr, valSize);
    }
    return true;
}

bool freeKeyVals(map<KVDBData, KVDBData>& keyVals) {

    for (auto& item : keyVals) {
        free((void*)item.first.data());
        free((void*)item.second.data());
    }

    return true;
}

Status getcoUtil(KVDB& kvdb,
                 KVSHandle& kvsHandle,
                 ClientTxn* txn,
                 const KVDBData& key,
                 KVDBData& val,
                 bool& found) {

    found = false;
    val.createOwned(HSE_KVS_VLEN_MAX);
    return kvdb.kvs_get(kvsHandle, txn, key, val, found);
}
}

namespace mongo {

class KVDBREGTEST : public unittest::Test {
protected:
    void setUp() {
        unsigned kvsCnt = 0;
        hse::Status st = _db.kvdb_kvs_count(&kvsCnt);
        ASSERT_EQUALS(0, st.getErrno());

        ASSERT_EQ((unsigned int)0, kvsCnt);

        //      ASSERT_EQ(0, _db._testGetMaxIdx());

        // Create all the kvses
        for (unsigned int i = 0; i < TEST_KVS_CNT; i++) {
            hse_params_create(&_kvsCParams[i]);
            hse_params_create(&_kvsRParams[i]);

            hse_params_set(_kvsCParams[i], "kvs.cparams.pfxlen", std::to_string(4).c_str());
            st = _db.kvdb_kvs_make(_kvsNames[i], _kvsCParams[i]);
            ASSERT_EQUALS(0, st.getErrno());
            st = _db.kvdb_kvs_open(_kvsNames[i], _kvsRParams[i], _kvsHandles[i]);
            ASSERT_EQUALS(0, st.getErrno());

            hse_params_destroy(_kvsCParams[i]);
            hse_params_destroy(_kvsRParams[i]);
        }


        st = _db.kvdb_kvs_count(&kvsCnt);
        ASSERT_EQUALS(0, st.getErrno());

        ASSERT_EQ(TEST_KVS_CNT, kvsCnt);
    }

    void tearDown() {
        hse::Status st{};

        for (unsigned int i = 0; i < TEST_KVS_CNT; i++) {
            st = _db.kvdb_kvs_close(_kvsHandles[i]);
            ASSERT_EQUALS(0, st.getErrno());
        }

        // drops all kvses + drops kvdb if requested via KVDB_PER_UT env
        _dbFixture.reset();
    }

protected:
    hse::KVDBTestSuiteFixture& _dbFixture = KVDBTestSuiteFixture::getFixture();
    hse::KVDB& _db = _dbFixture.getDb();

    const char* _kvsNames[2] = {"KVS1", "KVS2"};
    struct hse_params* _kvsCParams1;
    struct hse_params* _kvsCParams2;
    struct hse_params* _kvsCParams[2] = {_kvsCParams1, _kvsCParams1};

    struct hse_params* _kvsRParams1;
    struct hse_params* _kvsRParams[2] = {_kvsRParams1, _kvsRParams1};

    KVSHandle _kvsHandles[TEST_KVS_CNT];
};

TEST(KVDBREGTEST, DummyTest) {
    cout << "DUMMY TEST" << endl;
}

TEST_F(KVDBREGTEST, KvdbPutGetDelTest) {
    int dbSize = std::rand() % MAX_DB_SIZE;

    map<KVDBData, int> pInfo;

    map<KVDBData, KVDBData> keyVals;

    map<KVDBData, KVS> pVals;

    ASSERT(genRandKeyVal(pInfo, pVals, keyVals, dbSize));

    hse::Status st{};

    for (auto& item : keyVals) {

        st = _db.kvs_put(_kvsHandles[0], 0, item.first, item.second);

        ASSERT_EQUALS(0, st.getErrno());
    }

    // get
    for (auto& item : keyVals) {

        const KVDBData& key = item.first;
        KVDBData val{};
        bool found = false;

        st = getcoUtil(_db, _kvsHandles[0], 0, key, val, found);

        ASSERT_EQUALS(0, st.getErrno());

        ASSERT(found);

        ASSERT(val == item.second);

        val.destroy();
    }

    // Delete every odd item
    int cnt = 0;
    for (auto& item : keyVals) {

        const KVDBData& key = item.first;

        if (cnt % 2) {
            st = _db.kvs_delete(_kvsHandles[0], 0, key);
        }
        ASSERT_EQUALS(0, st.getErrno());


        cnt++;
    }

    // test delete by get
    cnt = 0;
    for (auto& item : keyVals) {

        const KVDBData& key = item.first;
        KVDBData val{};
        bool found = true;

        st = getcoUtil(_db, _kvsHandles[0], 0, key, val, found);

        ASSERT_EQUALS(0, st.getErrno());


        if (cnt % 2) {
            ASSERT_FALSE(found);
        } else {
            ASSERT(val == item.second);

            val.destroy();
        }

        cnt++;
    }

    ASSERT(freeKeyVals(keyVals));
}

TEST_F(KVDBREGTEST, KvdbForwardFullScanTest) {
    // Populate the DB
    int dbSize = std::rand() % MAX_DB_SIZE;

    map<KVDBData, int> pInfo;

    KVS keyVals;

    map<KVDBData, KVS> pVals;

    ASSERT(genRandKeyVal(pInfo, pVals, keyVals, dbSize));

    hse::Status st{};

    for (map<KVDBData, KVDBData>::iterator iter = keyVals.begin(); iter != keyVals.end(); iter++) {

        st = _db.kvs_put(_kvsHandles[0], 0, iter->first, iter->second);
        ASSERT_EQUALS(0, st.getErrno());
    }

    // Begin a scan
    KVDBData prefix{};
    const struct CompParms compparms {};
    hse::KvsCursor* cursor = create_cursor(_kvsHandles[0], prefix, true, compparms);
    ASSERT_FALSE(cursor == 0);

    // get
    // int i = 0;
    bool eof = false;

    for (map<KVDBData, KVDBData>::iterator iter = keyVals.begin(); iter != keyVals.end(); iter++) {

        KVDBData cKey{};
        KVDBData cVal{};

        cursor->read(cKey, cVal, eof);

        // cout << i++ << endl;

        ASSERT(!eof);
        ASSERT_EQUALS(0, st.getErrno());
        ASSERT_TRUE(cVal == iter->second);
        ASSERT_TRUE(cKey == iter->first);
    }

    KVDBData cKey{};
    KVDBData cVal{};

    st = cursor->read(cKey, cVal, eof);
    ASSERT(eof);

    st = cursor->read(cKey, cVal, eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT(eof);

    delete cursor;

    ASSERT(freeKeyVals(keyVals));
}


TEST_F(KVDBREGTEST, KvdbPrefixForwardScanTest) {

    // Populate the DB
    int dbSize = std::rand() % MAX_DB_SIZE;

    map<KVDBData, int> pInfo;

    ASSERT(genPrefixInfo(pInfo));

    KVS keyVals;

    map<KVDBData, KVS> pVals;

    ASSERT(genRandKeyVal(pInfo, pVals, keyVals, dbSize));

    hse::Status st{};

    for (map<KVDBData, KVDBData>::iterator iter = keyVals.begin(); iter != keyVals.end(); iter++) {

        st = _db.kvs_put(_kvsHandles[0], 0, iter->first, iter->second);
        ASSERT_EQUALS(0, st.getErrno());
    }

    hse::KvsCursor* cursor;
    // Begin a scan
    bool eof = false;

    for (auto& item : pInfo) {
        KVDBData prefix = item.first;
        int numPrefixes = item.second;

        const struct CompParms compparms {};
        cursor = create_cursor(_kvsHandles[0], prefix, true, compparms);
        ASSERT_FALSE(cursor == 0);

        int fCnt = 0;
        // get
        KVS& prefixVals = pVals[item.first];

        for (KVS::iterator iter = prefixVals.begin(); iter != prefixVals.end(); iter++) {
            KVDBData cKey{};
            KVDBData cVal{};

            st = cursor->read(cKey, cVal, eof);
            ASSERT(!eof);
            ASSERT_EQUALS(0, st.getErrno());

            ASSERT_TRUE(cVal == iter->second);
            ASSERT_TRUE(cKey == iter->first);

            fCnt++;
        }

        ASSERT_EQUALS(numPrefixes, fCnt);

        KVDBData cKey{};
        KVDBData cVal{};

        st = cursor->read(cKey, cVal, eof);
        ASSERT_EQUALS(0, st.getErrno());
        ASSERT(eof);

        st = cursor->read(cKey, cVal, eof);
        ASSERT_EQUALS(0, st.getErrno());
        ASSERT(eof);

        delete cursor;
    }

    ASSERT(freeKeyVals(keyVals));
}


TEST_F(KVDBREGTEST, KvdbNormalCursorSeekTest) {

    // Populate the DB

    KVS keyVals{};

    hse::Status st{};

    vector<const uint8_t*> keyStrs{(const uint8_t*)"0",
                                   (const uint8_t*)"1",
                                   (const uint8_t*)"2",
                                   (const uint8_t*)"3",
                                   (const uint8_t*)"4",
                                   (const uint8_t*)"5",
                                   (const uint8_t*)"6",
                                   (const uint8_t*)"7"};
    const uint8_t* commonVal = (const uint8_t*)"COMMON_VALUE";

    for (unsigned int i = 0; i < keyStrs.size(); i++) {
        KVDBData rKey = KVDBData(keyStrs[i], seqlen(keyStrs[i]) + 1);
        KVDBData rVal = KVDBData(commonVal, seqlen(commonVal) + 1);

        keyVals[rKey] = rVal;
    }

    KVDBData rKey5{keyStrs[5], seqlen(keyStrs[5]) + 1};
    KVDBData rVal5 = keyVals[rKey5];

    KVDBData rKey7{keyStrs[7], seqlen(keyStrs[7]) + 1};
    KVDBData rVal7 = keyVals[rKey7];

    for (map<KVDBData, KVDBData>::iterator iter = keyVals.begin(); iter != keyVals.end(); iter++) {
        st = _db.kvs_put(_kvsHandles[0], 0, iter->first, iter->second);
        ASSERT_EQUALS(0, st.getErrno());
    }

    // Begin a scan

    bool eof = false;
    KVDBData prefix{};
    const struct CompParms compparms {};
    hse::KvsCursor* cursor = create_cursor(_kvsHandles[0], prefix, true, compparms);
    ASSERT_FALSE(cursor == 0);

    // Do a seek to added element.
    KVDBData peekKey{};
    st = cursor->seek(rKey5, 0, &peekKey);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(rKey5 == peekKey);

    KVDBData fKey{};
    KVDBData fVal{};

    st = cursor->read(fKey, fVal, eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT(!eof);

    ASSERT_TRUE(fVal == rVal5);
    ASSERT_TRUE(fKey == rKey5);

    // Do a seek to added element.
    st = cursor->seek(rKey7, 0, &peekKey);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(rKey7 == peekKey);

    st = cursor->read(fKey, fVal, eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT(!eof);

    ASSERT_TRUE(fVal == rVal7);
    ASSERT_TRUE(fKey == rKey7);

    st = cursor->read(fKey, fVal, eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT(eof);

    delete cursor;
    // No free required for this test
}


TEST_F(KVDBREGTEST, KvdbPrefixCursorSeekTest) {

    // Populate the DB

    KVS keyVals{};

    hse::Status st{};

    vector<const uint8_t*> keyStrs{(const uint8_t*)"0",
                                   (const uint8_t*)"1",
                                   (const uint8_t*)"2",
                                   (const uint8_t*)"3",
                                   (const uint8_t*)"30",
                                   (const uint8_t*)"31",
                                   (const uint8_t*)"32",
                                   (const uint8_t*)"33",
                                   (const uint8_t*)"4",
                                   (const uint8_t*)"5",
                                   (const uint8_t*)"6",
                                   (const uint8_t*)"7"};
    const uint8_t* commonVal = (const uint8_t*)"COMMON_VALUE";

    for (unsigned int i = 0; i < keyStrs.size(); i++) {
        KVDBData rKey = KVDBData(keyStrs[i], seqlen(keyStrs[i]) + 1);

        KVDBData rVal = KVDBData(commonVal, seqlen(commonVal) + 1);

        keyVals[rKey] = rVal;
    }

    KVDBData rKey31{keyStrs[5], seqlen(keyStrs[5]) + 1};
    KVDBData rVal31 = keyVals[rKey31];

    KVDBData rKey33{keyStrs[7], seqlen(keyStrs[7]) + 1};
    KVDBData rVal33 = keyVals[rKey33];

    for (map<KVDBData, KVDBData>::iterator iter = keyVals.begin(); iter != keyVals.end(); iter++) {
        st = _db.kvs_put(_kvsHandles[0], 0, iter->first, iter->second);
        ASSERT_EQUALS(0, st.getErrno());
    }

    // Begin a scan
    bool eof = false;
    KVDBData prefix{(const uint8_t*)"3", 1};
    const struct CompParms compparms {};
    hse::KvsCursor* cursor = create_cursor(_kvsHandles[0], prefix, true, compparms);
    ASSERT_FALSE(cursor == 0);

    // Do a seek to added element.
    KVDBData sKey1{(const uint8_t*)"31", 3};
    KVDBData peekKey{};

    st = cursor->seek(sKey1, 0, &peekKey);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(peekKey == sKey1);

    KVDBData fKey{};
    KVDBData fVal{};
    st = cursor->read(fKey, fVal, eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_FALSE(eof);

    ASSERT_TRUE(fVal == rVal31);
    ASSERT_TRUE(fKey == rKey31);

    // Do a seek to added element.
    sKey1 = KVDBData((const uint8_t*)"33", 3);
    st = cursor->seek(sKey1, 0, &peekKey);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(peekKey == sKey1);

    st = cursor->read(fKey, fVal, eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_FALSE(eof);

    ASSERT_TRUE(fVal == rVal33);
    ASSERT_TRUE(fKey == rKey33);

    st = cursor->read(fKey, fVal, eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(eof);

    delete cursor;
    // No free required for this test
}

TEST_F(KVDBREGTEST, KvdbReverseFullScanTest) {

    // Populate the DB
    int dbSize = std::rand() % MAX_DB_SIZE;

    map<KVDBData, int> pInfo;

    KVS keyVals;

    map<KVDBData, KVS> pVals;

    ASSERT(genRandKeyVal(pInfo, pVals, keyVals, dbSize));

    hse::Status st{};

    for (map<KVDBData, KVDBData>::iterator iter = keyVals.begin(); iter != keyVals.end(); iter++) {

        st = _db.kvs_put(_kvsHandles[0], 0, iter->first, iter->second);
        ASSERT_EQUALS(0, st.getErrno());
    }

    // Begin a scan
    bool eof = false;
    KVDBData prefix{};
    const struct CompParms compparms {};
    hse::KvsCursor* cursor = create_cursor(_kvsHandles[0], prefix, false, compparms);
    ASSERT_FALSE(cursor == 0);

    // get
    for (map<KVDBData, KVDBData>::reverse_iterator iter = keyVals.rbegin(); iter != keyVals.rend();
         iter++) {

        KVDBData cKey{};
        KVDBData cVal{};

        st = cursor->read(cKey, cVal, eof);

        ASSERT(!eof);
        ASSERT_EQUALS(0, st.getErrno());

        ASSERT_TRUE(cVal == iter->second);
        ASSERT_TRUE(cKey == iter->first);
    }

    KVDBData cKey{};
    KVDBData cVal{};

    st = cursor->read(cKey, cVal, eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT(eof);

    st = cursor->read(cKey, cVal, eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT(eof);

    delete cursor;

    ASSERT(freeKeyVals(keyVals));
}


TEST_F(KVDBREGTEST, KvdbPrefixReverseScanTest) {

    // Populate the DB
    int dbSize = std::rand() % MAX_DB_SIZE;

    map<KVDBData, int> pInfo;

    ASSERT(genPrefixInfo(pInfo));

    KVS keyVals;

    map<KVDBData, KVS> pVals;

    ASSERT(genRandKeyVal(pInfo, pVals, keyVals, dbSize));

    hse::Status st{};

    for (map<KVDBData, KVDBData>::iterator iter = keyVals.begin(); iter != keyVals.end(); iter++) {

        st = _db.kvs_put(_kvsHandles[0], 0, iter->first, iter->second);
        ASSERT_EQUALS(0, st.getErrno());
    }


    // Begin a scan
    bool eof = false;
    for (auto& item : pInfo) {
        int numPrefixes = item.second;
        KVDBData prefix = item.first;
        KVDBData cKey{};
        KVDBData cVal{};

        const struct CompParms compparms {};
        hse::KvsCursor* cursor = create_cursor(_kvsHandles[0], prefix, false, compparms);
        ASSERT_FALSE(cursor == 0);

        int fCnt = 0;
        // get
        KVS& prefixVals = pVals[item.first];

        for (KVS::reverse_iterator iter = prefixVals.rbegin(); iter != prefixVals.rend(); iter++) {
            st = cursor->read(cKey, cVal, eof);
            ASSERT(!eof);
            ASSERT_EQUALS(0, st.getErrno());

            ASSERT_TRUE(cVal == iter->second);
            ASSERT_TRUE(cKey == iter->first);

            fCnt++;
        }

        ASSERT_EQUALS(numPrefixes, fCnt);

        st = cursor->read(cKey, cVal, eof);
        ASSERT_EQUALS(0, st.getErrno());
        ASSERT(eof);

        st = cursor->read(cKey, cVal, eof);
        ASSERT_EQUALS(0, st.getErrno());
        ASSERT(eof);

        delete cursor;
    }

    ASSERT(freeKeyVals(keyVals));
}


TEST_F(KVDBREGTEST, KvdbReverseCursorSeekTest) {

    // Populate the DB

    KVS keyVals{};

    hse::Status st{};

    vector<const uint8_t*> keyStrs{(const uint8_t*)"0",
                                   (const uint8_t*)"1",
                                   (const uint8_t*)"2",
                                   (const uint8_t*)"3",
                                   (const uint8_t*)"4",
                                   (const uint8_t*)"5",
                                   (const uint8_t*)"6",
                                   (const uint8_t*)"7"};
    const uint8_t* commonVal = (const uint8_t*)"COMMON_VALUE";

    for (unsigned int i = 0; i < keyStrs.size(); i++) {
        KVDBData rKey = KVDBData(keyStrs[i], seqlen(keyStrs[i]) + 1);
        KVDBData rVal = KVDBData(commonVal, seqlen(commonVal) + 1);

        keyVals[rKey] = rVal;
    }

    KVDBData rKey0{keyStrs[0], seqlen(keyStrs[0]) + 1};
    KVDBData rVal0 = keyVals[rKey0];

    KVDBData rKey2{keyStrs[2], seqlen(keyStrs[2]) + 1};
    KVDBData rVal2 = keyVals[rKey2];

    for (map<KVDBData, KVDBData>::iterator iter = keyVals.begin(); iter != keyVals.end(); iter++) {
        st = _db.kvs_put(_kvsHandles[0], 0, iter->first, iter->second);
        ASSERT_EQUALS(0, st.getErrno());
    }

    // Begin a scan

    bool eof = false;
    KVDBData prefix{};
    const struct CompParms compparms {};
    hse::KvsCursor* cursor = create_cursor(_kvsHandles[0], prefix, false, compparms);
    ASSERT_FALSE(cursor == 0);

    // Do a seek to added element.
    KVDBData peekKey{};
    st = cursor->seek(rKey2, 0, &peekKey);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(peekKey == rKey2);


    KVDBData fKey{};
    KVDBData fVal{};
    st = cursor->read(fKey, fVal, eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_FALSE(eof);


    ASSERT_TRUE(fVal == rVal2);

    ASSERT_TRUE(fKey == rKey2);

    // Do a seek to added element.
    st = cursor->seek(rKey0, 0, &peekKey);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(peekKey == rKey0);

    st = cursor->read(fKey, fVal, eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_FALSE(eof);

    ASSERT_TRUE(fVal == rVal0);
    ASSERT_TRUE(fKey == rKey0);

    st = cursor->read(fKey, fVal, eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(eof);

    delete cursor;

    // No free required for this test
}


TEST_F(KVDBREGTEST, KvdbPrefixReverseCursorSeekTest) {

    // Populate the DB

    KVS keyVals{};

    hse::Status st{};

    vector<const uint8_t*> keyStrs{(const uint8_t*)"0",
                                   (const uint8_t*)"1",
                                   (const uint8_t*)"20",
                                   (const uint8_t*)"21",
                                   (const uint8_t*)"30",
                                   (const uint8_t*)"31",
                                   (const uint8_t*)"32",
                                   (const uint8_t*)"33",
                                   (const uint8_t*)"4",
                                   (const uint8_t*)"5",
                                   (const uint8_t*)"6",
                                   (const uint8_t*)"7"};
    const uint8_t* commonVal = (const uint8_t*)"COMMON_VALUE";

    for (unsigned int i = 0; i < keyStrs.size(); i++) {
        KVDBData rKey = KVDBData(keyStrs[i], seqlen(keyStrs[i]) + 1);
        KVDBData rVal = KVDBData(commonVal, seqlen(commonVal) + 1);

        keyVals[rKey] = rVal;
    }

    KVDBData rKey32{keyStrs[6], seqlen(keyStrs[6]) + 1};
    KVDBData rVal32 = keyVals[rKey32];

    KVDBData rKey30{keyStrs[4], seqlen(keyStrs[4]) + 1};
    KVDBData rVal30 = keyVals[rKey30];

    for (map<KVDBData, KVDBData>::iterator iter = keyVals.begin(); iter != keyVals.end(); iter++) {

        st = _db.kvs_put(_kvsHandles[0], 0, iter->first, iter->second);
        ASSERT_EQUALS(0, st.getErrno());
    }

    // Begin a scan
    KVDBData prefix{(const uint8_t*)"3", 1};
    bool eof = false;

    const struct CompParms compparms {};
    hse::KvsCursor* cursor = create_cursor(_kvsHandles[0], prefix, false, compparms);
    ASSERT_FALSE(cursor == 0);

    // Do a seek to added element.
    KVDBData sKey1{(const uint8_t*)"32", 3};
    KVDBData peekKey{};

    st = cursor->seek(sKey1, 0, &peekKey);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(peekKey == sKey1);

    KVDBData fKey{};
    KVDBData fVal{};
    st = cursor->read(fKey, fVal, eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_FALSE(eof);

    ASSERT_TRUE(fVal == rVal32);
    ASSERT_TRUE(fKey == rKey32);

    // Do a seek to added element.
    sKey1 = KVDBData((const uint8_t*)"30", 3);

    st = cursor->seek(sKey1, 0, &peekKey);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(peekKey == sKey1);

    st = cursor->read(fKey, fVal, eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_FALSE(eof);

    ASSERT_TRUE(fVal == rVal30);
    ASSERT_TRUE(fKey == rKey30);

    st = cursor->read(fKey, fVal, eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(eof);

    delete cursor;

    // No free required for this test
}

TEST_F(KVDBREGTEST, KvdbTxnIsolationTest) {
    hse::Status st{};

    KVDBData key1{(const uint8_t*)"k1", strlen("k1") + 1};
    KVDBData key2{(const uint8_t*)"k2", strlen("k2") + 1};
    KVDBData key3{(const uint8_t*)"k3", strlen("k3") + 1};

    KVDBData val1{(const uint8_t*)"v1", strlen("v1") + 1};
    KVDBData val2{(const uint8_t*)"v2", strlen("v2") + 1};
    KVDBData val3{(const uint8_t*)"v3", strlen("v3") + 1};

    ClientTxn* txn = new ClientTxn((dynamic_cast<KVDBImpl&>(_db)).kvdb_handle());
    ASSERT_FALSE(txn == 0);

    st = txn->begin();
    ASSERT_EQUALS(0, st.getErrno());

    st = _db.kvs_put(_kvsHandles[0], txn, key2, val2);
    ASSERT_EQUALS(0, st.getErrno());

    st = txn->commit();
    ASSERT_EQUALS(0, st.getErrno());

    st = txn->begin();
    ASSERT_EQUALS(0, st.getErrno());

    // do a put
    st = _db.kvs_put(_kvsHandles[0], txn, key1, val1);
    ASSERT_EQUALS(0, st.getErrno());

    // do a get
    KVDBData val{};
    bool found = false;

    st = getcoUtil(_db, _kvsHandles[0], txn, key1, val, found);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT(found);
    ASSERT(val == val1);

    found = false;
    st = getcoUtil(_db, _kvsHandles[0], 0, key1, val, found);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_FALSE(found);
    ASSERT_FALSE(val == val1);

    st = txn->commit();
    ASSERT_EQUALS(0, st.getErrno());

    // do a get
    found = false;
    st = getcoUtil(_db, _kvsHandles[0], 0, key1, val, found);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT(found);
    ASSERT(val == val1);

    delete txn;
}


TEST_F(KVDBREGTEST, KvdbUpdateNoTxnTest) {

    KVDBData pref{(const uint8_t*)"k0001", strlen("k0001")};

    KVDBData key1{(const uint8_t*)"k00011", strlen("k00011") + 1};
    KVDBData key2{(const uint8_t*)"k00012", strlen("k00012") + 1};
    KVDBData key3{(const uint8_t*)"k00013", strlen("k00013") + 1};

    KVDBData val1{(const uint8_t*)"v1", strlen("v1") + 1};
    KVDBData val2{(const uint8_t*)"v2", strlen("v2") + 1};
    KVDBData val3{(const uint8_t*)"v3", strlen("v3") + 1};


    // put 2 vals
    auto st = _db.kvs_put(_kvsHandles[0], 0, key1, val1);
    ASSERT_EQUALS(0, st.getErrno());

    st = _db.kvs_put(_kvsHandles[0], 0, key2, val2);
    ASSERT_EQUALS(0, st.getErrno());

    // create cursor
    const struct CompParms compparms {};
    hse::KvsCursor* cursor = create_cursor(_kvsHandles[0], pref, true, compparms);
    ASSERT_FALSE(cursor == 0);

    // iterate
    bool eof = false;
    KVDBData cKey{};
    KVDBData cVal{};

    st = cursor->read(cKey, cVal, eof);
    ASSERT(!eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(cKey == key1);
    ASSERT_TRUE(cVal == val1);

    st = cursor->read(cKey, cVal, eof);
    ASSERT(!eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(cKey == key2);
    ASSERT_TRUE(cVal == val2);

    st = cursor->read(cKey, cVal, eof);
    ASSERT(eof);
    ASSERT_EQUALS(0, st.getErrno());

    // put third val
    st = _db.kvs_put(_kvsHandles[0], 0, key3, val3);
    ASSERT_EQUALS(0, st.getErrno());

    // update
    st = cursor->update();
    ASSERT_EQUALS(0, st.getErrno());

    st = cursor->seek(pref, 0, nullptr);
    ASSERT_EQUALS(0, st.getErrno());

    st = cursor->read(cKey, cVal, eof);
    ASSERT(!eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(cKey == key1);
    ASSERT_TRUE(cVal == val1);

    st = cursor->read(cKey, cVal, eof);
    ASSERT(!eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(cKey == key2);
    ASSERT_TRUE(cVal == val2);

    st = cursor->read(cKey, cVal, eof);
    ASSERT(!eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(cKey == key3);
    ASSERT_TRUE(cVal == val3);

    st = cursor->read(cKey, cVal, eof);
    ASSERT(eof);
    ASSERT_EQUALS(0, st.getErrno());

    delete cursor;
}

TEST_F(KVDBREGTEST, KvdbUpdateTxnTest) {
    KVDBData pref{(const uint8_t*)"k0001", strlen("k0001")};

    KVDBData key1{(const uint8_t*)"k00011", strlen("k00011") + 1};
    KVDBData key2{(const uint8_t*)"k00012", strlen("k00012") + 1};
    KVDBData key3{(const uint8_t*)"k00013", strlen("k00013") + 1};

    KVDBData val1{(const uint8_t*)"v1", strlen("v1") + 1};
    KVDBData val2{(const uint8_t*)"v2", strlen("v2") + 1};
    KVDBData val3{(const uint8_t*)"v3", strlen("v3") + 1};


    // First test no txn

    // put 2 vals
    auto st = _db.kvs_put(_kvsHandles[0], 0, key1, val1);
    ASSERT_EQUALS(0, st.getErrno());

    st = _db.kvs_put(_kvsHandles[0], 0, key2, val2);
    ASSERT_EQUALS(0, st.getErrno());


    // create cursor
    const struct CompParms compparms {};
    hse::KvsCursor* cursor = create_cursor(_kvsHandles[0], pref, true, compparms);
    ASSERT_FALSE(cursor == 0);

    // iterate
    bool eof = false;
    KVDBData cKey{};
    KVDBData cVal{};

    st = cursor->read(cKey, cVal, eof);
    ASSERT(!eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(cKey == key1);
    ASSERT_TRUE(cVal == val1);

    st = cursor->read(cKey, cVal, eof);
    ASSERT(!eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(cKey == key2);
    ASSERT_TRUE(cVal == val2);

    st = cursor->read(cKey, cVal, eof);
    ASSERT(eof);
    ASSERT_EQUALS(0, st.getErrno());


    // Create a txn put a third key/val and commit
    ClientTxn* txn = new ClientTxn(_db.kvdb_handle());
    ASSERT_FALSE(txn == 0);

    st = txn->begin();
    ASSERT_EQUALS(0, st.getErrno());

    // do a put
    st = _db.kvs_put(_kvsHandles[0], txn, key3, val3);
    ASSERT_EQUALS(0, st.getErrno());

    st = txn->commit();
    ASSERT_EQUALS(0, st.getErrno());

    delete txn;

    // update
    st = cursor->update();
    ASSERT_EQUALS(0, st.getErrno());

    st = cursor->seek(pref, 0, 0);
    ASSERT_EQUALS(0, st.getErrno());

    st = cursor->read(cKey, cVal, eof);
    ASSERT(!eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(cKey == key1);
    ASSERT_TRUE(cVal == val1);

    st = cursor->read(cKey, cVal, eof);
    ASSERT(!eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(cKey == key2);
    ASSERT_TRUE(cVal == val2);

    st = cursor->read(cKey, cVal, eof);
    ASSERT(!eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(cKey == key3);
    ASSERT_TRUE(cVal == val3);

    st = cursor->read(cKey, cVal, eof);
    ASSERT(eof);
    ASSERT_EQUALS(0, st.getErrno());

    delete cursor;
}


TEST_F(KVDBREGTEST, KvdbDeleteKeyCursorTest) {
    KVDBData pref{(const uint8_t*)"k0001", strlen("k0001")};

    KVDBData key1{(const uint8_t*)"k00011", strlen("k00011") + 1};
    KVDBData key2{(const uint8_t*)"k00012", strlen("k00012") + 1};
    KVDBData key3{(const uint8_t*)"k00013", strlen("k00013") + 1};

    KVDBData val1{(const uint8_t*)"v1", strlen("v1") + 1};
    KVDBData val2{(const uint8_t*)"v2", strlen("v2") + 1};
    KVDBData val3{(const uint8_t*)"v3", strlen("v3") + 1};

    // put 3 vals
    auto st = _db.kvs_put(_kvsHandles[0], 0, key1, val1);
    ASSERT_EQUALS(0, st.getErrno());

    st = _db.kvs_put(_kvsHandles[0], 0, key2, val2);
    ASSERT_EQUALS(0, st.getErrno());

    st = _db.kvs_put(_kvsHandles[0], 0, key3, val3);
    ASSERT_EQUALS(0, st.getErrno());


    // create cursor
    const struct CompParms compparms {};
    hse::KvsCursor* cursor = create_cursor(_kvsHandles[0], pref, true, compparms);
    ASSERT_FALSE(cursor == 0);

    // iterate
    bool eof = false;
    KVDBData cKey{};
    KVDBData cVal{};

    st = cursor->read(cKey, cVal, eof);
    ASSERT(!eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(cKey == key1);
    ASSERT_TRUE(cVal == val1);

    st = cursor->read(cKey, cVal, eof);
    ASSERT(!eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(cKey == key2);
    ASSERT_TRUE(cVal == val2);

    st = cursor->read(cKey, cVal, eof);
    ASSERT(!eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(cKey == key3);
    ASSERT_TRUE(cVal == val3);

    st = cursor->read(cKey, cVal, eof);
    ASSERT(eof);
    ASSERT_EQUALS(0, st.getErrno());

    // Destroy cursor
    delete cursor;

    // do a delete
    st = _db.kvs_delete(_kvsHandles[0], 0, key3);
    ASSERT_EQUALS(0, st.getErrno());

    // Recreate the cursor and iterate
    cursor = create_cursor(_kvsHandles[0], pref, true, compparms);
    ASSERT_FALSE(cursor == 0);

    st = cursor->read(cKey, cVal, eof);
    ASSERT(!eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(cKey == key1);
    ASSERT_TRUE(cVal == val1);

    st = cursor->read(cKey, cVal, eof);
    ASSERT(!eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(cKey == key2);
    ASSERT_TRUE(cVal == val2);

    st = cursor->read(cKey, cVal, eof);
    ASSERT(eof);
    ASSERT_EQUALS(0, st.getErrno());

    delete cursor;
}


TEST_F(KVDBREGTEST, KvdbDeleteTxnCursorTest) {
    KVDBData pref{(const uint8_t*)"k0001", strlen("k0001")};

    KVDBData key1{(const uint8_t*)"k00011", strlen("k00011") + 1};
    KVDBData key2{(const uint8_t*)"k00012", strlen("k00012") + 1};
    KVDBData key3{(const uint8_t*)"k00013", strlen("k00013") + 1};

    KVDBData val1{(const uint8_t*)"v1", strlen("v1") + 1};
    KVDBData val2{(const uint8_t*)"v2", strlen("v2") + 1};
    KVDBData val3{(const uint8_t*)"v3", strlen("v3") + 1};

    // put 3 vals
    auto st = _db.kvs_put(_kvsHandles[0], 0, key1, val1);
    ASSERT_EQUALS(0, st.getErrno());

    st = _db.kvs_put(_kvsHandles[0], 0, key2, val2);
    ASSERT_EQUALS(0, st.getErrno());

    st = _db.kvs_put(_kvsHandles[0], 0, key3, val3);
    ASSERT_EQUALS(0, st.getErrno());


    // create cursor
    const struct CompParms compparms {};
    hse::KvsCursor* cursor = create_cursor(_kvsHandles[0], pref, true, compparms);
    ASSERT_FALSE(cursor == 0);

    // iterate
    bool eof = false;
    KVDBData cKey{};
    KVDBData cVal{};

    st = cursor->read(cKey, cVal, eof);
    ASSERT(!eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(cKey == key1);
    ASSERT_TRUE(cVal == val1);

    st = cursor->read(cKey, cVal, eof);
    ASSERT(!eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(cKey == key2);
    ASSERT_TRUE(cVal == val2);

    st = cursor->read(cKey, cVal, eof);
    ASSERT(!eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(cKey == key3);
    ASSERT_TRUE(cVal == val3);

    st = cursor->read(cKey, cVal, eof);
    ASSERT(eof);
    ASSERT_EQUALS(0, st.getErrno());

    // Destroy cursor
    delete cursor;

    // Create a txn and delete third key/val and commit
    ClientTxn* txn = new ClientTxn(_db.kvdb_handle());
    ASSERT_FALSE(txn == 0);

    st = txn->begin();
    ASSERT_EQUALS(0, st.getErrno());

    // do a put
    st = _db.kvs_delete(_kvsHandles[0], txn, key3);
    ASSERT_EQUALS(0, st.getErrno());

    st = txn->commit();
    ASSERT_EQUALS(0, st.getErrno());

    delete txn;

    // Recreate the cursor and iterate
    cursor = create_cursor(_kvsHandles[0], pref, true, compparms);
    ASSERT_FALSE(cursor == 0);

    st = cursor->read(cKey, cVal, eof);
    ASSERT(!eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(cKey == key1);
    ASSERT_TRUE(cVal == val1);

    st = cursor->read(cKey, cVal, eof);
    ASSERT(!eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(cKey == key2);
    ASSERT_TRUE(cVal == val2);

    st = cursor->read(cKey, cVal, eof);
    ASSERT(eof);
    ASSERT_EQUALS(0, st.getErrno());

    delete cursor;
}

TEST_F(KVDBREGTEST, KvdbProbeTxnTest) {
    KVDBData pref{(const uint8_t*)"k0001", strlen("k0001")};

    KVDBData key1{(const uint8_t*)"k00011", strlen("k00011") + 1};
    KVDBData key2{(const uint8_t*)"k00012", strlen("k00012") + 1};

    KVDBData val1{(const uint8_t*)"v1", strlen("v1") + 1};
    KVDBData val2{(const uint8_t*)"v2", strlen("v2") + 1};


    // put 2 vals
    auto st = _db.kvs_put(_kvsHandles[0], 0, key1, val1);
    ASSERT_EQUALS(0, st.getErrno());

    st = _db.kvs_put(_kvsHandles[0], 0, key2, val2);
    ASSERT_EQUALS(0, st.getErrno());

    // Create a txn and delete third key/val and commit
    ClientTxn* txn = new ClientTxn(_db.kvdb_handle());
    ASSERT_FALSE(txn == 0);

    st = txn->begin();
    ASSERT_EQUALS(0, st.getErrno());

    st = _db.kvs_delete(_kvsHandles[0], txn, key2);
    ASSERT_EQUALS(0, st.getErrno());

    // do probe on 1st key
    bool found = false;
    st = _db.kvs_probe_key(_kvsHandles[0], txn, key1, found);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(found);

    // do probe on 2nd Key
    st = _db.kvs_probe_key(_kvsHandles[0], txn, key2, found);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_FALSE(found);

    st = txn->commit();
    ASSERT_EQUALS(0, st.getErrno());

    delete txn;
}

TEST_F(KVDBREGTEST, KvdbPrefixDeleteTest) {
    hse::Status st{};

    // Populate the DB
    int dbSize = std::rand() % MAX_DB_SIZE;

    map<KVDBData, int> pInfo;
    ASSERT(genPrefixInfo(pInfo));

    KVS keyVals;
    map<KVDBData, KVS> pVals;
    ASSERT(genRandKeyVal(pInfo, pVals, keyVals, dbSize));

    for (map<KVDBData, KVDBData>::iterator iter = keyVals.begin(); iter != keyVals.end(); iter++) {

        st = _db.kvs_put(_kvsHandles[0], 0, iter->first, iter->second);
        ASSERT_EQUALS(0, st.getErrno());
    }

    // select a prefix to delete
    map<KVDBData, int>::iterator i = pInfo.begin();
    KVDBData prefix{i->first};

    // Do a scan
    bool eof = false;
    const struct CompParms compparms {};
    hse::KvsCursor* cursor = create_cursor(_kvsHandles[0], prefix, true, compparms);
    ASSERT_FALSE(cursor == 0);

    KVDBData cKey{};
    KVDBData cVal{};
    st = cursor->read(cKey, cVal, eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_FALSE(eof);

    delete cursor;

    st = _db.kvs_prefix_delete(_kvsHandles[0], 0, prefix);
    ASSERT_EQUALS(0, st.getErrno());

    // Do a scan
    cursor = create_cursor(_kvsHandles[0], prefix, true, compparms);
    ASSERT_FALSE(cursor == 0);

    st = cursor->read(cKey, cVal, eof);
    ASSERT_EQUALS(0, st.getErrno());
    ASSERT_TRUE(eof);

    delete cursor;

    ASSERT(freeKeyVals(keyVals));
}

TEST_F(KVDBREGTEST, KvdbTransactionTest) {
    hse::Status st{};

    ClientTxn* txn = new ClientTxn(_db.kvdb_handle());
    ASSERT_FALSE(txn == 0);

    st = txn->begin();
    ASSERT_EQUALS(0, st.getErrno());

    st = txn->commit();
    ASSERT_EQUALS(0, st.getErrno());

    st = txn->commit();
    ASSERT_EQUALS(EINVAL, st.getErrno());

    st = txn->begin();
    ASSERT_EQUALS(0, st.getErrno());

    st = txn->abort();
    ASSERT_EQUALS(0, st.getErrno());

    st = txn->commit();
    ASSERT_EQUALS(EINVAL, st.getErrno());

    delete txn;
}
}
