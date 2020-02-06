/**
 * TODO: HSE License
 */
#include "mongo/platform/basic.h"

#include <cerrno>
#include <memory>
#include <vector>

#include <boost/filesystem/operations.hpp>

#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/storage/record_store_test_harness.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"

#include "hse_impl.h"
#include "hse_record_store.h"
#include "hse_recovery_unit.h"
#include "hse_ut_common.h"

namespace mongo {

using std::string;

using hse::VALUE_META_THRESHOLD_LEN;
using hse::DEFAULT_PFX_LEN;
using hse::OPLOG_PFX_LEN;


class KVDBRecordStoreHarnessHelper final : public HarnessHelper {
public:
    KVDBRecordStoreHarnessHelper() {
        setupDb();
        _prefix = 1U;
        _ident = "ColIdent";

        _counterManager.reset(new KVDBCounterManager(true));
        _durabilityManager.reset(new KVDBDurabilityManager(_db, false, 0));
        _compparms = {};
    }

    ~KVDBRecordStoreHarnessHelper() {
        teardownDb();
    }


    virtual std::unique_ptr<RecordStore> newNonCappedRecordStore() {
        return newNonCappedRecordStore("foo.bar");
    }
    std::unique_ptr<RecordStore> newNonCappedRecordStore(const std::string& ns) {
        auto opCtx = newOperationContext();

        return stdx::make_unique<KVDBRecordStore>(opCtx.get(),
                                                  ns,
                                                  "1",
                                                  _db,
                                                  _colKvs,
                                                  _largeKvs,
                                                  _prefix,
                                                  *_durabilityManager.get(),
                                                  *_counterManager.get(),
                                                  _compparms);
    }

    std::unique_ptr<RecordStore> newCappedRecordStore(int64_t cappedMaxSize,
                                                      int64_t cappedMaxDocs) final {
        return newCappedRecordStore("a.b", cappedMaxSize, cappedMaxDocs);
    }

    std::unique_ptr<RecordStore> newCappedRecordStore(const std::string& ns,
                                                      int64_t cappedMaxSize,
                                                      int64_t cappedMaxDocs) {

        auto opCtx = newOperationContext();

        if (NamespaceString::oplog(ns)) {
            return stdx::make_unique<KVDBOplogStore>(opCtx.get(),
                                                     ns,
                                                     "1",
                                                     _db,
                                                     _oplogKvs,
                                                     _oplogLargeKvs,
                                                     _prefix,
                                                     *_durabilityManager.get(),
                                                     *_counterManager.get(),
                                                     cappedMaxSize,
                                                     _compparms);

        } else {
            return stdx::make_unique<KVDBCappedRecordStore>(opCtx.get(),
                                                            ns,
                                                            "1",
                                                            _db,
                                                            _colKvs,
                                                            _largeKvs,
                                                            _prefix,
                                                            *_durabilityManager.get(),
                                                            *_counterManager.get(),
                                                            cappedMaxSize,
                                                            cappedMaxDocs,
                                                            _compparms);
        }
    }

    RecoveryUnit* newRecoveryUnit() final {
        return new KVDBRecoveryUnit(_db, *_counterManager.get(), *_durabilityManager.get());
    }

    bool supportsDocLocking() final {
        return true;
    }

    void setupDb() {
        hse_params_create(&_colKvsCParams);
        hse_params_create(&_idxKvsCParams);
        hse_params_create(&_largeKvsCParams);
        hse_params_create(&_oplogKvsCParams);
        hse_params_create(&_oplogLargeKvsCParams);

        hse_params_set(
            _colKvsCParams, "kvs.cparams.pfxlen", std::to_string(hse::DEFAULT_PFX_LEN).c_str());
        hse::Status hseSt = _db.kvdb_kvs_make(_colKvsName.c_str(), _colKvsCParams);
        invariantHseSt(hseSt);

        hseSt = _db.kvdb_kvs_open(_colKvsName.c_str(), nullptr, _colKvs);
        invariantHseSt(hseSt);

        hse_params_set(
            _idxKvsCParams, "kvs.cparams.pfxlen", std::to_string(hse::DEFAULT_PFX_LEN).c_str());
        hseSt = _db.kvdb_kvs_make(_idxKvsName.c_str(), _idxKvsCParams);
        invariantHseSt(hseSt);

        hseSt = _db.kvdb_kvs_open(_idxKvsName.c_str(), nullptr, _idxKvs);
        invariantHseSt(hseSt);

        hse_params_set(
            _largeKvsCParams, "kvs.cparams.pfxlen", std::to_string(hse::DEFAULT_PFX_LEN).c_str());
        hseSt = _db.kvdb_kvs_make(_largeKvsName.c_str(), _largeKvsCParams);
        invariantHseSt(hseSt);

        hseSt = _db.kvdb_kvs_open(_largeKvsName.c_str(), nullptr, _largeKvs);
        invariantHseSt(hseSt);

        hse_params_set(
            _oplogKvsCParams, "kvs.cparams.pfxlen", std::to_string(hse::OPLOG_PFX_LEN).c_str());
        hseSt = _db.kvdb_kvs_make(_oplogKvsName.c_str(), _oplogKvsCParams);
        invariantHseSt(hseSt);

        hseSt = _db.kvdb_kvs_open(_oplogKvsName.c_str(), nullptr, _oplogKvs);
        invariantHseSt(hseSt);

        hse_params_set(_oplogLargeKvsCParams,
                       "kvs.cparams.pfxlen",
                       std::to_string(hse::OPLOG_PFX_LEN).c_str());
        hseSt = _db.kvdb_kvs_make(_oplogLargeKvsName.c_str(), _oplogLargeKvsCParams);
        invariantHseSt(hseSt);

        hseSt = _db.kvdb_kvs_open(_oplogLargeKvsName.c_str(), nullptr, _oplogLargeKvs);
        invariantHseSt(hseSt);

        hse_params_destroy(_colKvsCParams);
        hse_params_destroy(_idxKvsCParams);
        hse_params_destroy(_largeKvsCParams);
        hse_params_destroy(_oplogKvsCParams);
        hse_params_destroy(_oplogLargeKvsCParams);
    }

    void teardownDb() {
        auto hseSt = _db.kvdb_kvs_close(_colKvs);
        invariantHseSt(hseSt);

        hseSt = _db.kvdb_kvs_close(_idxKvs);
        invariantHseSt(hseSt);

        hseSt = _db.kvdb_kvs_close(_largeKvs);
        invariantHseSt(hseSt);

        hseSt = _db.kvdb_kvs_close(_oplogKvs);
        invariantHseSt(hseSt);

        hseSt = _db.kvdb_kvs_close(_oplogLargeKvs);
        invariantHseSt(hseSt);

        // drops all kvses + drops kvdb if requested via KVDB_PER_UT env
        _dbFixture.reset();
    }

private:
    string _colKvsName = "ColKVS";
    struct hse_params* _colKvsCParams;
    KVSHandle _colKvs;

    string _idxKvsName = "IdxKVS";
    struct hse_params* _idxKvsCParams;
    KVSHandle _idxKvs;

    string _largeKvsName = "LargeKVS";
    struct hse_params* _largeKvsCParams;
    KVSHandle _largeKvs;

    string _oplogKvsName = "OplogKVS";
    struct hse_params* _oplogKvsCParams;
    KVSHandle _oplogKvs;

    string _oplogLargeKvsName = "OplogLargeKVS";
    struct hse_params* _oplogLargeKvsCParams;
    KVSHandle _oplogLargeKvs;

    hse::KVDBTestSuiteFixture& _dbFixture = KVDBTestSuiteFixture::getFixture();
    hse::KVDB& _db = _dbFixture.getDb();
    std::unique_ptr<KVDBDurabilityManager> _durabilityManager;
    std::unique_ptr<KVDBCounterManager> _counterManager;
    uint32_t _prefix;
    string _ident;
    struct CompParms _compparms;
};

std::unique_ptr<HarnessHelper> newHarnessHelper() {
    return stdx::make_unique<KVDBRecordStoreHarnessHelper>();
}


TEST(KVDBRecordStoreTest, Isolation1) {
    std::unique_ptr<HarnessHelper> harnessHelper(newHarnessHelper());
    std::unique_ptr<RecordStore> rs(harnessHelper->newNonCappedRecordStore());

    RecordId loc1;
    RecordId loc2;

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper->newOperationContext());
        {
            WriteUnitOfWork uow(opCtx.get());

            StatusWith<RecordId> res = rs->insertRecord(opCtx.get(), "a", 2, false);
            ASSERT_OK(res.getStatus());
            loc1 = res.getValue();

            res = rs->insertRecord(opCtx.get(), "a", 2, false);
            ASSERT_OK(res.getStatus());
            loc2 = res.getValue();

            uow.commit();
        }
    }


    {
        ServiceContext::UniqueOperationContext t1(harnessHelper->newOperationContext());
        auto client2 = harnessHelper->serviceContext()->makeClient("c2");
        auto t2 = harnessHelper->newOperationContext(client2.get());

        std::unique_ptr<WriteUnitOfWork> w1(new WriteUnitOfWork(t1.get()));
        std::unique_ptr<WriteUnitOfWork> w2(new WriteUnitOfWork(t2.get()));

        rs->dataFor(t1.get(), loc1);
        rs->dataFor(t2.get(), loc1);

        ASSERT_OK(rs->updateRecord(t1.get(), loc1, "b", 2, false, NULL));
        ASSERT_OK(rs->updateRecord(t1.get(), loc2, "B", 2, false, NULL));

        // this should throw
        ASSERT_THROWS(rs->updateRecord(t2.get(), loc1, "c", 2, false, NULL),
                      WriteConflictException);

        w1->commit();  // this should succeed
    }
}

TEST(KVDBRecordStoreTest, Isolation2) {
    std::unique_ptr<HarnessHelper> harnessHelper(newHarnessHelper());
    std::unique_ptr<RecordStore> rs(harnessHelper->newNonCappedRecordStore());

    RecordId loc1;
    RecordId loc2;

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper->newOperationContext());
        {
            WriteUnitOfWork uow(opCtx.get());

            StatusWith<RecordId> res = rs->insertRecord(opCtx.get(), "a", 2, false);
            ASSERT_OK(res.getStatus());
            loc1 = res.getValue();

            res = rs->insertRecord(opCtx.get(), "a", 2, false);
            ASSERT_OK(res.getStatus());
            loc2 = res.getValue();

            uow.commit();
        }
    }

    {
        ServiceContext::UniqueOperationContext t1(harnessHelper->newOperationContext());
        auto client2 = harnessHelper->serviceContext()->makeClient("c2");
        auto t2 = harnessHelper->newOperationContext(client2.get());

        // ensure we start transactions
        rs->dataFor(t1.get(), loc2);
        rs->dataFor(t2.get(), loc2);

        {
            WriteUnitOfWork w(t1.get());
            ASSERT_OK(rs->updateRecord(t1.get(), loc1, "b", 2, false, NULL));
            w.commit();
        }

        {
            WriteUnitOfWork w(t2.get());
            ASSERT_EQUALS(string("a"), rs->dataFor(t2.get(), loc1).data());
            // this should fail as our version of loc1 is too old
            ASSERT_THROWS(rs->updateRecord(t2.get(), loc1, "c", 2, false, NULL),
                          WriteConflictException);
        }
    }
}

std::string random_string(size_t length) {
    auto randchar = []() -> char {
        const char charset[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
        const size_t max_index = (sizeof(charset) - 1);
        return charset[rand() % max_index];
    };

    std::string str(length, 0);
    std::generate_n(str.begin(), length, randchar);

    return str;
}

TEST(KVDBRecordStoreTest, Chunker) {
    std::unique_ptr<HarnessHelper> harnessHelper(newHarnessHelper());
    std::unique_ptr<RecordStore> rs(harnessHelper->newNonCappedRecordStore());

    int i, j, num_records, index, test;
    unsigned int length, prev;
    const int num_values = 5;
    unsigned int lengths[num_values] = {VALUE_META_THRESHOLD_LEN - 1,
                                        VALUE_META_THRESHOLD_LEN,
                                        HSE_KVS_VLEN_MAX,
                                        HSE_KVS_VLEN_MAX * 2,
                                        16 * 1024 * 1024};
    string strings[num_values];
    RecordId locs[num_values];
    RecordData record;
    struct CompParms comparms_comp = {ALGO_LZ4, 0, true};

    for (i = 0; i < num_values; i++)
        strings[i] = random_string(lengths[i] - 1);

    for (test = 0; test < 2; test++) {
        num_records = 0;
        length = 0;

        if (test > 0) {
            KVDBRecordStore* rrs = dynamic_cast<KVDBRecordStore*>(rs.get());
            rrs->setCompParms(comparms_comp);
        }
        {
            ServiceContext::UniqueOperationContext opCtx(harnessHelper->newOperationContext());
            {
                WriteUnitOfWork uow(opCtx.get());

                for (i = 0; i < num_values; i++) {
                    StatusWith<RecordId> res =
                        rs->insertRecord(opCtx.get(), strings[i].c_str(), lengths[i], false);
                    ASSERT_OK(res.getStatus());
                    locs[i] = res.getValue();

                    num_records++;
                    length += lengths[i];

                    ASSERT_EQUALS(rs->numRecords(opCtx.get()), num_records);
                    ASSERT_EQUALS(rs->dataSize(opCtx.get()), length);

                    // Validate the contents of the record.
                    record = rs->dataFor(opCtx.get(), locs[i]);
                    ASSERT_EQUALS(lengths[i], static_cast<size_t>(record.size()));
                    ASSERT_EQUALS(record.data(), strings[i]);

                    prev = lengths[i];

                    // Update the record contents with each one of the values (round robin
                    // over them). The last update leaves it with its original value.
                    for (j = 1; j <= num_values; j++) {
                        index = (i + j) % num_values;
                        ASSERT_OK(rs->updateRecord(opCtx.get(),
                                                   locs[i],
                                                   strings[index].c_str(),
                                                   lengths[index],
                                                   false,
                                                   NULL));

                        length = length - prev + lengths[index];
                        prev = lengths[index];

                        ASSERT_EQUALS(rs->numRecords(opCtx.get()), num_records);
                        ASSERT_EQUALS(rs->dataSize(opCtx.get()), length);

                        // Validate the contents of the record.
                        record = rs->dataFor(opCtx.get(), locs[i]);
                        ASSERT_EQUALS(lengths[index], static_cast<size_t>(record.size()));
                        ASSERT_EQUALS(record.data(), strings[index]);
                    }
                }

                uow.commit();
            }
        }

        {
            ServiceContext::UniqueOperationContext opCtx(harnessHelper->newOperationContext());

            {
                auto cursor = rs->getCursor(opCtx.get(), true);

                for (i = 0; i < num_values; i++) {
                    auto item = cursor->seekExact(locs[i]);
                    ASSERT(item);

                    ASSERT_EQUALS(item->id, locs[i]);
                    ASSERT_EQUALS(static_cast<size_t>(item->data.size()), lengths[i]);
                    ASSERT_EQUALS(item->data.data(), strings[i]);
                }
            }
        }

        {
            ServiceContext::UniqueOperationContext opCtx(harnessHelper->newOperationContext());

            {
                auto cursor = rs->getCursor(opCtx.get(), true);
                auto item = cursor->seekExact(locs[0]);

                for (i = 0; i < num_values; i++) {
                    ASSERT(item);
                    ASSERT_EQUALS(item->id, locs[i]);
                    ASSERT_EQUALS(static_cast<size_t>(item->data.size()), lengths[i]);
                    ASSERT_EQUALS(item->data.data(), strings[i]);

                    item = cursor->next();
                }

                ASSERT(!cursor->next());
            }
        }

        {
            ServiceContext::UniqueOperationContext opCtx(harnessHelper->newOperationContext());
            {
                WriteUnitOfWork uow(opCtx.get());

                for (i = 0; i < num_values; i++) {
                    rs->deleteRecord(opCtx.get(), locs[i]);
                    num_records--;
                    length -= lengths[i];

                    ASSERT_EQUALS(rs->numRecords(opCtx.get()), num_records);
                    ASSERT_EQUALS(rs->dataSize(opCtx.get()), length);
                }

                uow.commit();
            }
        }

    } /* for */
}

StatusWith<RecordId> insertBSONTs(ServiceContext::UniqueOperationContext& opCtx,
                                  std::unique_ptr<RecordStore>& rs,
                                  const Timestamp& opTime) {
    BSONObj obj = BSON("ts" << opTime);
    WriteUnitOfWork wuow(opCtx.get());
    KVDBRecordStore* rrs = dynamic_cast<KVDBRecordStore*>(rs.get());
    invariantHse(rrs);
    Status status = rrs->oplogDiskLocRegister(opCtx.get(), opTime);
    if (!status.isOK())
        return StatusWith<RecordId>(status);
    StatusWith<RecordId> res = rs->insertRecord(opCtx.get(), obj.objdata(), obj.objsize(), false);
    if (res.isOK())
        wuow.commit();
    return res;
}

StatusWith<RecordId> insertBSON(ServiceContext::UniqueOperationContext& opCtx,
                                std::unique_ptr<RecordStore>& rs,
                                const RecordId& idVal) {
    BSONObj obj = BSON("ID" << idVal.repr());
    WriteUnitOfWork wuow(opCtx.get());
    KVDBRecordStore* rrs = dynamic_cast<KVDBRecordStore*>(rs.get());
    invariantHse(rrs);
    StatusWith<RecordId> res = rs->insertRecord(opCtx.get(), obj.objdata(), obj.objsize(), false);
    if (res.isOK())
        wuow.commit();
    return res;
}

// TODO make generic
TEST(KVDBRecordStoreTest, OplogHack) {
    KVDBRecordStoreHarnessHelper harnessHelper;
    // Use a large enough cappedMaxSize so that the limit is not reached by doing the inserts within
    // the test itself.
    const int64_t cappedMaxSize = 10 * 1024;  // 10KB
    unique_ptr<RecordStore> rs(
        harnessHelper.newCappedRecordStore("local.oplog.foo", cappedMaxSize, -1));
    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        // always illegal
        ASSERT_EQ(insertBSONTs(opCtx, rs, Timestamp(2, -1)).getStatus(), ErrorCodes::BadValue);

        {
            BSONObj obj = BSON("not_ts" << Timestamp(2, 1));
            ASSERT_EQ(
                rs->insertRecord(opCtx.get(), obj.objdata(), obj.objsize(), false).getStatus(),
                ErrorCodes::BadValue);

            obj = BSON("ts"
                       << "not a Timestamp");
            ASSERT_EQ(
                rs->insertRecord(opCtx.get(), obj.objdata(), obj.objsize(), false).getStatus(),
                ErrorCodes::BadValue);
        }

        // currently dasserts
        // ASSERT_EQ(insertBSON(opCtx, rs, BSON("ts" << Timestamp(-2,1))).getStatus(),
        // ErrorCodes::BadValue);

        // success cases
        ASSERT_EQ(insertBSONTs(opCtx, rs, Timestamp(1, 1)).getValue(), RecordId(1, 1));

        ASSERT_EQ(insertBSONTs(opCtx, rs, Timestamp(1, 2)).getValue(), RecordId(1, 2));

        ASSERT_EQ(insertBSONTs(opCtx, rs, Timestamp(2, 2)).getValue(), RecordId(2, 2));
    }

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());
        // find start
        ASSERT_EQ(rs->oplogStartHack(opCtx.get(), RecordId(0, 1)), RecordId());      // nothing <=
        ASSERT_EQ(rs->oplogStartHack(opCtx.get(), RecordId(2, 1)), RecordId(1, 2));  // between
        ASSERT_EQ(rs->oplogStartHack(opCtx.get(), RecordId(2, 2)), RecordId(2, 2));  // ==
        ASSERT_EQ(rs->oplogStartHack(opCtx.get(), RecordId(2, 3)), RecordId(2, 2));  // > highest
    }

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());
        rs->temp_cappedTruncateAfter(opCtx.get(), RecordId(2, 2), false);  // no-op
    }

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());
        ASSERT_EQ(rs->oplogStartHack(opCtx.get(), RecordId(2, 3)), RecordId(2, 2));
    }

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());
        rs->temp_cappedTruncateAfter(opCtx.get(), RecordId(1, 2), false);  // deletes 2,2
    }

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());
        ASSERT_EQ(rs->oplogStartHack(opCtx.get(), RecordId(2, 3)), RecordId(1, 2));
    }

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());
        rs->temp_cappedTruncateAfter(opCtx.get(), RecordId(1, 2), true);  // deletes 1,2
    }

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());
        ASSERT_EQ(rs->oplogStartHack(opCtx.get(), RecordId(2, 3)), RecordId(1, 1));
    }

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());
        WriteUnitOfWork wuow(opCtx.get());
        ASSERT_OK(rs->truncate(opCtx.get()));  // deletes 1,1 and leaves collection empty
        wuow.commit();
    }

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());
        ASSERT_EQ(rs->oplogStartHack(opCtx.get(), RecordId(2, 3)), RecordId());
    }
}

void testDeleteSeekExactRecordNonCapped(bool forward) {
    KVDBRecordStoreHarnessHelper harnessHelper;
    std::unique_ptr<RecordStore> rs;
    RecordId loc1{1};
    RecordId loc2{2};
    RecordId loc3{3};
    rs = harnessHelper.newNonCappedRecordStore("local.not_oplog.foo");

    ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

    ASSERT_EQ(insertBSON(opCtx, rs, RecordId(1, 1)).getValue(), loc1);

    ASSERT_EQ(insertBSON(opCtx, rs, RecordId(1, 2)).getValue(), loc2);

    ASSERT_EQ(insertBSON(opCtx, rs, RecordId(2, 2)).getValue(), loc3);


    auto cursor = rs->getCursor(opCtx.get(), forward);
    auto record = cursor->seekExact(loc2);
    ASSERT(record);
    cursor->save();
    rs->deleteRecord(opCtx.get(), loc2);
    cursor->restore();

    auto next = cursor->next();
    ASSERT(next);
    ASSERT_EQ(next->id, forward ? loc3 : loc1);
    ASSERT(!cursor->next());
}


void testDeleteSeekExactRecordCapped(bool forward) {
    KVDBRecordStoreHarnessHelper harnessHelper;
    std::unique_ptr<RecordStore> rs;
    RecordId loc1(1, 1);
    RecordId loc2(1, 2);
    RecordId loc3(2, 2);
    rs = harnessHelper.newCappedRecordStore("local.oplog.foo", 100000, -1);

    ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

    ASSERT_EQ(insertBSONTs(opCtx, rs, Timestamp(1, 1)).getValue(), loc1);

    ASSERT_EQ(insertBSONTs(opCtx, rs, Timestamp(1, 2)).getValue(), loc2);

    ASSERT_EQ(insertBSONTs(opCtx, rs, Timestamp(2, 2)).getValue(), loc3);

    auto cursor = rs->getCursor(opCtx.get(), forward);
    auto record = cursor->seekExact(loc2);
    ASSERT(record);
    cursor->save();
    rs->deleteRecord(opCtx.get(), loc2);
    cursor->restore();

    ASSERT(!cursor->next());
}


void testDeleteSeekExactRecord(bool forward, bool capped) {

    if (capped) {
        testDeleteSeekExactRecordCapped(forward);
    } else {
        testDeleteSeekExactRecordNonCapped(forward);
    }
}

TEST(KVDBRecordStoreTest, DeleteSeekExactRecord_Forward_NonCapped) {
    testDeleteSeekExactRecord(true, false);
}

TEST(KVDBRecordStoreTest, DeleteSeekExactRecord_Reversed_NonCapped) {
    testDeleteSeekExactRecord(false, false);
}

TEST(KVDBRecordStoreTest, OplogHackOnNonOplog) {
    KVDBRecordStoreHarnessHelper harnessHelper;
    std::unique_ptr<RecordStore> rs(harnessHelper.newNonCappedRecordStore("local.NOT_oplog.foo"));

    ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

    BSONObj obj = BSON("ts" << Timestamp(2, -1));
    {
        WriteUnitOfWork wuow(opCtx.get());
        ASSERT_OK(rs->insertRecord(opCtx.get(), obj.objdata(), obj.objsize(), false).getStatus());
        wuow.commit();
    }
    ASSERT_TRUE(rs->oplogStartHack(opCtx.get(), RecordId(0, 1)) == boost::none);
}

TEST(KVDBRecordStoreTest, CappedOrder) {
    std::unique_ptr<KVDBRecordStoreHarnessHelper> harnessHelper(new KVDBRecordStoreHarnessHelper());
    std::unique_ptr<RecordStore> rs(harnessHelper->newCappedRecordStore("a.b", 100000, 10000));

    RecordId loc1;

    {  // first insert a document
        ServiceContext::UniqueOperationContext opCtx(harnessHelper->newOperationContext());
        {
            WriteUnitOfWork uow(opCtx.get());
            StatusWith<RecordId> res = rs->insertRecord(opCtx.get(), "a", 2, false);
            ASSERT_OK(res.getStatus());
            loc1 = res.getValue();
            uow.commit();
        }
    }

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper->newOperationContext());
        auto cursor = rs->getCursor(opCtx.get());
        auto record = cursor->seekExact(loc1);
        ASSERT(record);
        ASSERT_EQ(loc1, record->id);
        ASSERT(!cursor->next());
    }

    {
        // now we insert 2 docs, but commit the 2nd one fiirst
        // we make sure we can't find the 2nd until the first is commited
        ServiceContext::UniqueOperationContext t1(harnessHelper->newOperationContext());
        std::unique_ptr<WriteUnitOfWork> w1(new WriteUnitOfWork(t1.get()));
        rs->insertRecord(t1.get(), "b", 2, false);
        // do not commit yet

        {  // create 2nd doc
            auto client2 = harnessHelper->serviceContext()->makeClient("c2");
            auto t2 = harnessHelper->newOperationContext(client2.get());
            {
                WriteUnitOfWork w2(t2.get());
                rs->insertRecord(t2.get(), "c", 2, false);
                w2.commit();
            }
        }

        {  // state should be the same
            auto client2 = harnessHelper->serviceContext()->makeClient("c2");
            auto opCtx = harnessHelper->newOperationContext(client2.get());
            auto cursor = rs->getCursor(opCtx.get());
            auto record = cursor->seekExact(loc1);
            ASSERT(record);
            ASSERT_EQ(loc1, record->id);
            ASSERT(!cursor->next());
        }

        w1->commit();
    }

    {  // now all 3 docs should be visible
        ServiceContext::UniqueOperationContext opCtx(harnessHelper->newOperationContext());
        auto cursor = rs->getCursor(opCtx.get());
        auto record = cursor->seekExact(loc1);
        ASSERT(record);
        ASSERT_EQ(loc1, record->id);
        ASSERT(cursor->next());
        ASSERT(cursor->next());
        ASSERT(!cursor->next());
    }
}

RecordId _oplogOrderInsertOplog(OperationContext* txn, std::unique_ptr<RecordStore>& rs, int inc) {
    Timestamp opTime = Timestamp(5, inc);
    KVDBRecordStore* rrs = dynamic_cast<KVDBRecordStore*>(rs.get());
    Status status = rrs->oplogDiskLocRegister(txn, opTime);
    ASSERT_OK(status);
    BSONObj obj = BSON("ts" << opTime);
    StatusWith<RecordId> res = rs->insertRecord(txn, obj.objdata(), obj.objsize(), false);
    ASSERT_OK(res.getStatus());
    return res.getValue();
}

TEST(KVDBRecordStoreTest, OplogOrder) {
    std::unique_ptr<KVDBRecordStoreHarnessHelper> harnessHelper(new KVDBRecordStoreHarnessHelper());
    std::unique_ptr<RecordStore> rs(
        harnessHelper->newCappedRecordStore("local.oplog.foo", 100000, -1));
    {
        const KVDBRecordStore* rrs = dynamic_cast<KVDBRecordStore*>(rs.get());
        ASSERT(rrs->isOplog());
    }

    RecordId loc1;

    {  // first insert a document
        ServiceContext::UniqueOperationContext opCtx(harnessHelper->newOperationContext());
        {
            WriteUnitOfWork uow(opCtx.get());
            loc1 = _oplogOrderInsertOplog(opCtx.get(), rs, 1);
            uow.commit();
        }
    }

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper->newOperationContext());
        auto cursor = rs->getCursor(opCtx.get());
        auto record = cursor->seekExact(loc1);
        ASSERT(record);
        ASSERT_EQ(loc1, record->id);
        ASSERT(!cursor->next());
    }

    {
        // now we insert 2 docs, but commit the 2nd one first.
        // we make sure we can't find the 2nd until the first is committed.
        ServiceContext::UniqueOperationContext earlyReader(harnessHelper->newOperationContext());
        auto earlyCursor = rs->getCursor(earlyReader.get());
        ASSERT_EQ(earlyCursor->seekExact(loc1)->id, loc1);
        earlyCursor->save();
        earlyReader->recoveryUnit()->abandonSnapshot();

        auto client1 = harnessHelper->serviceContext()->makeClient("c1");
        auto t1 = harnessHelper->newOperationContext(client1.get());
        WriteUnitOfWork w1(t1.get());
        _oplogOrderInsertOplog(t1.get(), rs, 20);
        // do not commit yet

        {  // create 2nd doc
            auto client2 = harnessHelper->serviceContext()->makeClient("c2");
            auto t2 = harnessHelper->newOperationContext(client2.get());
            {
                WriteUnitOfWork w2(t2.get());
                _oplogOrderInsertOplog(t2.get(), rs, 30);
                w2.commit();
            }
        }

        {  // Other operations should not be able to see 2nd doc until w1 commits.
            earlyCursor->restore();
            ASSERT(!earlyCursor->next());

            auto client2 = harnessHelper->serviceContext()->makeClient("c2");
            auto opCtx = harnessHelper->newOperationContext(client2.get());
            auto cursor = rs->getCursor(opCtx.get());
            auto record = cursor->seekExact(loc1);
            ASSERT_EQ(loc1, record->id);
            ASSERT(!cursor->next());
        }

        w1.commit();
    }

    rs->waitForAllEarlierOplogWritesToBeVisible(harnessHelper->newOperationContext().get());


    {  // now all 3 docs should be visible
        ServiceContext::UniqueOperationContext opCtx(harnessHelper->newOperationContext());
        auto cursor = rs->getCursor(opCtx.get());
        auto record = cursor->seekExact(loc1);
        ASSERT_EQ(loc1, record->id);
        ASSERT(cursor->next());
        ASSERT(cursor->next());
        ASSERT(!cursor->next());
    }

    // Rollback the last two oplog entries, then insert entries with older optimes and ensure that
    // the visibility rules aren't violated. See SERVER-21645
    {
        ServiceContext::UniqueOperationContext txn(harnessHelper->newOperationContext());
        rs->temp_cappedTruncateAfter(txn.get(), loc1, false);
    }

    {
        // Now we insert 2 docs with timestamps earlier than before, but commit the 2nd one first.
        // We make sure we can't find the 2nd until the first is commited.
        ServiceContext::UniqueOperationContext earlyReader(harnessHelper->newOperationContext());
        auto earlyCursor = rs->getCursor(earlyReader.get());
        ASSERT_EQ(earlyCursor->seekExact(loc1)->id, loc1);
        earlyCursor->save();
        earlyReader->recoveryUnit()->abandonSnapshot();

        auto client1 = harnessHelper->serviceContext()->makeClient("c1");
        auto t1 = harnessHelper->newOperationContext(client1.get());
        WriteUnitOfWork w1(t1.get());
        _oplogOrderInsertOplog(t1.get(), rs, 2);
        // do not commit yet

        {  // create 2nd doc
            auto client2 = harnessHelper->serviceContext()->makeClient("c2");
            auto t2 = harnessHelper->newOperationContext(client2.get());
            {
                WriteUnitOfWork w2(t2.get());
                _oplogOrderInsertOplog(t2.get(), rs, 3);
                w2.commit();
            }
        }

        {  // Other operations should not be able to see 2nd doc until w1 commits.
            ASSERT(earlyCursor->restore());
            ASSERT(!earlyCursor->next());

            auto client2 = harnessHelper->serviceContext()->makeClient("c2");
            auto opCtx = harnessHelper->newOperationContext(client2.get());
            auto cursor = rs->getCursor(opCtx.get());
            auto record = cursor->seekExact(loc1);
            ASSERT(record);
            ASSERT_EQ(loc1, record->id);
            ASSERT(!cursor->next());
        }

        w1.commit();
    }

    rs->waitForAllEarlierOplogWritesToBeVisible(harnessHelper->newOperationContext().get());

    {  // now all 3 docs should be visible
        ServiceContext::UniqueOperationContext opCtx(harnessHelper->newOperationContext());
        auto cursor = rs->getCursor(opCtx.get());
        auto record = cursor->seekExact(loc1);
        ASSERT(record);
        ASSERT_EQ(loc1, record->id);
        ASSERT(cursor->next());
        ASSERT(cursor->next());
        ASSERT(!cursor->next());
    }
}

BSONObj makeBSONObjWithSize(const Timestamp& opTime, int size, char fill = 'x') {
    BSONObj objTemplate = BSON("ts" << opTime << "str"
                                    << "");
    ASSERT_LTE(objTemplate.objsize(), size);
    std::string str(size - objTemplate.objsize(), fill);

    BSONObj obj = BSON("ts" << opTime << "str" << str);
    ASSERT_EQ(size, obj.objsize());

    return obj;
}

StatusWith<RecordId> insertBSONWithSize(OperationContext* opCtx,
                                        RecordStore* rs,
                                        const Timestamp& opTime,
                                        int size) {
    BSONObj obj = makeBSONObjWithSize(opTime, size);

    WriteUnitOfWork wuow(opCtx);
    KVDBRecordStore* wtrs = checked_cast<KVDBRecordStore*>(rs);
    invariantHse(wtrs);
    Status status = wtrs->oplogDiskLocRegister(opCtx, opTime);
    if (!status.isOK()) {
        return StatusWith<RecordId>(status);
    }
    StatusWith<RecordId> res = rs->insertRecord(opCtx, obj.objdata(), obj.objsize(), false);
    if (res.isOK()) {
        wuow.commit();
    }
    return res;
}


// oplog import  test
// initialize and oplog and try to import it back.
TEST(KVDBRecordStoreTest, OplogBlock_import1) {
    KVDBRecordStoreHarnessHelper harnessHelper;

    const int64_t cappedMaxSize = 50 * 10 * 100;  // 10KB
    int numRecs = 100 * 10;
    int sizePerRec = 50;

    // Create an oplog
    {

        unique_ptr<RecordStore> rs(
            harnessHelper.newCappedRecordStore("local.oplog.block", cappedMaxSize, -1));

        KVDBOplogStore* kvdbRs = static_cast<KVDBOplogStore*>(rs.get());
        KVDBOplogBlockManager* opBlkMgr = kvdbRs->getOpBlkMgr();

        opBlkMgr->setMinBytesPerBlock(100);
        opBlkMgr->setMaxBlocksToKeep(10U);

        {
            ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

            for (int i = 1; i <= numRecs; i++) {

                ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(0, i), sizePerRec),
                          RecordId(0, i));
            }

            ASSERT_EQ(500U, opBlkMgr->numBlocks());
            ASSERT_EQ(0, opBlkMgr->currentRecords());
            ASSERT_EQ(0, opBlkMgr->currentBytes());
        }

        {
            ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

            kvdbRs->reclaimOplog(opCtx.get());
        }
    }

    // import the oplog
    {

        unique_ptr<RecordStore> rs(
            harnessHelper.newCappedRecordStore("local.oplog.block", cappedMaxSize, -1));

        KVDBOplogStore* kvdbRs = static_cast<KVDBOplogStore*>(rs.get());
        KVDBOplogBlockManager* opBlkMgr = kvdbRs->getOpBlkMgr();
        ASSERT_EQ(10U, opBlkMgr->numBlocks());
        ASSERT_EQ(0, opBlkMgr->currentRecords());
        ASSERT_EQ(0, opBlkMgr->currentBytes());
    }
}

// TEST(KVDBRecordStoreTest, OplogBlock_import2) {
//     KVDBRecordStoreHarnessHelper harnessHelper;

//     const int64_t cappedMaxSize = 50 * 10 * 100;  // 10KB
//     int numRecs = 100 * 10;
//     int sizePerRec = 50;


//     // Create an oplog
//     {

//         unique_ptr<RecordStore> rs(
//             harnessHelper.newCappedRecordStore("local.oplog.block", cappedMaxSize, -1));

//         KVDBRecordStore* kvdbRs = static_cast<KVDBRecordStore*>(rs.get());
//         KVDBOplogBlockManager* opBlkMgr = kvdbRs->getOpBlkMgr();

//         opBlkMgr->setMinBytesPerBlock(100);
//         opBlkMgr->setMaxBlocksToKeep(10U);


//         {
//             ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

//             for (int i = 1; i <= numRecs; i++) {

//                 ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(0, i), sizePerRec),
//                           RecordId(0, i));
//             }

//             ASSERT_EQ(500U, opBlkMgr->numBlocks());
//             ASSERT_EQ(0, opBlkMgr->currentRecords());
//             ASSERT_EQ(0, opBlkMgr->currentBytes());
//         }

//         {
//             ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

//             kvdbRs->reclaimOplog(opCtx.get());
//         }

//         unique_ptr<RecordStore> rs2(
//             harnessHelper.newCappedRecordStore("local.oplog.block", cappedMaxSize, -1));

//         KVDBRecordStore* kvdbRs2 = static_cast<KVDBRecordStore*>(rs2.get());
//         KVDBOplogBlockManager* opBlkMgr2 = kvdbRs2->getOpBlkMgr();

//         ASSERT_EQ(10U, opBlkMgr2->numBlocks());
//         ASSERT_EQ(0, opBlkMgr2->currentRecords());
//         ASSERT_EQ(0, opBlkMgr2->currentBytes());
//     }
// }

// op log cursor test
// insert multiple records that span blocks and read them using a cursor.
TEST(KVDBRecordStoreTest, OplogBlock_cursorReadLarge) {
    KVDBRecordStoreHarnessHelper harnessHelper;

    const int64_t cappedMaxSize = 15 * 1024 * 10;  // 10KB
    unique_ptr<RecordStore> rs(
        harnessHelper.newCappedRecordStore("local.oplog.block", cappedMaxSize, -1));

    KVDBOplogStore* kvdbRs = static_cast<KVDBOplogStore*>(rs.get());
    // KVDBOplogBlockManager* opBlkMgr = kvdbRs->getOpBlkMgr();

    // opBlkMgr->setMinBytesPerBlock(26000000);
    int numRecs = 11;
    int sizePerRec = 15 * 1024;

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        for (int i = 1; i <= numRecs; i++) {

            ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(0, i), sizePerRec),
                      RecordId(0, i));
        }
    }

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        kvdbRs->reclaimOplog(opCtx.get());
    }

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());
        {
            auto cursor = rs->getCursor(opCtx.get(), true);

            for (int i = 2; i <= numRecs; i++) {
                auto item = cursor->next();
                ASSERT(item);
                ASSERT_EQUALS(item->id, RecordId(0, i));
                ASSERT_EQUALS(static_cast<int>(item->data.size()), sizePerRec);
            }
        }
    }

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());
        {
            auto cursor = rs->getCursor(opCtx.get(), false);

            auto item = cursor->seekExact(RecordId(0, 5));
            // assert(item);

            for (int i = 4; i >= 2; i--) {
                auto item = cursor->next();
                ASSERT(item);
                ASSERT_EQUALS(item->id, RecordId(0, i));
                ASSERT_EQUALS(static_cast<int>(item->data.size()), sizePerRec);
            }
        }
    }


    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());
        {
            auto cursor = rs->getCursor(opCtx.get(), false);

            for (int i = numRecs; i >= 2; i--) {
                auto item = cursor->next();
                ASSERT(item);
                ASSERT_EQUALS(item->id, RecordId(0, i));
                ASSERT_EQUALS(static_cast<int>(item->data.size()), sizePerRec);
            }
        }
    }
}

// insert multiple records that span blocks and run oplogStartHack
TEST(KVDBRecordStoreTest, OplogBlock_oploghack) {
    KVDBRecordStoreHarnessHelper harnessHelper;

    const int64_t cappedMaxSize = 10 * 1024;  // 10KB
    unique_ptr<RecordStore> rs(
        harnessHelper.newCappedRecordStore("local.oplog.block", cappedMaxSize, -1));

    KVDBOplogStore* kvdbRs = static_cast<KVDBOplogStore*>(rs.get());
    KVDBOplogBlockManager* opBlkMgr = kvdbRs->getOpBlkMgr();

    opBlkMgr->setMinBytesPerBlock(1000);

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 1), 400), RecordId(1, 1));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 2), 800), RecordId(1, 2));

        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 4), 200), RecordId(1, 4));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 5), 300), RecordId(1, 5));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 6), 350), RecordId(1, 6));

        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 7), 50), RecordId(1, 7));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 8), 100), RecordId(1, 8));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 9), 150), RecordId(1, 9));
    }


    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        ASSERT_EQ(rs->oplogStartHack(opCtx.get(), RecordId(0, 1)), RecordId());  // nothing <=
        ASSERT_EQ(rs->oplogStartHack(opCtx.get(), RecordId(1, 2)), RecordId(1, 2));
        ASSERT_EQ(rs->oplogStartHack(opCtx.get(), RecordId(1, 4)), RecordId(1, 4));
        ASSERT_EQ(rs->oplogStartHack(opCtx.get(), RecordId(1, 3)), RecordId(1, 2));
        ASSERT_EQ(rs->oplogStartHack(opCtx.get(), RecordId(1, 10)), RecordId(1, 9));
    }
}

// op log cursor test
// insert multiple records that span blocks and read them using a cursor.
TEST(KVDBRecordStoreTest, OplogBlock_cursorRead) {
    KVDBRecordStoreHarnessHelper harnessHelper;

    const int64_t cappedMaxSize = 10 * 1024;  // 10KB
    unique_ptr<RecordStore> rs(
        harnessHelper.newCappedRecordStore("local.oplog.block", cappedMaxSize, -1));

    KVDBOplogStore* kvdbRs = static_cast<KVDBOplogStore*>(rs.get());
    KVDBOplogBlockManager* opBlkMgr = kvdbRs->getOpBlkMgr();

    opBlkMgr->setMinBytesPerBlock(1000);

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 1), 400), RecordId(1, 1));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 2), 800), RecordId(1, 2));

        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 3), 200), RecordId(1, 3));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 4), 250), RecordId(1, 4));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 5), 300), RecordId(1, 5));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 6), 350), RecordId(1, 6));

        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 7), 50), RecordId(1, 7));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 8), 100), RecordId(1, 8));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 9), 150), RecordId(1, 9));
    }


    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        {
            auto cursor = rs->getCursor(opCtx.get(), true);

            auto item = cursor->next();
            ASSERT(item);
            ASSERT_EQUALS(item->id, RecordId(1, 1));
            ASSERT_EQUALS(static_cast<int>(item->data.size()), 400);

            item = cursor->next();
            ASSERT(item);
            ASSERT_EQUALS(item->id, RecordId(1, 2));
            ASSERT_EQUALS(static_cast<int>(item->data.size()), 800);

            item = cursor->next();
            ASSERT(item);
            ASSERT_EQUALS(item->id, RecordId(1, 3));
            ASSERT_EQUALS(static_cast<int>(item->data.size()), 200);

            item = cursor->next();
            ASSERT(item);
            ASSERT_EQUALS(item->id, RecordId(1, 4));
            ASSERT_EQUALS(static_cast<int>(item->data.size()), 250);

            item = cursor->next();
            ASSERT(item);
            ASSERT_EQUALS(item->id, RecordId(1, 5));
            ASSERT_EQUALS(static_cast<int>(item->data.size()), 300);

            item = cursor->next();
            ASSERT(item);
            ASSERT_EQUALS(item->id, RecordId(1, 6));
            ASSERT_EQUALS(static_cast<int>(item->data.size()), 350);

            item = cursor->next();
            ASSERT(item);
            ASSERT_EQUALS(item->id, RecordId(1, 7));
            ASSERT_EQUALS(static_cast<int>(item->data.size()), 50);

            item = cursor->next();
            ASSERT(item);
            ASSERT_EQUALS(item->id, RecordId(1, 8));
            ASSERT_EQUALS(static_cast<int>(item->data.size()), 100);

            item = cursor->next();
            ASSERT(item);
            ASSERT_EQUALS(item->id, RecordId(1, 9));
            ASSERT_EQUALS(static_cast<int>(item->data.size()), 150);

            ASSERT(!cursor->next());

            // ASSERT_EQUALS(item->data.data(), strings[i]);
        }
    }
}

// Insert records into an oplog and verify the number of blocks that are created.
TEST(KVDBRecordStoreTest, OplogBlock_CreateNewBlock) {
    KVDBRecordStoreHarnessHelper harnessHelper;

    const int64_t cappedMaxSize = 10 * 1024;  // 10KB
    unique_ptr<RecordStore> rs(
        harnessHelper.newCappedRecordStore("local.oplog.blocks", cappedMaxSize, -1));

    KVDBOplogStore* kvdbRs = static_cast<KVDBOplogStore*>(rs.get());
    KVDBOplogBlockManager* opBlkMgr = kvdbRs->getOpBlkMgr();

    opBlkMgr->setMinBytesPerBlock(100);

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        ASSERT_EQ(0U, opBlkMgr->numBlocks());

        // Inserting a record smaller than 'minBytesPerBlock' shouldn't create a new oplog
        // block.
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 1), 99), RecordId(1, 1));
        ASSERT_EQ(0U, opBlkMgr->numBlocks());
        ASSERT_EQ(1, opBlkMgr->currentRecords());
        ASSERT_EQ(99, opBlkMgr->currentBytes());

        // Inserting another record such that their combined size exceeds 'minBytesPerBlock'
        // should
        // cause a new block to be created.
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 2), 51), RecordId(1, 2));
        ASSERT_EQ(1U, opBlkMgr->numBlocks());
        ASSERT_EQ(0, opBlkMgr->currentRecords());
        ASSERT_EQ(0, opBlkMgr->currentBytes());

        // Inserting a record such that the combined size of this record and the previously
        // inserted
        // one exceed 'minBytesPerBlock' shouldn't cause a new block to be created because we've
        // started filling a new block.
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 3), 50), RecordId(1, 3));
        ASSERT_EQ(1U, opBlkMgr->numBlocks());
        ASSERT_EQ(1, opBlkMgr->currentRecords());
        ASSERT_EQ(50, opBlkMgr->currentBytes());

        // Inserting a record such that the combined size of this record and the previously
        // inserted
        // one is exactly equal to 'minBytesPerBlock' should cause a new block to be created.
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 4), 50), RecordId(1, 4));
        ASSERT_EQ(2U, opBlkMgr->numBlocks());
        ASSERT_EQ(0, opBlkMgr->currentRecords());
        ASSERT_EQ(0, opBlkMgr->currentBytes());

        // Inserting a single record that exceeds 'minBytesPerBlock' should cause a new block to
        // be created.
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 5), 101), RecordId(1, 5));
        ASSERT_EQ(3U, opBlkMgr->numBlocks());
        ASSERT_EQ(0, opBlkMgr->currentRecords());
        ASSERT_EQ(0, opBlkMgr->currentBytes());
    }
}

// Insert records into an oplog and try to update them. The updates shouldn't succeed if the
// size of
// record is changed.
TEST(KVDBRecordStoreTest, OplogBlock_UpdateRecord) {
    KVDBRecordStoreHarnessHelper harnessHelper;

    const int64_t cappedMaxSize = 10 * 1024;  // 10KB
    unique_ptr<RecordStore> rs(
        harnessHelper.newCappedRecordStore("local.oplog.blocks", cappedMaxSize, -1));

    KVDBOplogStore* kvdbRs = static_cast<KVDBOplogStore*>(rs.get());
    KVDBOplogBlockManager* opBlkMgr = kvdbRs->getOpBlkMgr();

    opBlkMgr->setMinBytesPerBlock(100);

    // Insert two records such that one makes up a full block and the other is a part of the
    // block
    // currently being filled.
    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 1), 100), RecordId(1, 1));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 2), 50), RecordId(1, 2));

        ASSERT_EQ(1U, opBlkMgr->numBlocks());
        ASSERT_EQ(1, opBlkMgr->currentRecords());
        ASSERT_EQ(50, opBlkMgr->currentBytes());
    }

    // Attempts to grow the records should fail.
    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        BSONObj changed1 = makeBSONObjWithSize(Timestamp(1, 1), 101);
        BSONObj changed2 = makeBSONObjWithSize(Timestamp(1, 2), 51);

        WriteUnitOfWork wuow(opCtx.get());
        ASSERT_NOT_OK(rs->updateRecord(
            opCtx.get(), RecordId(1, 1), changed1.objdata(), changed1.objsize(), false, nullptr));
        ASSERT_NOT_OK(rs->updateRecord(
            opCtx.get(), RecordId(1, 2), changed2.objdata(), changed2.objsize(), false, nullptr));
    }

    // Attempts to shrink the records should also fail.
    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        BSONObj changed1 = makeBSONObjWithSize(Timestamp(1, 1), 99);
        BSONObj changed2 = makeBSONObjWithSize(Timestamp(1, 2), 49);

        WriteUnitOfWork wuow(opCtx.get());
        ASSERT_NOT_OK(rs->updateRecord(
            opCtx.get(), RecordId(1, 1), changed1.objdata(), changed1.objsize(), false, nullptr));
        ASSERT_NOT_OK(rs->updateRecord(
            opCtx.get(), RecordId(1, 2), changed2.objdata(), changed2.objsize(), false, nullptr));
    }

    // Changing the contents of the records without changing their size should succeed.
    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        BSONObj changed1 = makeBSONObjWithSize(Timestamp(1, 1), 100, 'y');
        BSONObj changed2 = makeBSONObjWithSize(Timestamp(1, 2), 50, 'z');

        WriteUnitOfWork wuow(opCtx.get());
        ASSERT_OK(rs->updateRecord(
            opCtx.get(), RecordId(1, 1), changed1.objdata(), changed1.objsize(), false, nullptr));
        ASSERT_OK(rs->updateRecord(
            opCtx.get(), RecordId(1, 2), changed2.objdata(), changed2.objsize(), false, nullptr));
        wuow.commit();

        ASSERT_EQ(1U, opBlkMgr->numBlocks());
        ASSERT_EQ(1, opBlkMgr->currentRecords());
        ASSERT_EQ(50, opBlkMgr->currentBytes());
    }
}

// Insert multiple records and truncate the oplog using RecordStore::truncate(). The operation
// should leave no blocks, including the partially filled one.
TEST(KVDBRecordStoreTest, OplogBlock_Truncate) {
    KVDBRecordStoreHarnessHelper harnessHelper;

    const int64_t cappedMaxSize = 10 * 1024;  // 10KB
    unique_ptr<RecordStore> rs(
        harnessHelper.newCappedRecordStore("local.oplog.blocks", cappedMaxSize, -1));

    KVDBOplogStore* kvdbRs = static_cast<KVDBOplogStore*>(rs.get());
    KVDBOplogBlockManager* opBlkMgr = kvdbRs->getOpBlkMgr();

    opBlkMgr->setMinBytesPerBlock(100);

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 1), 50), RecordId(1, 1));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 2), 50), RecordId(1, 2));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 3), 50), RecordId(1, 3));

        ASSERT_EQ(1U, opBlkMgr->numBlocks());
        ASSERT_EQ(1, opBlkMgr->currentRecords());
        ASSERT_EQ(50, opBlkMgr->currentBytes());
    }

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        ASSERT_EQ(3, rs->numRecords(opCtx.get()));
        ASSERT_EQ(150, rs->dataSize(opCtx.get()));

        WriteUnitOfWork wuow(opCtx.get());
        ASSERT_OK(rs->truncate(opCtx.get()));
        wuow.commit();

        ASSERT_EQ(0, rs->dataSize(opCtx.get()));
        ASSERT_EQ(0, rs->numRecords(opCtx.get()));
        ASSERT_EQ(0U, opBlkMgr->numBlocks());
        ASSERT_EQ(0, opBlkMgr->currentRecords());
        ASSERT_EQ(0, opBlkMgr->currentBytes());
    }
}

// Insert multiple records, truncate the oplog using RecordStore::temp_cappedTruncateAfter(),
// and
// verify that the metadata for each block is updated. If a full block is partially truncated,
// then
// it should become the block currently being filled.
TEST(KVDBRecordStoreTest, OplogBlocks_CappedTruncateAfter) {
    KVDBRecordStoreHarnessHelper harnessHelper;

    const int64_t cappedMaxSize = 10 * 1024;  // 10KB
    unique_ptr<RecordStore> rs(
        harnessHelper.newCappedRecordStore("local.oplog.blocks", cappedMaxSize, -1));

    KVDBOplogStore* kvdbRs = static_cast<KVDBOplogStore*>(rs.get());
    KVDBOplogBlockManager* opBlkMgr = kvdbRs->getOpBlkMgr();

    opBlkMgr->setMinBytesPerBlock(1000);

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 1), 400), RecordId(1, 1));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 2), 800), RecordId(1, 2));

        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 3), 200), RecordId(1, 3));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 4), 250), RecordId(1, 4));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 5), 300), RecordId(1, 5));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 6), 350), RecordId(1, 6));

        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 7), 50), RecordId(1, 7));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 8), 100), RecordId(1, 8));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 9), 150), RecordId(1, 9));

        ASSERT_EQ(9, rs->numRecords(opCtx.get()));
        ASSERT_EQ(2600, rs->dataSize(opCtx.get()));
        ASSERT_EQ(2U, opBlkMgr->numBlocks());
        ASSERT_EQ(3, opBlkMgr->currentRecords());
        ASSERT_EQ(300, opBlkMgr->currentBytes());
    }

    // Truncate data using an inclusive RecordId that exists inside the block currently being
    // filled.
    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        rs->temp_cappedTruncateAfter(opCtx.get(), RecordId(1, 8), true);

        ASSERT_EQ(7, rs->numRecords(opCtx.get()));
        ASSERT_EQ(2350, rs->dataSize(opCtx.get()));
        ASSERT_EQ(2U, opBlkMgr->numBlocks());
        ASSERT_EQ(1, opBlkMgr->currentRecords());
        ASSERT_EQ(50, opBlkMgr->currentBytes());
    }

    // Truncate data using an inclusive RecordId that refers to the 'lastRecord' of a full
    // block.
    // The block should become the one currently being filled.
    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        rs->temp_cappedTruncateAfter(opCtx.get(), RecordId(1, 6), true);

        ASSERT_EQ(5, rs->numRecords(opCtx.get()));
        ASSERT_EQ(1950, rs->dataSize(opCtx.get()));
        ASSERT_EQ(1U, opBlkMgr->numBlocks());
        ASSERT_EQ(3, opBlkMgr->currentRecords());
        ASSERT_EQ(750, opBlkMgr->currentBytes());
    }

    // Truncate data using a non-inclusive RecordId that exists inside the block currently being
    // filled.
    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        rs->temp_cappedTruncateAfter(opCtx.get(), RecordId(1, 3), false);

        ASSERT_EQ(3, rs->numRecords(opCtx.get()));
        ASSERT_EQ(1400, rs->dataSize(opCtx.get()));
        ASSERT_EQ(1U, opBlkMgr->numBlocks());
        ASSERT_EQ(1, opBlkMgr->currentRecords());
        ASSERT_EQ(200, opBlkMgr->currentBytes());
    }

    // Truncate data using a non-inclusive RecordId that refers to the 'lastRecord' of a full
    // block.
    // The block should remain intact.
    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        rs->temp_cappedTruncateAfter(opCtx.get(), RecordId(1, 2), false);

        ASSERT_EQ(2, rs->numRecords(opCtx.get()));
        ASSERT_EQ(1200, rs->dataSize(opCtx.get()));
        ASSERT_EQ(1U, opBlkMgr->numBlocks());
        ASSERT_EQ(0, opBlkMgr->currentRecords());
        ASSERT_EQ(0, opBlkMgr->currentBytes());
    }

    // Truncate data using a non-inclusive RecordId that exists inside a full block. The block
    // should become the one currently being filled.
    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        rs->temp_cappedTruncateAfter(opCtx.get(), RecordId(1, 1), false);

        ASSERT_EQ(1, rs->numRecords(opCtx.get()));
        ASSERT_EQ(400, rs->dataSize(opCtx.get()));
        ASSERT_EQ(0U, opBlkMgr->numBlocks());
        ASSERT_EQ(1, opBlkMgr->currentRecords());
        ASSERT_EQ(400, opBlkMgr->currentBytes());
    }
}
// Verify that oplog blocks are reclaimed when the number of blocks to keep is exceeded.
TEST(KVDBRecordStoreTest, OplogBlock_ReclaimBlocks) {
    KVDBRecordStoreHarnessHelper harnessHelper;

    const int64_t cappedMaxSize = 10 * 1024;  // 10KB
    unique_ptr<RecordStore> rs(
        harnessHelper.newCappedRecordStore("local.oplog.blocks", cappedMaxSize, -1));

    KVDBOplogStore* kvdbRs = static_cast<KVDBOplogStore*>(rs.get());
    KVDBOplogBlockManager* opBlkMgr = kvdbRs->getOpBlkMgr();

    opBlkMgr->setMinBytesPerBlock(100);
    opBlkMgr->setMaxBlocksToKeep(2U);

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 1), 100), RecordId(1, 1));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 2), 110), RecordId(1, 2));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 3), 120), RecordId(1, 3));

        ASSERT_EQ(3, rs->numRecords(opCtx.get()));
        ASSERT_EQ(330, rs->dataSize(opCtx.get()));
        ASSERT_EQ(3U, opBlkMgr->numBlocks());
        ASSERT_EQ(0, opBlkMgr->currentRecords());
        ASSERT_EQ(0, opBlkMgr->currentBytes());
    }

    // Truncate a block when number of blocks to keep is exceeded.
    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        kvdbRs->reclaimOplog(opCtx.get());

        ASSERT_EQ(2, rs->numRecords(opCtx.get()));
        ASSERT_EQ(230, rs->dataSize(opCtx.get()));
        ASSERT_EQ(2U, opBlkMgr->numBlocks());
        ASSERT_EQ(0, opBlkMgr->currentRecords());
        ASSERT_EQ(0, opBlkMgr->currentBytes());
    }

    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 4), 130), RecordId(1, 4));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 5), 140), RecordId(1, 5));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 6), 50), RecordId(1, 6));

        ASSERT_EQ(5, rs->numRecords(opCtx.get()));
        ASSERT_EQ(550, rs->dataSize(opCtx.get()));
        ASSERT_EQ(4U, opBlkMgr->numBlocks());
        ASSERT_EQ(1, opBlkMgr->currentRecords());
        ASSERT_EQ(50, opBlkMgr->currentBytes());
    }

    // Truncate multiple blocks if necessary.
    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        kvdbRs->reclaimOplog(opCtx.get());

        ASSERT_EQ(3, rs->numRecords(opCtx.get()));
        ASSERT_EQ(320, rs->dataSize(opCtx.get()));
        ASSERT_EQ(2U, opBlkMgr->numBlocks());
        ASSERT_EQ(1, opBlkMgr->currentRecords());
        ASSERT_EQ(50, opBlkMgr->currentBytes());
    }

    // No-op if the number of oplog blocks is less than or equal to the number of blocks to
    // keep.
    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        kvdbRs->reclaimOplog(opCtx.get());

        ASSERT_EQ(3, rs->numRecords(opCtx.get()));
        ASSERT_EQ(320, rs->dataSize(opCtx.get()));
        ASSERT_EQ(2U, opBlkMgr->numBlocks());
        ASSERT_EQ(1, opBlkMgr->currentRecords());
        ASSERT_EQ(50, opBlkMgr->currentBytes());
    }
}

// Verify that oplog blocks are not reclaimed even if the size of the record store exceeds
// 'cappedMaxSize'.
TEST(KVDBRecordStoreTest, OplogBlock_ExceedCappedMaxSize) {
    KVDBRecordStoreHarnessHelper harnessHelper;

    const int64_t cappedMaxSize = 256;
    unique_ptr<RecordStore> rs(
        harnessHelper.newCappedRecordStore("local.oplog.blocks", cappedMaxSize, -1));

    KVDBOplogStore* kvdbRs = static_cast<KVDBOplogStore*>(rs.get());
    KVDBOplogBlockManager* opBlkMgr = kvdbRs->getOpBlkMgr();

    opBlkMgr->setMinBytesPerBlock(100);
    opBlkMgr->setMaxBlocksToKeep(10U);
    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 1), 100), RecordId(1, 1));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 2), 110), RecordId(1, 2));
        ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 3), 120), RecordId(1, 3));

        ASSERT_EQ(3, rs->numRecords(opCtx.get()));
        ASSERT_EQ(330, rs->dataSize(opCtx.get()));
        ASSERT_EQ(3U, opBlkMgr->numBlocks());
        ASSERT_EQ(0, opBlkMgr->currentRecords());
        ASSERT_EQ(0, opBlkMgr->currentBytes());
    }

    // Shouldn't truncate a block when the number of oplog blocks is less than the number of
    // blocks
    // to keep, even though the size of the record store exceeds 'cappedMaxSize'.
    {
        ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

        kvdbRs->reclaimOplog(opCtx.get());

        ASSERT_EQ(3, rs->numRecords(opCtx.get()));
        ASSERT_EQ(330, rs->dataSize(opCtx.get()));
        ASSERT_EQ(3U, opBlkMgr->numBlocks());
        ASSERT_EQ(0, opBlkMgr->currentRecords());
        ASSERT_EQ(0, opBlkMgr->currentBytes());
    }
}

TEST(KVDBRecordStoreTest, leb128) {
    uint8_t buf[10];
    uint32_t leb128_bytes;
    uint64_t val;
    int i;
    hse::Status hseSt;

    for (i = 0; i < 127; i++) {
        val = i;
        hseSt = hse::encodeLeb128(val, buf, 10, &leb128_bytes);
        ASSERT_EQ(true, hseSt.ok());
        ASSERT_EQ(1, static_cast<int>(leb128_bytes));

        hseSt = hse::decodeLeb128(buf, 10, &val, &leb128_bytes);
        ASSERT_EQ(true, hseSt.ok());
        ASSERT_EQ(1, static_cast<int>(leb128_bytes));
        ASSERT_EQ(i, static_cast<int>(val));
    }

    for (val = 1 << (8 - 1), i = 2; i < 8; i++, val <<= 8) {
        uint64_t val1 = val;

        hseSt = hse::encodeLeb128(val1, buf, 10, &leb128_bytes);
        ASSERT_EQ(true, hseSt.ok());
        ASSERT_EQ(i, static_cast<int>(leb128_bytes));
        hseSt = hse::decodeLeb128(buf, 10, &val1, &leb128_bytes);
        ASSERT_EQ(true, hseSt.ok());
        ASSERT_EQ(i, static_cast<int>(leb128_bytes));
        ASSERT_EQ(val1, val);

        val1 = val - 1;
        hseSt = hse::encodeLeb128(val1, buf, 10, &leb128_bytes);
        ASSERT_EQ(true, hseSt.ok());
        ASSERT_EQ(i == 2 ? i - 1 : i, static_cast<int>(leb128_bytes));
        hseSt = hse::decodeLeb128(buf, 10, &val1, &leb128_bytes);
        ASSERT_EQ(true, hseSt.ok());
        ASSERT_EQ(i == 2 ? i - 1 : i, static_cast<int>(leb128_bytes));
        ASSERT_EQ(val1, val - 1);
    }
}

TEST(KVDBRecordStoreTest, compress) {
    struct CompParms compparms = {};
    KVDBData comp = {};
    KVDBData unc = {};
#define BUF_SIZE 1024
    uint32_t buf[BUF_SIZE];
    hse::Status hseSt;
    uint32_t i;
    uint32_t* ptr;
    void* unc_buf;
    size_t unc_len;

    compparms.compalgo = ALGO_NONE;
    hseSt = hse::compressdata(compparms, (const char*)buf, sizeof(buf), comp);
    ASSERT_EQ(false, hseSt.ok());

    for (i = 0; i < BUF_SIZE; i++)
        buf[i] = i;

    compparms.compalgo = ALGO_LZ4;
    hseSt = hse::compressdata(compparms, (const char*)buf, sizeof(buf), comp);
    ASSERT_EQ(true, hseSt.ok());

    hseSt = hse::decompressdata(compparms, comp, 0, unc);
    ASSERT_EQ(true, hseSt.ok());
    ptr = (uint32_t*)unc.data();
    for (i = 0; i < BUF_SIZE; i++, ptr++)
        ASSERT_EQ(i, *ptr);


    compparms.compminsize = sizeof(buf);
    hseSt = hse::compressdata(compparms, (const char*)buf, sizeof(buf), comp);
    ASSERT_EQ(true, hseSt.ok());

    hseSt = hse::decompressdata(compparms, comp, 0, unc);
    ASSERT_EQ(true, hseSt.ok());
    ptr = (uint32_t*)unc.data();
    for (i = 0; i < BUF_SIZE; i++, ptr++)
        ASSERT_EQ(i, *ptr);


    compparms.compminsize = 0;
    hseSt = hse::compressdata(compparms, (const char*)buf, sizeof(buf), comp);
    ASSERT_EQ(true, hseSt.ok());

    hseSt = hse::decompressdata1(compparms, comp.data(), comp.len(), 0, &unc_buf, &unc_len);
    ASSERT_EQ(true, hseSt.ok());
    ptr = (uint32_t*)unc_buf;
    for (i = 0; i < BUF_SIZE; i++, ptr++)
        ASSERT_EQ(i, *ptr);


    compparms.compminsize = sizeof(buf);
    hseSt = hse::compressdata(compparms, (const char*)buf, sizeof(buf), comp);
    ASSERT_EQ(true, hseSt.ok());

    hseSt = hse::decompressdata1(compparms, comp.data(), comp.len(), 0, &unc_buf, &unc_len);
    ASSERT_EQ(true, hseSt.ok());
    ptr = (uint32_t*)unc_buf;
    for (i = 0; i < BUF_SIZE; i++, ptr++)
        ASSERT_EQ(i, *ptr);
}

// Verify that an oplog block isn't created if it would cause the logical representation of the
// records to not be in increasing order.
// TEST(KVDBRecordStoreTest, OplogBlock_AscendingOrder) {
//     KVDBRecordStoreHarnessHelper harnessHelper;

//     const int64_t cappedMaxSize = 10 * 1024;  // 10KB
//     unique_ptr<RecordStore> rs(
//         harnessHelper.newCappedRecordStore("local.oplog.blocks", cappedMaxSize, -1));

//     KVDBRecordStore* kvdbRs = static_cast<KVDBRecordStore*>(rs.get());
//     KVDBOplogBlockManager* opBlkMgr = kvdbRs->getOpBlkMgr();

//     opBlkMgr->setMinBytesPerBlock(100);
//     {
//         ServiceContext::UniqueOperationContext opCtx(harnessHelper.newOperationContext());

//         ASSERT_EQ(0U, opBlkMgr->numBlocks());
//         ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(2, 2), 50), RecordId(2,
//         2));
//         ASSERT_EQ(0U, opBlkMgr->numBlocks());
//         ASSERT_EQ(1, opBlkMgr->currentRecords());
//         ASSERT_EQ(50, opBlkMgr->currentBytes());

//         // Inserting a record that has a smaller RecordId than the previously inserted record
//         // should
//         // be able to create a new block when no blocks already exist.
//         ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(2, 1), 50), RecordId(2,
//         1));
//         ASSERT_EQ(1U, opBlkMgr->numBlocks());
//         ASSERT_EQ(0, opBlkMgr->currentRecords());
//         ASSERT_EQ(0, opBlkMgr->currentBytes());

//         // However, inserting a record that has a smaller RecordId than most recently created
//         // block's last record shouldn't cause a new block to be created, even if the size of
//         // the
//         // inserted record exceeds 'minBytesPerBlock'.
//         ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(1, 1), 100), RecordId(1,
//         1));
//         ASSERT_EQ(1U, opBlkMgr->numBlocks());
//         ASSERT_EQ(0, opBlkMgr->currentRecords());
//         ASSERT_EQ(0, opBlkMgr->currentBytes());

//         // Inserting a record that has a larger RecordId than the most recently created block's
//         // last
//         // record should then cause a new block to be created.
//         ASSERT_EQ(insertBSONWithSize(opCtx.get(), rs.get(), Timestamp(2, 3), 100), RecordId(2,
//         3));
//         ASSERT_EQ(2U, opBlkMgr->numBlocks());
//         ASSERT_EQ(0, opBlkMgr->currentRecords());
//         ASSERT_EQ(0, opBlkMgr->currentBytes());
//     }
// }
}
