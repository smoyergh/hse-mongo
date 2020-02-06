/**
 * TODO: HSE License
 */
#include "mongo/platform/basic.h"

#include <boost/filesystem/operations.hpp>
#include <string>

#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/storage/sorted_data_interface_test_harness.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"

#include "hse_counter_manager.h"
#include "hse_durability_manager.h"
#include "hse_impl.h"
#include "hse_index.h"
#include "hse_recovery_unit.h"
#include "hse_ut_common.h"

using hse::KVSHandle;
using hse::KVDB_prefix;

namespace mongo {

using std::string;

class HSEKVDBIndexHarness final : public HarnessHelper {
public:
    HSEKVDBIndexHarness() : _order(Ordering::make(BSONObj())) {
        setupDb();
        _counterManager.reset(new KVDBCounterManager(true));

        _prefix = string{"\0\0\0\1", 4};
        _ident = "IDXIdent";
    }

    ~HSEKVDBIndexHarness() {
        teardownDb();
    }

    void setupDb() {
        hse_params_create(&_colKvsCParams);
        hse_params_create(&_uniqIdxKvsCParams);
        hse_params_create(&_stdIdxKvsCParams);
        hse_params_create(&_largeKvsCParams);

        hse::Status hseSt = _db.kvdb_kvs_make(_colKvsName.c_str(), nullptr);
        invariantHseSt(hseSt);

        hseSt = _db.kvdb_kvs_open(_colKvsName.c_str(), nullptr, _colKvs);
        invariantHseSt(hseSt);

        hse_params_set(
            _uniqIdxKvsCParams, "kvs.cparams.sfxlen", std::to_string(hse::DEFAULT_SFX_LEN).c_str());
        hseSt = _db.kvdb_kvs_make(_uniqIdxKvsName.c_str(), _uniqIdxKvsCParams);
        invariantHseSt(hseSt);

        hseSt = _db.kvdb_kvs_open(_uniqIdxKvsName.c_str(), nullptr, _uniqIdxKvs);
        invariantHseSt(hseSt);

        hse_params_set(
            _stdIdxKvsCParams, "kvs.cparams.sfxlen", std::to_string(hse::STDIDX_SFX_LEN).c_str());
        hseSt = _db.kvdb_kvs_make(_stdIdxKvsName.c_str(), _stdIdxKvsCParams);
        invariantHseSt(hseSt);

        hseSt = _db.kvdb_kvs_open(_stdIdxKvsName.c_str(), nullptr, _stdIdxKvs);
        invariantHseSt(hseSt);

        hseSt = _db.kvdb_kvs_make(_largeKvsName.c_str(), _largeKvsCParams);
        invariantHseSt(hseSt);

        hseSt = _db.kvdb_kvs_open(_largeKvsName.c_str(), nullptr, _largeKvs);
        invariantHseSt(hseSt);

        hse_params_destroy(_colKvsCParams);
        hse_params_destroy(_uniqIdxKvsCParams);
        hse_params_destroy(_stdIdxKvsCParams);
        hse_params_destroy(_largeKvsCParams);
    }

    void teardownDb() {
        auto hseSt = _db.kvdb_kvs_close(_colKvs);
        invariantHseSt(hseSt);

        hseSt = _db.kvdb_kvs_close(_stdIdxKvs);
        invariantHseSt(hseSt);

        hseSt = _db.kvdb_kvs_close(_uniqIdxKvs);
        invariantHseSt(hseSt);

        hseSt = _db.kvdb_kvs_close(_largeKvs);
        invariantHseSt(hseSt);

        // drops all kvses + drops kvdb if requested via KVDB_PER_UT env
        _dbFixture.reset();
    }

    std::unique_ptr<SortedDataInterface> newSortedDataInterface(bool unique) {
        // [MU_REVISIT] Passes 0 for numFields, so doesn't exercise point gets.
        // Changing the code to not verify numFields in _seek indicates that the tests
        // still pass. Need to fix this to automatically test the point get path.
        BSONObjBuilder configBuilder;
        if (unique) {
            return stdx::make_unique<KVDBUniqIdx>(_db,
                                                  _uniqIdxKvs,
                                                  *_counterManager.get(),
                                                  _prefix,
                                                  _ident,
                                                  _order,
                                                  configBuilder.obj(),
                                                  false,
                                                  0,
                                                  KVDB_prefix + "indexsize-" + _ident);
        } else {
            return stdx::make_unique<KVDBStdIdx>(_db,
                                                 _stdIdxKvs,
                                                 *_counterManager.get(),
                                                 _prefix,
                                                 _ident,
                                                 _order,
                                                 configBuilder.obj(),
                                                 0,
                                                 KVDB_prefix + "indexsize-" + _ident);
        }
    }

    std::unique_ptr<RecoveryUnit> newRecoveryUnit() {
        // return stdx::make_unique<KVDBRecoveryUnit>(_db.get(), _counterManager.get(), nullptr,
        // _durabilityManager.get(), true);
        return stdx::make_unique<KVDBRecoveryUnit>(
            _db, *_counterManager.get(), *_durabilityManager.get());
    }

private:
    Ordering _order;

    string _colKvsName = "ColKvs";
    struct hse_params* _colKvsCParams;
    KVSHandle _colKvs;

    string _uniqIdxKvsName = "UniqIdxKvs";
    struct hse_params* _uniqIdxKvsCParams;
    KVSHandle _uniqIdxKvs;

    string _stdIdxKvsName = "StdIdxKvs";
    struct hse_params* _stdIdxKvsCParams;
    KVSHandle _stdIdxKvs;

    string _largeKvsName = "LargeKVS";
    struct hse_params* _largeKvsCParams;
    KVSHandle _largeKvs;

    hse::KVDBTestSuiteFixture& _dbFixture = KVDBTestSuiteFixture::getFixture();
    hse::KVDB& _db = _dbFixture.getDb();
    std::unique_ptr<KVDBDurabilityManager> _durabilityManager;
    std::unique_ptr<KVDBCounterManager> _counterManager;
    string _prefix;
    string _ident;
};

std::unique_ptr<HarnessHelper> newHarnessHelper() {
    return stdx::make_unique<HSEKVDBIndexHarness>();
}


TEST(KVDBIndexTest, Isolation) {
    const std::unique_ptr<HarnessHelper> harnessHelper(newHarnessHelper());

    const std::unique_ptr<SortedDataInterface> sorted(harnessHelper->newSortedDataInterface(true));

    {
        const ServiceContext::UniqueOperationContext opCtx(harnessHelper->newOperationContext());
        ASSERT(sorted->isEmpty(opCtx.get()));
    }

    {
        const ServiceContext::UniqueOperationContext opCtx(harnessHelper->newOperationContext());
        {
            WriteUnitOfWork uow(opCtx.get());

            ASSERT_OK(sorted->insert(opCtx.get(), key1, loc1, false));
            ASSERT_OK(sorted->insert(opCtx.get(), key2, loc2, false));

            uow.commit();
        }
    }

    {
        const ServiceContext::UniqueOperationContext t1(harnessHelper->newOperationContext());
        const auto client2 = harnessHelper->serviceContext()->makeClient("c2");
        const auto t2 = harnessHelper->newOperationContext(client2.get());

        const std::unique_ptr<WriteUnitOfWork> w1(new WriteUnitOfWork(t1.get()));
        const std::unique_ptr<WriteUnitOfWork> w2(new WriteUnitOfWork(t2.get()));

        ASSERT_OK(sorted->insert(t1.get(), key3, loc3, false));
        ASSERT_OK(sorted->insert(t2.get(), key4, loc4, false));

        // this should throw
        ASSERT_THROWS(sorted->insert(t2.get(), key3, loc5, false), WriteConflictException);

        w1->commit();  // this should succeed
    }

    {
        const ServiceContext::UniqueOperationContext t1(harnessHelper->newOperationContext());
        const auto client2 = harnessHelper->serviceContext()->makeClient("c2");
        const auto t2 = harnessHelper->newOperationContext(client2.get());

        const std::unique_ptr<WriteUnitOfWork> w2(new WriteUnitOfWork(t2.get()));
        // ensure we start w2 transaction
        ASSERT_OK(sorted->insert(t2.get(), key4, loc4, false));

        {
            const std::unique_ptr<WriteUnitOfWork> w1(new WriteUnitOfWork(t1.get()));

            {
                WriteUnitOfWork w(t1.get());
                ASSERT_OK(sorted->insert(t1.get(), key5, loc3, false));
                w.commit();
            }
            w1->commit();
        }

        // this should throw
        ASSERT_THROWS(sorted->insert(t2.get(), key5, loc3, false), WriteConflictException);
    }
}

void testSeekExactRemoveNext(bool forward, bool unique) {
    auto harnessHelper = newHarnessHelper();
    auto opCtx = harnessHelper->newOperationContext();
    auto sorted =
        harnessHelper->newSortedDataInterface(unique, {{key1, loc1}, {key2, loc1}, {key3, loc1}});
    auto cursor = sorted->newCursor(opCtx.get(), forward);
    ASSERT_EQ(cursor->seekExact(key2), IndexKeyEntry(key2, loc1));
    cursor->save();
    removeFromIndex(opCtx, sorted, {{key2, loc1}});
    cursor->restore();
    ASSERT_EQ(cursor->next(), forward ? IndexKeyEntry(key3, loc1) : IndexKeyEntry(key1, loc1));
    ASSERT_EQ(cursor->next(), boost::none);
}

TEST(KVDBIndexTest, SeekExactRemoveNext_Forward_Unique) {
    testSeekExactRemoveNext(true, true);
}

TEST(KVDBIndexTest, SeekExactRemoveNext_Forward_Standard) {
    testSeekExactRemoveNext(true, false);
}

TEST(KVDBIndexTest, SeekExactRemoveNext_Reverse_Unique) {
    testSeekExactRemoveNext(false, true);
}

TEST(KVDBIndexTest, SeekExactRemoveNext_Reverse_Standard) {
    testSeekExactRemoveNext(false, false);
}
}
