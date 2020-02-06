/**
 * TBD: HSE License
 */

#include "mongo/platform/basic.h"

#include <boost/filesystem/operations.hpp>
#include <memory>

#include "mongo/db/storage/kv/kv_engine.h"
#include "mongo/db/storage/kv/kv_engine_test_harness.h"
#include "mongo/unittest/temp_dir.h"

#include "hse_engine.h"
#include "hse_global_options.h"
#include "hse_ut_common.h"

namespace mongo {
class KVDBEngineHarnessHelper : public KVHarnessHelper {
public:
    KVDBEngineHarnessHelper() : _dbpath("mongo-hse-engine-test") {
        boost::filesystem::remove_all(_dbpath.path());

        _dbFixture.closeDb();
        kvdbGlobalOptions.setKvdbName(_dbFixture.getDbName());
        restartEngine();
    }

    virtual ~KVDBEngineHarnessHelper() {
        _engine.reset();

        // drops all kvses + drops kvdb if requested via KVDB_PER_UT env
        _dbFixture.reset();
        _dbFixture.closeDb();
    }

    virtual KVEngine* getEngine() {
        return _engine.get();
    }

    virtual KVEngine* restartEngine() {
        _engine.reset(nullptr);
        _engine.reset(new KVDBEngine(_dbpath.path(), false, 3, false));
        return _engine.get();
    }

private:
    unittest::TempDir _dbpath;

    std::unique_ptr<KVDBEngine> _engine;

    hse::KVDBTestSuiteFixture& _dbFixture = KVDBTestSuiteFixture::getFixture();
    hse::KVDB& _db = _dbFixture.getDb();
};

KVHarnessHelper* KVHarnessHelper::create() {
    return new KVDBEngineHarnessHelper();
}
}
