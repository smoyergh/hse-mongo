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
