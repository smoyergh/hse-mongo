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

#include "mongo/base/init.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/kv/kv_storage_engine.h"
#include "mongo/db/storage/storage_engine_metadata.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/util/mongoutils/str.h"

#include "hse_engine.h"
#include "hse_global_options.h"
#include "hse_server_status.h"
#include "hse_stats.h"


namespace mongo {
const std::string kKVDBEngineName = "hse";

namespace {
using hse_stat::KVDBStat;

class KVDBFactory : public StorageEngine::Factory {
public:
    virtual ~KVDBFactory() {}
    virtual StorageEngine* create(const StorageGlobalParams& params,
                                  const StorageEngineLockFile* lockFile) const {
        KVStorageEngineOptions options;
        options.directoryPerDB = params.directoryperdb;
        options.forRepair = params.repair;
        // Mongo keeps some files in params.dbpath. To avoid collision, put out files under
        // db/ directory
        if (formatVersion == -1) {
            // it's a new database, set it to the newest hse version kKVDBFormatVersion
            formatVersion = kKVDBFormatVersion;
        }

        auto engine =
            new KVDBEngine(params.dbpath + "/hse", params.dur, formatVersion, params.readOnly);

        if (kvdbGlobalOptions.getMetricsEnabled()) {
            KVDBStat::enableStatsGlobally(true);
        }

        // Intentionally leaked.
        auto leaked __attribute__((unused)) = new KVDBServerStatusSection(*engine);

        return new KVStorageEngine(engine, options);
    }

    virtual StringData getCanonicalName() const {
        return kKVDBEngineName;
    }

    virtual Status validateCollectionStorageOptions(const BSONObj& options) const {
        return Status::OK();
    }

    virtual Status validateMetadata(const StorageEngineMetadata& metadata,
                                    const StorageGlobalParams& params) const {
        const BSONObj& options = metadata.getStorageEngineOptions();
        BSONElement element = options.getField(kKVDBFormatVersionString);
        if (element.eoo() || !element.isNumber()) {
            return Status(ErrorCodes::UnsupportedFormat,
                          "Storage engine metadata format not recognized. If you created "
                          "this database with older version of mongo, please reload the "
                          "database using mongodump and mongorestore");
        }
        if (element.numberInt() < kMinSupportedKVDBFormatVersion) {
            // database is older than what we can understand
            return Status(ErrorCodes::UnsupportedFormat,
                          str::stream()
                              << "Database was created with old format version "
                              << element.numberInt()
                              << " and this version only supports format versions from "
                              << kMinSupportedKVDBFormatVersion
                              << " to "
                              << kKVDBFormatVersion
                              << ". Please reload the database using mongodump and mongorestore");
        } else if (element.numberInt() > kKVDBFormatVersion) {
            // database is newer than what we can understand
            return Status(ErrorCodes::UnsupportedFormat,
                          str::stream()
                              << "Database was created with newer format version "
                              << element.numberInt()
                              << " and this version only supports format versions from "
                              << kMinSupportedKVDBFormatVersion
                              << " to "
                              << kKVDBFormatVersion
                              << ". Please reload the database using mongodump and mongorestore");
        }
        formatVersion = element.numberInt();
        return Status::OK();
    }

    virtual BSONObj createMetadataOptions(const StorageGlobalParams& params) const {
        BSONObjBuilder builder;
        builder.append(kKVDBFormatVersionString, kKVDBFormatVersion);
        return builder.obj();
    }

    bool supportsReadOnly() const final {
        return true;
    }

private:
    // Current disk format. We bump this number when we change the disk format. MongoDB will
    // fail to start if the versions don't match. In that case a user needs to run mongodump
    // and mongorestore.
    // * Version 0 was the format with many column families -- one column family for each
    // collection and index
    // * Version 1 keeps all collections and indexes in a single column family
    // * Version 2 reserves two prefixes for oplog. one prefix keeps the oplog
    // documents and another only keeps keys. That way, we can cleanup the oplog without
    // reading full documents
    // * Version 3 (current) understands the Decimal128 index format. It also understands
    // the version 2, so it's backwards compatible, but not forward compatible
    const int kKVDBFormatVersion = 0;
    const int kMinSupportedKVDBFormatVersion = 0;
    const std::string kKVDBFormatVersionString = "HSEKVDBFormatVersion";
    int mutable formatVersion = -1;
};
}  // namespace

MONGO_INITIALIZER_WITH_PREREQUISITES(KVDBEngineInit, ("SetGlobalEnvironment"))
(InitializerContext* context) {
    getGlobalServiceContext()->registerStorageEngine(kKVDBEngineName, new KVDBFactory());
    return Status::OK();
}
}  // namespace mongo
