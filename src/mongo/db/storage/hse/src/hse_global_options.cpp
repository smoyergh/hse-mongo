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
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "mongo/base/status.h"
#include "mongo/util/log.h"
#include "mongo/util/options_parser/constraints.h"

#include "hse_global_options.h"


namespace mongo {

const int KVDBGlobalOptions::kDefaultForceLag = 0;

// Collection options
const std::string KVDBGlobalOptions::kDefaultCollectionCompressionStr = "lz4";
const std::string KVDBGlobalOptions::kDefaultCollectionCompressionMinBytesStr = "0";

const bool KVDBGlobalOptions::kDefaultEnableMetrics = false;


KVDBGlobalOptions kvdbGlobalOptions;

namespace {
const std::string modName{"hse"};

const std::string forceLagOptStr = modName + "ForceLag";

const std::string cfgStrPrefix = ("storage." + modName) + ".";

const std::string forceLagCfgStr = cfgStrPrefix + "forceLag";

// Collection options.
const std::string collectionCompressionCfgStr = cfgStrPrefix + "collectionCompression";
const std::string collectionCompressionMinBytesCfgStr =
    cfgStrPrefix + "collectionCompressionMinBytes";
const std::string collectionCompressionOptStr = modName + "CollectionCompression";
const std::string collectionCompressionMinBytesOptStr = modName + "CollectionCompressionMinBytes";

// Enable metrics
const std::string enableMetricsCfgStr = cfgStrPrefix + "enableMetrics";
const std::string enableMetricsOptStr = modName + "EnableMetrics";
}  // namespace

Status KVDBGlobalOptions::add(moe::OptionSection* options) {
    moe::OptionSection kvdbOptions("Heterogeneous-memory Storage Engine options");

    kvdbOptions
        .addOptionChaining(forceLagCfgStr, forceLagOptStr, moe::Int, "force x seconds of lag")
        .hidden()
        .setDefault(moe::Value(kDefaultForceLag));

    // Collection options
    kvdbOptions
        .addOptionChaining(collectionCompressionCfgStr,
                           collectionCompressionOptStr,
                           moe::String,
                           "collection compression algorithm [none|lz4]")
        .format("(:?none)|(:?lz4)", "[none|lz4]")
        .setDefault(moe::Value(kDefaultCollectionCompressionStr));
    kvdbOptions
        .addOptionChaining(collectionCompressionMinBytesCfgStr,
                           collectionCompressionMinBytesOptStr,
                           moe::String,
                           "compression minimum size <values whose size is <= to this size are "
                           "not compressed>")
        .setDefault(moe::Value(kDefaultCollectionCompressionMinBytesStr));

    kvdbOptions
        .addOptionChaining(
            enableMetricsCfgStr, enableMetricsOptStr, moe::Switch, "enable metrics collection")
        .hidden();


    return options->addSection(kvdbOptions);
}

Status KVDBGlobalOptions::store(const moe::Environment& params,
                                const std::vector<std::string>& args) {
    if (params.count(forceLagCfgStr)) {
        kvdbGlobalOptions._forceLag = params[forceLagCfgStr].as<int>();
        log() << "Force Lag: " << kvdbGlobalOptions._forceLag;
    }

    if (params.count(collectionCompressionCfgStr)) {
        kvdbGlobalOptions._collectionCompressionStr =
            params[collectionCompressionCfgStr].as<std::string>();
        log() << "Collection compression Algo str: " << kvdbGlobalOptions._collectionCompressionStr;
    }

    if (params.count(collectionCompressionMinBytesCfgStr)) {
        kvdbGlobalOptions._collectionCompressionMinBytesStr =
            params[collectionCompressionMinBytesCfgStr].as<std::string>();
        log() << "Collection compression minimum size  str: "
              << kvdbGlobalOptions._collectionCompressionMinBytesStr;
    }

    if (params.count(enableMetricsCfgStr)) {
        kvdbGlobalOptions._enableMetrics = params[enableMetricsCfgStr].as<bool>();
        log() << "Metrics enabled: " << kvdbGlobalOptions._enableMetrics;
    }

    return Status::OK();
}

bool KVDBGlobalOptions::getCrashSafeCounters() const {
    return _crashSafeCounters;
}

std::string KVDBGlobalOptions::getCollectionCompressionStr() const {
    return _collectionCompressionStr;
}

std::string KVDBGlobalOptions::getCollectionCompressionMinBytesStr() const {
    return _collectionCompressionMinBytesStr;
}

bool KVDBGlobalOptions::getMetricsEnabled() const {
    return _enableMetrics;
}

int KVDBGlobalOptions::getForceLag() const {
    return _forceLag;
}

}  // namespace mongo
