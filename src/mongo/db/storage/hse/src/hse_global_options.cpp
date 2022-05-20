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
const std::string KVDBGlobalOptions::kDefaultCompressionStr = "lz4";
const std::string KVDBGlobalOptions::kDefaultCompressionMinBytesStr = "0";

const bool KVDBGlobalOptions::kDefaultEnableMetrics = false;

// Default staging path is empty.
const std::string KVDBGlobalOptions::kDefaultStagingPathStr{};

// Default pmem path is empty.
const std::string KVDBGlobalOptions::kDefaultPmemPathStr{};

// Default config path is empty.
const std::string KVDBGlobalOptions::kDefaultConfigPathStr{};


KVDBGlobalOptions kvdbGlobalOptions;

namespace {
const std::string modName{"hse"};

const std::string forceLagOptStr = modName + "ForceLag";

const std::string cfgStrPrefix = ("storage." + modName) + ".";

const std::string forceLagCfgStr = cfgStrPrefix + "forceLag";

// Collection options.
const std::string compressionCfgStr = cfgStrPrefix + "compression";
const std::string compressionMinBytesCfgStr = cfgStrPrefix + "compressionMinBytes";
const std::string optimizeForCollectionCountCfgStr = cfgStrPrefix + "optimizeForCollectionCount";
const std::string compressionOptStr = modName + "Compression";
const std::string compressionMinBytesOptStr = modName + "CompressionMinBytes";

// Enable metrics
const std::string enableMetricsCfgStr = cfgStrPrefix + "enableMetrics";
const std::string enableMetricsOptStr = modName + "EnableMetrics";

// HSE staging path
const std::string stagingPathCfgStr = cfgStrPrefix + "stagingPath";
const std::string stagingPathOptStr = modName + "StagingPath";

// HSE pmem path
const std::string pmemPathCfgStr = cfgStrPrefix + "pmemPath";
const std::string pmemPathOptStr = modName + "PmemPath";

// HSE config path
const std::string configPathCfgStr = cfgStrPrefix + "configPath";
const std::string configPathOptStr = modName + "ConfigPath";

}  // namespace

Status KVDBGlobalOptions::add(moe::OptionSection* options) {
    moe::OptionSection kvdbOptions("Heterogeneous-memory Storage Engine options");

    kvdbOptions
        .addOptionChaining(forceLagCfgStr, forceLagOptStr, moe::Int, "force x seconds of lag")
        .hidden()
        .setDefault(moe::Value(kDefaultForceLag));

    // Collection options
    kvdbOptions
        .addOptionChaining(
            compressionCfgStr, compressionOptStr, moe::String, "compression algorithm [none|lz4]")
        .format("(:?none)|(:?lz4)", "[none|lz4]")
        .setDefault(moe::Value(kDefaultCompressionStr));
    kvdbOptions
        .addOptionChaining(compressionMinBytesCfgStr,
                           compressionMinBytesOptStr,
                           moe::String,
                           "compression minimum size <values whose size is <= to this size are "
                           "not compressed>")
        .setDefault(moe::Value(kDefaultCompressionMinBytesStr));

    kvdbOptions
        .addOptionChaining(
            enableMetricsCfgStr, enableMetricsOptStr, moe::Switch, "enable metrics collection")
        .hidden();

    kvdbOptions
        .addOptionChaining(
            stagingPathCfgStr, stagingPathOptStr, moe::String, "path for staging media class")
        .setDefault(moe::Value(kDefaultStagingPathStr));

    kvdbOptions
        .addOptionChaining(pmemPathCfgStr, pmemPathOptStr, moe::String, "path for pmem media class")
        .setDefault(moe::Value(kDefaultPmemPathStr));

    kvdbOptions
        .addOptionChaining(configPathCfgStr, configPathOptStr, moe::String, "path for config file")
        .setDefault(moe::Value(kDefaultConfigPathStr));

    return options->addSection(kvdbOptions);
}

Status KVDBGlobalOptions::store(const moe::Environment& params,
                                const std::vector<std::string>& args) {
    if (params.count(forceLagCfgStr)) {
        kvdbGlobalOptions._forceLag = params[forceLagCfgStr].as<int>();
        log() << "Force Lag: " << kvdbGlobalOptions._forceLag;
    }

    if (params.count(compressionCfgStr)) {
        kvdbGlobalOptions._compressionStr = params[compressionCfgStr].as<std::string>();
        log() << "Compression Algo str: " << kvdbGlobalOptions._compressionStr;
    }

    if (params.count(compressionMinBytesCfgStr)) {
        kvdbGlobalOptions._compressionMinBytesStr =
            params[compressionMinBytesCfgStr].as<std::string>();
        log() << "Compression minimum size  str: " << kvdbGlobalOptions._compressionMinBytesStr;
    }

    if (params.count(optimizeForCollectionCountCfgStr)) {
        kvdbGlobalOptions._optimizeForCollectionCountStr =
            params[optimizeForCollectionCountCfgStr].as<std::string>();
        log() << "Optimize for collection count str: "
              << kvdbGlobalOptions._optimizeForCollectionCountStr;
    }

    if (params.count(enableMetricsCfgStr)) {
        kvdbGlobalOptions._enableMetrics = params[enableMetricsCfgStr].as<bool>();
        log() << "Metrics enabled: " << kvdbGlobalOptions._enableMetrics;
    }

    if (params.count(stagingPathCfgStr)) {
        kvdbGlobalOptions._stagingPathStr = params[stagingPathCfgStr].as<std::string>();
        log() << "Staging path str: " << kvdbGlobalOptions._stagingPathStr;
    }

    if (params.count(pmemPathCfgStr)) {
        kvdbGlobalOptions._pmemPathStr = params[pmemPathCfgStr].as<std::string>();
        log() << "Pmem path str: " << kvdbGlobalOptions._pmemPathStr;
    }

    if (params.count(configPathCfgStr)) {
        kvdbGlobalOptions._configPathStr = params[configPathCfgStr].as<std::string>();
        log() << "Config path str: " << kvdbGlobalOptions._configPathStr;
    }

    return Status::OK();
}

bool KVDBGlobalOptions::getCrashSafeCounters() const {
    return _crashSafeCounters;
}

std::string KVDBGlobalOptions::getCompressionStr() const {
    return _compressionStr;
}

std::string KVDBGlobalOptions::getCompressionMinBytesStr() const {
    return _compressionMinBytesStr;
}

bool KVDBGlobalOptions::getMetricsEnabled() const {
    return _enableMetrics;
}

int KVDBGlobalOptions::getForceLag() const {
    return _forceLag;
}

std::string KVDBGlobalOptions::getStagingPathStr() const {
    return _stagingPathStr;
}

std::string KVDBGlobalOptions::getPmemPathStr() const {
    return _pmemPathStr;
}

std::string KVDBGlobalOptions::getConfigPathStr() const {
    return _configPathStr;
}


}  // namespace mongo
