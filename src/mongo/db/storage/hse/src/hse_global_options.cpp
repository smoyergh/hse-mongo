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

// Default REST server is enabled.
const bool KVDBGlobalOptions::kDefaultRestEnabled = true;

// Collection options
const std::string KVDBGlobalOptions::kDefaultValueCompressionDefaultStr = "on";

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

const std::string restEnabledCfgStr = cfgStrPrefix + "restEnabled";
const std::string restEnabledOptStr = modName + "RestEnabled";

// Collection options.
const std::string optimizeForCollectionCountCfgStr = cfgStrPrefix + "optimizeForCollectionCount";
const std::string valueCompressionDefaultCfgStr = cfgStrPrefix + "valueCompressionDefault";
const std::string valueCompressionDefaultOptStr = modName + "ValueCompressionDefault";

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

    kvdbOptions
        .addOptionChaining(
            restEnabledCfgStr, restEnabledOptStr, moe::Bool, "enable the REST server")
        .setDefault(moe::Value(kDefaultRestEnabled));

    // Collection options
    kvdbOptions
        .addOptionChaining(valueCompressionDefaultCfgStr,
                           valueCompressionDefaultOptStr,
                           moe::String,
                           "whether to compress values by default")
        .setDefault(moe::Value(kDefaultValueCompressionDefaultStr));

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

    if (params.count(restEnabledCfgStr)) {
        kvdbGlobalOptions._restEnabled = params[restEnabledCfgStr].as<bool>();
        log() << "REST enabled: " << kvdbGlobalOptions._restEnabled;
    }

    if (params.count(valueCompressionDefaultCfgStr)) {
        kvdbGlobalOptions._valueCompressionDefaultStr =
            params[valueCompressionDefaultCfgStr].as<std::string>();
        log() << "Value compression default: " << kvdbGlobalOptions._valueCompressionDefaultStr;
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

bool KVDBGlobalOptions::getRestEnabled() const {
    return _restEnabled;
}

bool KVDBGlobalOptions::getCrashSafeCounters() const {
    return _crashSafeCounters;
}

std::string KVDBGlobalOptions::getValueCompressionDefaultStr() const {
    return _valueCompressionDefaultStr;
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
