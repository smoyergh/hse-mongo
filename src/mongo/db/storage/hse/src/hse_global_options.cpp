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

const std::string KVDBGlobalOptions::kDefaultMpoolName = "mp1";
const int KVDBGlobalOptions::kDefaultForceLag = 0;
const std::string KVDBGlobalOptions::kDefaultConfigPathStr = "";
const std::string KVDBGlobalOptions::kDefaultParamsStr = "";

// Collection options
const std::string KVDBGlobalOptions::kDefaultCollectionCompressionStr = "lz4";
const std::string KVDBGlobalOptions::kDefaultCollectionCompressionMinBytesStr = "0";

const bool KVDBGlobalOptions::kDefaultEnableMetrics = false;


KVDBGlobalOptions kvdbGlobalOptions;

namespace {
const std::string modName{"hse"};

const std::string mpoolNameOptStr = modName + "MpoolName";

const std::string forceLagOptStr = modName + "ForceLag";

const std::string configPathOptStr = modName + "ConfigPath";

const std::string paramsOptStr = modName + "Params";

const std::string cfgStrPrefix = ("storage." + modName) + ".";

const std::string mpoolNameCfgStr = cfgStrPrefix + "mpoolName";

const std::string forceLagCfgStr = cfgStrPrefix + "forceLag";

const std::string configPathCfgStr = cfgStrPrefix + "configPath";

const std::string paramsCfgStr = cfgStrPrefix + "params";


// Collection options.
const std::string collectionCompressionCfgStr = cfgStrPrefix + "collectionCompression";
const std::string collectionCompressionMinBytesCfgStr =
    cfgStrPrefix + "collectionCompressionMinBytes";
const std::string collectionCompressionOptStr = modName + "CollectionCompression";
const std::string collectionCompressionMinBytesOptStr = modName + "CollectionCompressionMinBytes";

// Enable metrics
const std::string enableMetricsCfgStr = cfgStrPrefix + "enableMetrics";
const std::string enableMetricsOptStr = modName + "EnableMetrics";
}

Status KVDBGlobalOptions::add(moe::OptionSection* options) {
    moe::OptionSection kvdbOptions("Heterogeneous-memory Storage Engine options");

    kvdbOptions
        .addOptionChaining(
            mpoolNameCfgStr, mpoolNameOptStr, moe::String, "name of the mpool containing the kvdb")
        .setDefault(moe::Value(kDefaultMpoolName));
    kvdbOptions
        .addOptionChaining(forceLagCfgStr, forceLagOptStr, moe::Int, "force x seconds of lag")
        .hidden()
        .setDefault(moe::Value(kDefaultForceLag));
    kvdbOptions
        .addOptionChaining(configPathCfgStr, configPathOptStr, moe::String, "HSE config path")
        .setDefault(moe::Value(kDefaultParamsStr));
    kvdbOptions.addOptionChaining(paramsCfgStr, paramsOptStr, moe::String, "HSE parameters")
        .setDefault(moe::Value(kDefaultParamsStr));

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
    if (params.count(mpoolNameCfgStr)) {
        kvdbGlobalOptions._mpoolName = params[mpoolNameCfgStr].as<std::string>();
        log() << "Mpool Name: " << kvdbGlobalOptions._mpoolName;
    }

    if (params.count(forceLagCfgStr)) {
        kvdbGlobalOptions._forceLag = params[forceLagCfgStr].as<int>();
        log() << "Force Lag: " << kvdbGlobalOptions._forceLag;
    }

    if (params.count(configPathCfgStr)) {
        kvdbGlobalOptions._configPathStr = params[configPathCfgStr].as<std::string>();
        log() << "HSE config path str: " << kvdbGlobalOptions._configPathStr;
    }

    if (params.count(paramsCfgStr)) {
        kvdbGlobalOptions._paramsStr = params[paramsCfgStr].as<std::string>();
        log() << "HSE params str: " << kvdbGlobalOptions._paramsStr;
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

std::string KVDBGlobalOptions::getMpoolName() const {
    return _mpoolName;
}

bool KVDBGlobalOptions::getCrashSafeCounters() const {
    return _crashSafeCounters;
}

std::string KVDBGlobalOptions::getConfigPathStr() const {
    return _configPathStr;
}

std::string KVDBGlobalOptions::getParamsStr() const {
    return _paramsStr;
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

bool KVDBGlobalOptions::getKvdbC1Enabled() const {
    return _kvdbC1Enabled;
}

int KVDBGlobalOptions::getForceLag() const {
    return _forceLag;
}

void KVDBGlobalOptions::setKvdbC1Enabled(bool enabled) {
    _kvdbC1Enabled = enabled;
}

}  // namespace mongo
