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
const std::string KVDBGlobalOptions::kDefaultKvdbName = "mp1";
const int KVDBGlobalOptions::kDefaultForceLag = 0;

const std::string KVDBGlobalOptions::kDefaultKvdbCParamsStr = "";
const std::string KVDBGlobalOptions::kDefaultKvdbRParamsStr = "";

// Old
const std::string KVDBGlobalOptions::kDefaultPriKvsCParamsStr = "";
const std::string KVDBGlobalOptions::kDefaultPriKvsRParamsStr = "";
const std::string KVDBGlobalOptions::kDefaultSecKvsCParamsStr = "";
const std::string KVDBGlobalOptions::kDefaultSecKvsRParamsStr = "";

// Collection options
const std::string KVDBGlobalOptions::kDefaultCollParamsStr = "compalgo=lz4,compminsize=0";

const bool KVDBGlobalOptions::kDefaultEnableMetrics = false;


KVDBGlobalOptions kvdbGlobalOptions;

namespace {
const std::string modName{"hse"};
const std::string mpoolNameOptStr = modName + "MpoolName";
const std::string kvdbNameOptStr = modName + "KvdbName";
const std::string forceLagOptStr = modName + "ForceLag";

const std::string kvdbCParamsOptStr = modName + "KvdbCParams";
const std::string kvdbRParamsOptStr = modName + "KvdbRParams";

// Old
const std::string priKvsCParamsOptStr = modName + "PriKvsCParams";
const std::string priKvsRParamsOptStr = modName + "PriKvsRParams";
const std::string secKvsCParamsOptStr = modName + "SecKvsCParams";
const std::string secKvsRParamsOptStr = modName + "SecKvsRParams";

// New
const std::string KvsCParamsOpt_MainKvs = modName + "MainKvsCParams";
const std::string KvsCParamsOpt_UniqIdxKvs = modName + "UniqIdxKvsCParams";
const std::string KvsCParamsOpt_StdIdxKvs = modName + "StdIdxKvsCParams";
const std::string KvsCParamsOpt_LargeKvs = modName + "LargeKvsCParams";
const std::string KvsCParamsOpt_OplogKvs = modName + "OplogKvsCParams";
const std::string KvsCParamsOpt_OplogLargeKvs = modName + "OplogLargeKvsCParams";

const std::string KvsRParamsOpt_MainKvs = modName + "MainKvsRParams";
const std::string KvsRParamsOpt_UniqIdxKvs = modName + "UniqIdxKvsRParams";
const std::string KvsRParamsOpt_StdIdxKvs = modName + "StdIdxKvsRParams";
const std::string KvsRParamsOpt_LargeKvs = modName + "LargeKvsRParams";
const std::string KvsRParamsOpt_OplogKvs = modName + "OplogKvsRParams";
const std::string KvsRParamsOpt_OplogLargeKvs = modName + "OplogLargeKvsRParams";


const std::string cfgStrPrefix = ("storage." + modName) + ".";

const std::string mpoolNameCfgStr = cfgStrPrefix + "mpoolName";
const std::string kvdbNameCfgStr = cfgStrPrefix + "kvdbName";
const std::string forceLagCfgStr = cfgStrPrefix + "forceLag";

const std::string kvdbCParamsCfgStr = cfgStrPrefix + "kvdbCParams";
const std::string kvdbRParamsCfgStr = cfgStrPrefix + "kvdbRParams";
// old
const std::string priKvsCParamsCfgStr = cfgStrPrefix + "priKvsCParams";
const std::string priKvsRParamsCfgStr = cfgStrPrefix + "priKvsRParams";
const std::string secKvsCParamsCfgStr = cfgStrPrefix + "secKvsCParams";
const std::string secKvsRParamsCfgStr = cfgStrPrefix + "secKvsRParams";
// new
const std::string KvsCParamsCfg_MainKvs = cfgStrPrefix + "MainKvsCParams";
const std::string KvsCParamsCfg_UniqIdxKvs = cfgStrPrefix + "UniqIdxKvsCParams";
const std::string KvsCParamsCfg_StdIdxKvs = cfgStrPrefix + "StdIdxKvsCParams";
const std::string KvsCParamsCfg_LargeKvs = cfgStrPrefix + "LargeKvsCParams";
const std::string KvsCParamsCfg_OplogKvs = cfgStrPrefix + "OplogKvsCParams";
const std::string KvsCParamsCfg_OplogLargeKvs = cfgStrPrefix + "OplogLargeKvsCParams";

const std::string KvsRParamsCfg_MainKvs = cfgStrPrefix + "MainKvsRParams";
const std::string KvsRParamsCfg_UniqIdxKvs = cfgStrPrefix + "UniqIdxKvsRParams";
const std::string KvsRParamsCfg_StdIdxKvs = cfgStrPrefix + "StdIdxKvsRParams";
const std::string KvsRParamsCfg_LargeKvs = cfgStrPrefix + "LargeKvsRParams";
const std::string KvsRParamsCfg_OplogKvs = cfgStrPrefix + "OplogKvsRParams";
const std::string KvsRParamsCfg_OplogLargeKvs = cfgStrPrefix + "OplogLargeKvsRParams";


// Collection options.

const std::string collectionParamsCfgStr = cfgStrPrefix + "collectionParams";
const std::string collectionParamsOptStr = modName + "CollectionParams";

// Enable metrics
const std::string enableMetricsCfgStr = cfgStrPrefix + "enableMetrics";
const std::string enableMetricsOptStr = modName + "EnableMetrics";
}

Status KVDBGlobalOptions::add(moe::OptionSection* options) {
    moe::OptionSection kvdbOptions("Heterogeneous-memory Storage Engine options");

    kvdbOptions
        .addOptionChaining(
            mpoolNameCfgStr, mpoolNameOptStr, moe::String, "name of the MPool containing the KVDB")
        .setDefault(moe::Value(kDefaultMpoolName));
    kvdbOptions
        .addOptionChaining(forceLagCfgStr, forceLagOptStr, moe::Int, "force x seconds of lag")
        .setDefault(moe::Value(kDefaultForceLag));
    kvdbOptions.addOptionChaining(kvdbNameCfgStr, kvdbNameOptStr, moe::String, "name of the KVDB")
        .setDefault(moe::Value(kDefaultKvdbName));
    kvdbOptions
        .addOptionChaining(
            kvdbCParamsCfgStr, kvdbCParamsOptStr, moe::String, "KVDB creation parameters")
        .setDefault(moe::Value(kDefaultKvdbCParamsStr));
    kvdbOptions
        .addOptionChaining(
            kvdbRParamsCfgStr, kvdbRParamsOptStr, moe::String, "KVDB runtime parameters")
        .setDefault(moe::Value(kDefaultKvdbRParamsStr));
    // Old
    kvdbOptions
        .addOptionChaining(priKvsCParamsCfgStr,
                           priKvsCParamsOptStr,
                           moe::String,
                           "primary KVS creation parameters")
        .setDefault(moe::Value(kDefaultPriKvsCParamsStr));
    kvdbOptions
        .addOptionChaining(
            priKvsRParamsCfgStr, priKvsRParamsOptStr, moe::String, "primary KVS runtime parameters")
        .setDefault(moe::Value(kDefaultPriKvsRParamsStr));
    kvdbOptions
        .addOptionChaining(secKvsCParamsCfgStr,
                           secKvsCParamsOptStr,
                           moe::String,
                           "secondary KVS creation parameters")
        .setDefault(moe::Value(kDefaultSecKvsCParamsStr));
    kvdbOptions
        .addOptionChaining(secKvsRParamsCfgStr,
                           secKvsRParamsOptStr,
                           moe::String,
                           "secondary KVS runtime parameters")
        .setDefault(moe::Value(kDefaultSecKvsRParamsStr));

    // New
    kvdbOptions
        .addOptionChaining(KvsCParamsCfg_MainKvs,
                           KvsCParamsOpt_MainKvs,
                           moe::String,
                           "MainKvs creation parameters")
        .setDefault(moe::Value(""));
    kvdbOptions
        .addOptionChaining(KvsCParamsCfg_UniqIdxKvs,
                           KvsCParamsOpt_UniqIdxKvs,
                           moe::String,
                           "UniqIdxKvs creation parameters")
        .setDefault(moe::Value(""));
    kvdbOptions
        .addOptionChaining(KvsCParamsCfg_StdIdxKvs,
                           KvsCParamsOpt_StdIdxKvs,
                           moe::String,
                           "StdIdxKvs creation parameters")
        .setDefault(moe::Value(""));
    kvdbOptions
        .addOptionChaining(KvsCParamsCfg_LargeKvs,
                           KvsCParamsOpt_LargeKvs,
                           moe::String,
                           "LargeKvs creation parameters")
        .setDefault(moe::Value(""));
    kvdbOptions
        .addOptionChaining(KvsCParamsCfg_OplogKvs,
                           KvsCParamsOpt_OplogKvs,
                           moe::String,
                           "OplogKvs creation parameters")
        .setDefault(moe::Value(""));
    kvdbOptions
        .addOptionChaining(KvsCParamsCfg_OplogLargeKvs,
                           KvsCParamsOpt_OplogLargeKvs,
                           moe::String,
                           "OplogLargeKvs creation parameters")
        .setDefault(moe::Value(""));

    kvdbOptions
        .addOptionChaining(
            KvsRParamsCfg_MainKvs, KvsRParamsOpt_MainKvs, moe::String, "MainKvs runtime parameters")
        .setDefault(moe::Value(""));
    kvdbOptions
        .addOptionChaining(KvsRParamsCfg_UniqIdxKvs,
                           KvsRParamsOpt_UniqIdxKvs,
                           moe::String,
                           "UniqIdxKvs runtime parameters")
        .setDefault(moe::Value(""));
    kvdbOptions
        .addOptionChaining(KvsRParamsCfg_StdIdxKvs,
                           KvsRParamsOpt_StdIdxKvs,
                           moe::String,
                           "StdIdxKvs runtime parameters")
        .setDefault(moe::Value(""));
    kvdbOptions
        .addOptionChaining(KvsRParamsCfg_LargeKvs,
                           KvsRParamsOpt_LargeKvs,
                           moe::String,
                           "LargeKvs runtime parameters")
        .setDefault(moe::Value(""));
    kvdbOptions
        .addOptionChaining(KvsRParamsCfg_OplogKvs,
                           KvsRParamsOpt_OplogKvs,
                           moe::String,
                           "OplogKvs runtime parameters")
        .setDefault(moe::Value(""));
    kvdbOptions
        .addOptionChaining(KvsRParamsCfg_OplogLargeKvs,
                           KvsRParamsOpt_OplogLargeKvs,
                           moe::String,
                           "OplogLargeKvs runtime parameters")
        .setDefault(moe::Value(""));


    // Collection options

    kvdbOptions
        .addOptionChaining(collectionParamsCfgStr,
                           collectionParamsOptStr,
                           moe::String,
                           "compalgo={lz4}[,compminsize=<values whose size is <= to this size are "
                           "not compressed>]")
        .setDefault(moe::Value(kDefaultCollParamsStr));

    kvdbOptions.addOptionChaining(
        enableMetricsCfgStr, enableMetricsOptStr, moe::Switch, "enable metrics collection");


    return options->addSection(kvdbOptions);
}

Status KVDBGlobalOptions::store(const moe::Environment& params,
                                const std::vector<std::string>& args) {
    if (params.count(mpoolNameCfgStr)) {
        kvdbGlobalOptions._mpoolName = params[mpoolNameCfgStr].as<std::string>();
        log() << "Mpool Name: " << kvdbGlobalOptions._mpoolName;
    }

    if (params.count(kvdbNameCfgStr)) {
        kvdbGlobalOptions._kvdbName = params[kvdbNameCfgStr].as<std::string>();
        log() << "KVDB Name: " << kvdbGlobalOptions._kvdbName;
    }

    if (params.count(forceLagCfgStr)) {
        kvdbGlobalOptions._forceLag = params[forceLagCfgStr].as<int>();
        log() << "Force Lag: " << kvdbGlobalOptions._forceLag;
    }

    if (params.count(kvdbCParamsCfgStr)) {
        kvdbGlobalOptions._kvdbCParamsStr = params[kvdbCParamsCfgStr].as<std::string>();
        log() << "KVDB creation params str: " << kvdbGlobalOptions._kvdbCParamsStr;
    }

    if (params.count(kvdbRParamsCfgStr)) {
        kvdbGlobalOptions._kvdbRParamsStr = params[kvdbRParamsCfgStr].as<std::string>();
        log() << "KVDB runtime params str: " << kvdbGlobalOptions._kvdbRParamsStr;
    }

    // old
    if (params.count(priKvsCParamsCfgStr)) {
        kvdbGlobalOptions._priKvsCParamsStr = params[priKvsCParamsCfgStr].as<std::string>();
        log() << "Primary KVS creation params str: " << kvdbGlobalOptions._priKvsCParamsStr;
    }

    if (params.count(priKvsRParamsCfgStr)) {
        kvdbGlobalOptions._priKvsRParamsStr = params[priKvsRParamsCfgStr].as<std::string>();
        log() << "Primary KVS runtime params str: " << kvdbGlobalOptions._priKvsRParamsStr;
    }

    if (params.count(secKvsCParamsCfgStr)) {
        kvdbGlobalOptions._secKvsCParamsStr = params[secKvsCParamsCfgStr].as<std::string>();
        log() << "Secondary KVS creation params str: " << kvdbGlobalOptions._secKvsCParamsStr;
    }

    if (params.count(secKvsRParamsCfgStr)) {
        kvdbGlobalOptions._secKvsRParamsStr = params[secKvsRParamsCfgStr].as<std::string>();
        log() << "Secondary KVS runtime params str: " << kvdbGlobalOptions._secKvsRParamsStr;
    }

    // new
    if (params.count(KvsCParamsCfg_MainKvs)) {
        kvdbGlobalOptions._CParamsStrMainKvs = params[KvsCParamsCfg_MainKvs].as<std::string>();
        log() << "MainKvs creation params str: " << kvdbGlobalOptions._CParamsStrMainKvs;
    }
    if (params.count(KvsCParamsCfg_UniqIdxKvs)) {
        kvdbGlobalOptions._CParamsStrUniqIdxKvs =
            params[KvsCParamsCfg_UniqIdxKvs].as<std::string>();
        log() << "UniqIdxKvs creation params str: " << kvdbGlobalOptions._CParamsStrUniqIdxKvs;
    }
    if (params.count(KvsCParamsCfg_StdIdxKvs)) {
        kvdbGlobalOptions._CParamsStrStdIdxKvs = params[KvsCParamsCfg_StdIdxKvs].as<std::string>();
        log() << "StdIdxKvs creation params str: " << kvdbGlobalOptions._CParamsStrStdIdxKvs;
    }
    if (params.count(KvsCParamsCfg_LargeKvs)) {
        kvdbGlobalOptions._CParamsStrLargeKvs = params[KvsCParamsCfg_LargeKvs].as<std::string>();
        log() << "LargeKvs creation params str: " << kvdbGlobalOptions._CParamsStrLargeKvs;
    }
    if (params.count(KvsCParamsCfg_OplogKvs)) {
        kvdbGlobalOptions._CParamsStrOplogKvs = params[KvsCParamsCfg_OplogKvs].as<std::string>();
        log() << "OplogKvs creation params str: " << kvdbGlobalOptions._CParamsStrOplogKvs;
    }
    if (params.count(KvsCParamsCfg_OplogLargeKvs)) {
        kvdbGlobalOptions._CParamsStrOplogLargeKvs =
            params[KvsCParamsCfg_OplogLargeKvs].as<std::string>();
        log() << "OplogLargeKvs creation params str: "
              << kvdbGlobalOptions._CParamsStrOplogLargeKvs;
    }

    if (params.count(KvsRParamsCfg_MainKvs)) {
        kvdbGlobalOptions._RParamsStrMainKvs = params[KvsRParamsCfg_MainKvs].as<std::string>();
        log() << "MainKvs runtime params str: " << kvdbGlobalOptions._RParamsStrMainKvs;
    }
    if (params.count(KvsRParamsCfg_UniqIdxKvs)) {
        kvdbGlobalOptions._RParamsStrUniqIdxKvs =
            params[KvsRParamsCfg_UniqIdxKvs].as<std::string>();
        log() << "UniqIdxKvs runtime params str: " << kvdbGlobalOptions._RParamsStrUniqIdxKvs;
    }
    if (params.count(KvsRParamsCfg_StdIdxKvs)) {
        kvdbGlobalOptions._RParamsStrStdIdxKvs = params[KvsRParamsCfg_StdIdxKvs].as<std::string>();
        log() << "StdIdxKvs runtime params str: " << kvdbGlobalOptions._RParamsStrStdIdxKvs;
    }
    if (params.count(KvsRParamsCfg_LargeKvs)) {
        kvdbGlobalOptions._RParamsStrLargeKvs = params[KvsRParamsCfg_LargeKvs].as<std::string>();
        log() << "LargeKvs runtime params str: " << kvdbGlobalOptions._RParamsStrLargeKvs;
    }
    if (params.count(KvsRParamsCfg_OplogKvs)) {
        kvdbGlobalOptions._RParamsStrOplogKvs = params[KvsRParamsCfg_OplogKvs].as<std::string>();
        log() << "OplogKvs runtime params str: " << kvdbGlobalOptions._RParamsStrOplogKvs;
    }
    if (params.count(KvsRParamsCfg_OplogLargeKvs)) {
        kvdbGlobalOptions._RParamsStrOplogLargeKvs =
            params[KvsRParamsCfg_OplogLargeKvs].as<std::string>();
        log() << "OplogLargeKvs runtime params str: " << kvdbGlobalOptions._RParamsStrOplogLargeKvs;
    }

    if (params.count(collectionParamsCfgStr)) {
        kvdbGlobalOptions._collectionParamsStr = params[collectionParamsCfgStr].as<std::string>();
        log() << "Collection creation parameters params str: "
              << kvdbGlobalOptions._collectionParamsStr;
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

std::string KVDBGlobalOptions::getKvdbName() const {
    return _kvdbName;
}

bool KVDBGlobalOptions::getCrashSafeCounters() const {
    return _crashSafeCounters;
}

std::string KVDBGlobalOptions::getKvdbCParamsStr() const {
    return _kvdbCParamsStr;
}

std::string KVDBGlobalOptions::getKvdbRParamsStr() const {
    return _kvdbRParamsStr;
}

// old
std::string KVDBGlobalOptions::getPriKvsCParamsStr() const {
    return _priKvsCParamsStr;
}

std::string KVDBGlobalOptions::getPriKvsRParamsStr() const {
    return _priKvsRParamsStr;
}

std::string KVDBGlobalOptions::getSecKvsCParamsStr() const {
    return _secKvsCParamsStr;
}

std::string KVDBGlobalOptions::getSecKvsRParamsStr() const {
    return _secKvsRParamsStr;
}

// New
// old
std::string KVDBGlobalOptions::getCParamsStrMainKvs() const {
    return _CParamsStrMainKvs;
}
std::string KVDBGlobalOptions::getCParamsStrUniqIdxKvs() const {
    return _CParamsStrUniqIdxKvs;
}
std::string KVDBGlobalOptions::getCParamsStrStdIdxKvs() const {
    return _CParamsStrStdIdxKvs;
}
std::string KVDBGlobalOptions::getCParamsStrLargeKvs() const {
    return _CParamsStrLargeKvs;
}
std::string KVDBGlobalOptions::getCParamsStrOplogKvs() const {
    return _CParamsStrOplogKvs;
}
std::string KVDBGlobalOptions::getCParamsStrOplogLargeKvs() const {
    return _CParamsStrOplogLargeKvs;
}

std::string KVDBGlobalOptions::getRParamsStrMainKvs() const {
    return _RParamsStrMainKvs;
}
std::string KVDBGlobalOptions::getRParamsStrUniqIdxKvs() const {
    return _RParamsStrUniqIdxKvs;
}
std::string KVDBGlobalOptions::getRParamsStrStdIdxKvs() const {
    return _RParamsStrStdIdxKvs;
}
std::string KVDBGlobalOptions::getRParamsStrLargeKvs() const {
    return _RParamsStrLargeKvs;
}
std::string KVDBGlobalOptions::getRParamsStrOplogKvs() const {
    return _RParamsStrOplogKvs;
}
std::string KVDBGlobalOptions::getRParamsStrOplogLargeKvs() const {
    return _RParamsStrOplogLargeKvs;
}

std::string KVDBGlobalOptions::getCollParamsStr() const {
    return _collectionParamsStr;
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

void KVDBGlobalOptions::setKvdbName(std::string name) {
    _kvdbName = name;
}

}  // namespace mongo
