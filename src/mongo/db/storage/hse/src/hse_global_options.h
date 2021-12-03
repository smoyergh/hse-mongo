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
#pragma once

#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {

namespace moe = mongo::optionenvironment;

class KVDBGlobalOptions {
public:
    KVDBGlobalOptions()
        : _forceLag{kDefaultForceLag},
          _compressionStr{kDefaultCompressionStr},
          _compressionMinBytesStr{kDefaultCompressionMinBytesStr},
          _optimizeForCollectionCountStr{kDefaultOptimizeForCollectionCountStr},
          _enableMetrics{kDefaultEnableMetrics},
          _crashSafeCounters{false},
          _stagingPathStr{kDefaultStagingPathStr},
          _pmemPathStr{kDefaultPmemPathStr},
          _configPathStr{kDefaultConfigPathStr} {}

    Status add(moe::OptionSection* options);
    Status store(const moe::Environment& params, const std::vector<std::string>& args);

    std::string getCompressionStr() const;
    std::string getCompressionMinBytesStr() const;
    std::string getOptimizeForCollectionCountStr() const;


    bool getMetricsEnabled() const;
    bool getCrashSafeCounters() const;
    int getForceLag() const;
    std::string getStagingPathStr() const;
    std::string getPmemPathStr() const;
    std::string getConfigPathStr() const;

private:
    static const int kDefaultForceLag;
    static const std::string kDefaultCompressionStr;
    static const std::string kDefaultCompressionMinBytesStr;
    static const std::string kDefaultOptimizeForCollectionCountStr;
    static const bool kDefaultEnableMetrics;
    static const std::string kDefaultStagingPathStr;
    static const std::string kDefaultPmemPathStr;
    static const std::string kDefaultConfigPathStr;

    int _forceLag;

    std::string _compressionStr;
    std::string _compressionMinBytesStr;
    std::string _optimizeForCollectionCountStr;
    bool _enableMetrics;
    bool _crashSafeCounters;
    std::string _stagingPathStr;
    std::string _pmemPathStr;
    std::string _configPathStr;
};

extern KVDBGlobalOptions kvdbGlobalOptions;
}  // namespace mongo
