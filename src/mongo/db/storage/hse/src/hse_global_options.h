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
        : _mpoolName{kDefaultMpoolName},
          _kvdbName{kDefaultKvdbName},
          _forceLag{kDefaultForceLag},
          _kvdbCParamsStr{kDefaultKvdbCParamsStr},
          _kvdbRParamsStr{kDefaultKvdbRParamsStr},
          // old
          _priKvsCParamsStr{kDefaultPriKvsCParamsStr},
          _priKvsRParamsStr{kDefaultPriKvsRParamsStr},
          _secKvsCParamsStr{kDefaultSecKvsCParamsStr},
          _secKvsRParamsStr{kDefaultSecKvsRParamsStr},
          // new
          _CParamsStrMainKvs{""},
          _CParamsStrUniqIdxKvs{""},
          _CParamsStrStdIdxKvs{""},
          _CParamsStrLargeKvs{""},
          _CParamsStrOplogKvs{""},
          _CParamsStrOplogLargeKvs{""},

          _RParamsStrMainKvs{""},
          _RParamsStrUniqIdxKvs{""},
          _RParamsStrStdIdxKvs{""},
          _RParamsStrLargeKvs{""},
          _RParamsStrOplogKvs{""},
          _RParamsStrOplogLargeKvs{""},

          _collectionParamsStr{kDefaultCollParamsStr},
          _enableMetrics{kDefaultEnableMetrics},
          _crashSafeCounters{false},
          _kvdbC1Enabled{false} {}

    Status add(moe::OptionSection* options);
    Status store(const moe::Environment& params, const std::vector<std::string>& args);

    std::string getMpoolName() const;

    std::string getKvdbName() const;

    std::string getKvdbCParamsStr() const;
    std::string getKvdbRParamsStr() const;
    // old
    std::string getPriKvsCParamsStr() const;
    std::string getPriKvsRParamsStr() const;
    std::string getSecKvsCParamsStr() const;
    std::string getSecKvsRParamsStr() const;
    // new
    std::string getCParamsStrMainKvs() const;
    std::string getCParamsStrUniqIdxKvs() const;
    std::string getCParamsStrStdIdxKvs() const;
    std::string getCParamsStrLargeKvs() const;
    std::string getCParamsStrOplogKvs() const;
    std::string getCParamsStrOplogLargeKvs() const;

    std::string getRParamsStrMainKvs() const;
    std::string getRParamsStrUniqIdxKvs() const;
    std::string getRParamsStrStdIdxKvs() const;
    std::string getRParamsStrLargeKvs() const;
    std::string getRParamsStrOplogKvs() const;
    std::string getRParamsStrOplogLargeKvs() const;

    std::string getCollParamsStr() const;


    bool getMetricsEnabled() const;
    bool getCrashSafeCounters() const;
    int getForceLag() const;
    bool getKvdbC1Enabled() const;
    void setKvdbC1Enabled(bool enabled);

    // For testing
    void setKvdbName(std::string name);

private:
    static const std::string kDefaultMpoolName;
    static const std::string kDefaultKvdbName;
    static const int kDefaultForceLag;

    static const std::string kDefaultKvdbCParamsStr;
    static const std::string kDefaultKvdbRParamsStr;
    // old
    static const std::string kDefaultPriKvsCParamsStr;
    static const std::string kDefaultPriKvsRParamsStr;
    static const std::string kDefaultSecKvsCParamsStr;
    static const std::string kDefaultSecKvsRParamsStr;

    static const std::string kDefaultCollParamsStr;
    static const bool kDefaultEnableMetrics;

    std::string _mpoolName;
    std::string _kvdbName;
    int _forceLag;

    std::string _kvdbCParamsStr;
    std::string _kvdbRParamsStr;
    // old
    std::string _priKvsCParamsStr;
    std::string _priKvsRParamsStr;
    std::string _secKvsCParamsStr;
    std::string _secKvsRParamsStr;
    // new
    std::string _CParamsStrMainKvs;
    std::string _CParamsStrUniqIdxKvs;
    std::string _CParamsStrStdIdxKvs;
    std::string _CParamsStrLargeKvs;
    std::string _CParamsStrOplogKvs;
    std::string _CParamsStrOplogLargeKvs;

    std::string _RParamsStrMainKvs;
    std::string _RParamsStrUniqIdxKvs;
    std::string _RParamsStrStdIdxKvs;
    std::string _RParamsStrLargeKvs;
    std::string _RParamsStrOplogKvs;
    std::string _RParamsStrOplogLargeKvs;


    std::string _collectionParamsStr;
    bool _enableMetrics;

    bool _crashSafeCounters;
    bool _kvdbC1Enabled;
};

extern KVDBGlobalOptions kvdbGlobalOptions;
}
