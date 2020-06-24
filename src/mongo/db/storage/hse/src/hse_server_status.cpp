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
#include <sstream>

#include "mongo/platform/basic.h"

#include "hse_server_status.h"

#include "boost/scoped_ptr.hpp"

#include "mongo/base/checked_cast.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/scopeguard.h"

#include "hse_engine.h"

namespace mongo {
using std::string;

using hse_stat::KVDBStat;
using hse_stat::gHseStatVersionList;
using hse_stat::gHseStatCounterList;
using hse_stat::gHseStatLatencyList;
using hse_stat::gHseStatAppBytesList;
using hse_stat::gHseStatRateList;


KVDBServerStatusSection::KVDBServerStatusSection(KVDBEngine& engine)
    : ServerStatusSection("hse"), _engine(engine) {}

bool KVDBServerStatusSection::includeByDefault() const {
    return true;
}

BSONObj KVDBServerStatusSection::generateSection(OperationContext* txn,
                                                 const BSONElement& configElement) const {

    BSONObjBuilder bob;

    bob.append("versionInfo", _buildStatsBObj(gHseStatVersionList));
    bob.append("appBytes", _buildStatsBObj(gHseStatAppBytesList));
    if (KVDBStat::isStatsEnabledGlobally()) {
        bob.append("counters", _buildStatsBObj(gHseStatCounterList));
        bob.append("latencies", _buildStatsBObj(gHseStatLatencyList));
        bob.append("rates", _buildStatsBObj(gHseStatRateList));
    }


    return bob.obj();
}

BSONObj KVDBServerStatusSection::_buildStatsBObj(vector<KVDBStat*>& statList) const {
    BSONObjBuilder bob;

    for (auto st : statList) {
        st->appendTo(bob);
    }

    return bob.obj();
}


}  // namespace mongo
