/**
 * TODO: HSE License
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
    if (KVDBStat::isStatsEnabled()) {
        bob.append("counters", _buildStatsBObj(gHseStatCounterList));
        bob.append("latencies", _buildStatsBObj(gHseStatLatencyList));
    }
    // some of the rates are are always enabled since needed functionally
    // hence always try to print.
    bob.append("rates", _buildStatsBObj(gHseStatRateList));

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
