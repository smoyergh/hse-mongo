/**
 * TODO: HSE License
 */

#pragma once

#include "mongo/db/commands/server_status.h"

#include "hse_stats.h"

namespace mongo {
using hse_stat::KVDBStat;

class KVDBEngine;

/**
 * Adds "hse" to the results of db.serverStatus().
 */
class KVDBServerStatusSection : public ServerStatusSection {
public:
    KVDBServerStatusSection(KVDBEngine& engine);
    virtual bool includeByDefault() const;
    virtual BSONObj generateSection(OperationContext* txn, const BSONElement& configElement) const;

private:
    KVDBEngine& _engine;
    BSONObj _buildStatsBObj(vector<KVDBStat*>& statList) const;
};

}  // namespace mongo
