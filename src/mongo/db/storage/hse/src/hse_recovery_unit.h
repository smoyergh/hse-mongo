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

#include <atomic>
#include <string>
#include <unordered_map>
#include <vector>

#include <cerrno>

#include "mongo/base/checked_cast.h"
#include "mongo/base/disallow_copying.h"
#include "mongo/base/owned_pointer_vector.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/record_id.h"
#include "mongo/db/storage/recovery_unit.h"


#include "hse.h"
#include "hse_clienttxn.h"
#include "hse_counter_manager.h"
#include "hse_durability_manager.h"
#include "hse_exceptions.h"
#include "hse_kvscursor.h"
#include "hse_util.h"

using hse::KVDB;
using hse::KVDBData;
using hse::KVSHandle;
using hse::KvsCursor;
using hse::ClientTxn;

using namespace std;

namespace mongo {

static const unsigned int MGETCO_DEFAULT_READSIZE = 4000;  // 1 page - malloc overhead

struct KVDBCounter {
    std::atomic<long long>* _value;
    long long _delta;
    KVDBCounter() : KVDBCounter(nullptr, 0) {}
    KVDBCounter(std::atomic<long long>* value, long long delta) : _value(value), _delta(delta) {}
};

typedef std::unordered_map<std::string, KVDBCounter> KVDBCounterMap;


class KVDBRecoveryUnit : public RecoveryUnit {

public:
    KVDBRecoveryUnit(KVDB& kvdb,
                     KVDBCounterManager& counterManager,
                     KVDBDurabilityManager& durabilityManager);

    virtual ~KVDBRecoveryUnit();

    virtual void beginUnitOfWork(OperationContext* opCtx);

    virtual void commitUnitOfWork();

    virtual void abortUnitOfWork();

    virtual bool waitUntilDurable();

    virtual void abandonSnapshot();

    // [MU_REVISIT] - Default for now
    // virtual Status setReadFromMajorityCommittedSnapshot() {
    //    return {ErrorCodes::CommandNotSupported,
    //            "Current storage engine does not support majority
    //            readConcerns"};
    //}

    // virtual bool isReadingFromMajorityCommittedSnapshot() const {
    //    return false;
    //}

    // virtual boost::optional<SnapshotName> getMajorityCommittedSnapshot() const
    // {
    //    dassert(!isReadingFromMajorityCommittedSnapshot());
    //    return {};
    //}

    virtual SnapshotId getSnapshotId() const;

    virtual void registerChange(Change* change);

    virtual void* writingPtr(void* data, size_t len);

    virtual void setRollbackWritesDisabled();

    /**
     * KVDB IO routines
     */
    hse::Status _get(
        const KVSHandle& h, const KVDBData& key, KVDBData& val, bool& found, bool use_txn);
    hse::Status probeVlen(
        const KVSHandle& h, const KVDBData& key, KVDBData& val, unsigned long len, bool& found);
    hse::Status put(const KVSHandle& h, const KVDBData& key, const KVDBData& val);
    hse::Status getCo(const KVSHandle& h,
                      const KVDBData& key,
                      KVDBData& val,
                      bool& found,
                      unsigned long& foundLen);
    hse::Status getMCo(
        const KVSHandle& h, const KVDBData& key, KVDBData& val, bool& found, bool use_txn = true);
    hse::Status probeKey(const KVSHandle& h, const KVDBData& key, bool& found);
    hse::Status del(const KVSHandle& h, const KVDBData& key);
    hse::Status prefixGet(const KVSHandle& h,
                          const KVDBData& prefix,
                          KVDBData& key,
                          KVDBData& val,
                          hse_kvs_pfx_probe_cnt& found);
    hse::Status prefixDelete(const KVSHandle& h, const KVDBData& prefix);
    hse::Status iterDelete(const KVSHandle& h, const KVDBData& prefix);
    hse::Status nonTxnPut(const KVSHandle& h, const KVDBData& key, const KVDBData& val);
    hse::Status nonTxnDel(const KVSHandle& h, const KVDBData& key);
    hse::Status nonTxnPfxDel(const KVSHandle& h, const KVDBData& prefix);
    hse::Status nonTxnIterDelete(const KVSHandle& h, const KVDBData& prefix);
    hse::Status beginScan(const KVSHandle& h,
                          KVDBData prefix,
                          bool forward,
                          KvsCursor** cursor,
                          const struct hse::CompParms& compparm);
    hse::Status cursorUpdate(KvsCursor* cursor);
    hse::Status cursorSeek(KvsCursor* cursor, const KVDBData& key, KVDBData* foundKey);
    hse::Status cursorRead(KvsCursor* cursor, KVDBData& key, KVDBData& val, bool& eof);
    hse::Status endScan(KvsCursor* cursor);

    hse::Status beginOplogScan(const KVSHandle& h,
                               KVDBData prefix,
                               bool forward,
                               KvsCursor** cursor,
                               const struct hse::CompParms& compparm,
                               bool readAhead = false);
    hse::Status oplogCursorUpdate(KvsCursor* cursor);
    hse::Status oplogCursorSeek(KvsCursor* cursor,
                                const KVDBData& key,
                                const KVDBData* kmax,
                                KVDBData* foundKey);


    static KVDBRecoveryUnit* getKVDBRecoveryUnit(OperationContext* opCtx) {
        return checked_cast<KVDBRecoveryUnit*>(opCtx->recoveryUnit());
    }

    void incrementCounter(const string counterKey,
                          std::atomic<long long>* counter,
                          long long delta);
    void resetCounter(const string counterKey, std::atomic<long long>* counter);

    long long getDeltaCounter(const string counterKey);

    bool ActiveClientTxn() {
        return (_txn != nullptr);
    }

    KVDBRecoveryUnit* newKVDBRecoveryUnit();

private:
    void _ensureTxn();

    KVDB& _kvdb;  // db handle

    uint64_t _snapId;  // read snapshot ID
    typedef OwnedPointerVector<Change> Changes;
    Changes _changes;

    ClientTxn* _txn;

    KVDBCounterManager& _counterManager;
    KVDBDurabilityManager& _durabilityManager;

    KVDBCounterMap _deltaCounters;
    long _updates = 0;  // used to determine if any updates between samples
};
}
