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

#include <chrono>
#include <string>
#include <vector>

#include "mongo/db/client.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"

#include "hse_stats.h"
#include "hse_util.h"
#include "hse_versions.h"

#include <syscall.h>

using namespace std;
using namespace std::chrono;

using mongo::BSONArrayBuilder;

namespace hse_stat {

// Global hse stat lists
vector<KVDBStat*> gHseStatVersionList;
vector<KVDBStat*> gHseStatCounterList;
vector<KVDBStat*> gHseStatLatencyList;
vector<KVDBStat*> gHseStatAppBytesList;
vector<KVDBStat*> gHseStatRateList;

// Current time as maintained by the stat rate counter thread, updated once per second.
// May lag the actual time due to scheduling, but will never go backward.
std::chrono::time_point<std::chrono::steady_clock> gHseStatTime;

bool KVDBStat::statsEnabled = false;

/* The countersv[] array provides COUNTER_GROUPS_MAX per-cpu atomic counters for each
 * counter of type KVDBStatCounter and KVDBStatAppBytes.
 *
 * The countersv[] array comprises COUNTER_GROUPS_MAX contiguous chunks of counters,
 * where each chunk contains COUNTERS_PER_GROUP atomic counters.  Each chunk is
 * sufficiently large to eliminate false-sharing due to adjacent cacheline prefetch.
 * It needs to be at least as large as the number of defined KVDBStatCounter and
 * KVDBStatAppBytes counters, and for efficiency should be a power of two.
 *
 * Each KVDBStat constructor increments countersc to obtain a unique offset into
 * countersv[], and then may access any atomic counter in any group by that same
 * offset into the group, where typically the CPU ID is used to determine which
 * group to access.  In practice, we split the array into two equal parts and
 * use the low bit of the NUMA node ID to select the part and hence eliminate
 * or reduce cacheline thrashing between NUMA nodes.
 */
#define COUNTERS_PER_GROUP (16)
#define COUNTER_GROUPS_MAX (16)

atomic<int64_t> countersc;
alignas(128) atomic<int64_t> countersv[COUNTERS_PER_GROUP * COUNTER_GROUPS_MAX];


// begin KVDBStat
KVDBStat::KVDBStat(const string name) : _name(name) {
    _enabled = statsEnabled || _enableOverride;
}

void KVDBStat::appendTo(BSONObjBuilder& bob) const {
    invariantHse(false);
}

void KVDBStat::enableStatsGlobally(bool enable) {
    statsEnabled = enable;

    for (auto st : gHseStatCounterList)
        st->_enabled = statsEnabled || st->_enableOverride;
    for (auto st : gHseStatLatencyList)
        st->_enabled = statsEnabled || st->_enableOverride;
    for (auto st : gHseStatAppBytesList)
        st->_enabled = statsEnabled || st->_enableOverride;
    for (auto st : gHseStatRateList)
        st->_enabled = statsEnabled || st->_enableOverride;
}

// This does not reflect enable overrides
bool KVDBStat::isStatsEnabledGlobally() {
    return statsEnabled;
}

KVDBStat::~KVDBStat() {}
// end KVDBStat

// begin KVDBStatCounter
KVDBStatCounter::KVDBStatCounter(const string name) : KVDBStat(name) {
    gHseStatCounterList.push_back(this);

    invariantHse(countersc.load() < COUNTERS_PER_GROUP);
    _counterv = countersv + countersc.fetch_add(1);
}

void KVDBStatCounter::appendTo(BSONObjBuilder& bob) const {
    int64_t accum = 0;
    unsigned int i;

    if (!isStatEnabled()) {
        return;
    }

    for (i = 0; i < COUNTER_GROUPS_MAX; ++i)
        accum += _counterv[i * COUNTERS_PER_GROUP].load(memory_order::memory_order_relaxed);

    bob.append(_name, accum);
}

void KVDBStatCounter::add_impl(int64_t incr) {
    unsigned int cpuid, nodeid;

    if (syscall(SYS_getcpu, &cpuid, &nodeid))
        cpuid = nodeid = 0;

    cpuid = cpuid % (COUNTER_GROUPS_MAX / 2) + (nodeid & 1u) * (COUNTER_GROUPS_MAX / 2);

    _counterv[cpuid * COUNTERS_PER_GROUP].fetch_add(incr, memory_order::memory_order_relaxed);
}

KVDBStatCounter::~KVDBStatCounter() {}
// end KVDBStatCounter

// begin KVDBStatLatency
KVDBStatLatency::KVDBStatLatency(const string name, int32_t buckets, int64_t interval)
    : KVDBStat(name), _buckets{buckets}, _interval{interval} {
    gHseStatLatencyList.push_back(this);

    /* Need an even number of buckets in order to simplify bucket interleaving,
     * plus one for the latencies that fall outside the histogram bounds.
     */
    _buckets = (_buckets + 1) & ~1u;
    _histogram.resize(_buckets + 1);
}

void KVDBStatLatency::appendTo(BSONObjBuilder& bob) const {
    if (!isStatEnabled()) {
        return;
    }

    BSONObjBuilder lBob;
    lBob.append("buckets", _buckets);
    lBob.append("interval", _interval);
    lBob.append("histogramsOverflow",
                _histogram[_buckets].hits.load(memory_order::memory_order_relaxed));
    lBob.append("minLatency", (_minLatency == INT64_MAX) ? 0 : _minLatency);
    lBob.append("maxLatency", _maxLatency);

    BSONArrayBuilder hitsArrBob{_buckets};
    BSONArrayBuilder avArrBob{_buckets};

    for (int i = 0; i < _buckets; i += 2) {
        int64_t hits = _histogram[i].hits.load(memory_order::memory_order_relaxed);
        int64_t total = _histogram[i].total.load(memory_order::memory_order_relaxed);

        hitsArrBob.append(hits);
        avArrBob.append(hits ? (total / hits) : 0);
    }

    for (int i = 1; i < _buckets; i += 2) {
        int64_t hits = _histogram[i].hits.load(memory_order::memory_order_relaxed);
        int64_t total = _histogram[i].total.load(memory_order::memory_order_relaxed);

        hitsArrBob.append(hits);
        avArrBob.append(hits ? (total / hits) : 0);
    }

    lBob.append("hits", hitsArrBob.arr());
    lBob.append("avglat", avArrBob.arr());

    bob.append(_name, lBob.obj());
}

void KVDBStatLatency::end_impl(LatencyToken bTime) {
    auto eTime = chrono::steady_clock::now();
    int64_t latency = (chrono::duration_cast<chrono::nanoseconds>(eTime - bTime)).count();

    // HSE_REVISIT - need faster approach?
    int32_t bucket = latency / _interval;

    if (bucket >= _buckets) {
        _histogram[_buckets].hits.fetch_add(1, memory_order::memory_order_relaxed);
        return;
    }

    /* Latency measurements tend to clump together in adjacent "hot" buckets,
     * so we interleave the buckets to eliminate false-sharing between them.
     * For example, given 16 buckets (from 0 to 15) we address them linearly
     * in memory as follows:  0 8 1 9 2 10 3 11 4 12 5 13 6 14 7 15 (16)
     * where latencies that fall outside the maximum bucket interval range
     * accumulate in the last bucket (bucket 16 in this example).
     */
    if (bucket < _buckets / 2)
        bucket = bucket * 2;
    else
        bucket = bucket - (_buckets / 2) + 1;

    HistogramBucket& bRef = _histogram[bucket];
    bRef.hits.fetch_add(1, memory_order::memory_order_relaxed);
    bRef.total.fetch_add(latency, memory_order::memory_order_relaxed);

    // update min and max (not atomic)
    if (latency < _minLatency) {
        _minLatency = latency;
    }

    if (latency > _maxLatency) {
        _maxLatency = latency;
    }
}

KVDBStatLatency::~KVDBStatLatency() {}
// end KVDBStatLatency

// begin KVDBStatVersion
KVDBStatVersion::KVDBStatVersion(const string name, const string ver) : KVDBStat(name), _ver{ver} {
    gHseStatVersionList.push_back(this);
}

void KVDBStatVersion::appendTo(BSONObjBuilder& bob) const {
    bob.append(_name, _ver);
}

KVDBStatVersion::~KVDBStatVersion() {}
// end KVDBStatVersion

// begin KVDBStatAppBytes
KVDBStatAppBytes::KVDBStatAppBytes(const string name, bool enableOverride) : KVDBStat(name) {
    _enabled = statsEnabled || enableOverride;
    _enableOverride = enableOverride;
    gHseStatAppBytesList.push_back(this);

    invariantHse(countersc.load() < COUNTERS_PER_GROUP);
    _counterv = countersv + countersc.fetch_add(1);
}

void KVDBStatAppBytes::appendTo(BSONObjBuilder& bob) const {
    if (!isStatEnabled()) {
        bob.append(_name, "DISABLED");
        return;
    }

    int64_t accum = 0;
    unsigned int i;

    for (i = 0; i < COUNTER_GROUPS_MAX; ++i)
        accum += _counterv[i * COUNTERS_PER_GROUP].load(memory_order::memory_order_relaxed);

    bob.append(_name, accum);
}

void KVDBStatAppBytes::add(int64_t incr) {
    unsigned int cpuid, nodeid;

    if (syscall(SYS_getcpu, &cpuid, &nodeid))
        cpuid = nodeid = 0;

    cpuid = cpuid % (COUNTER_GROUPS_MAX / 2) + (nodeid & 1u) * (COUNTER_GROUPS_MAX / 2);

    _counterv[cpuid * COUNTERS_PER_GROUP].fetch_add(incr, memory_order::memory_order_relaxed);
}

KVDBStatAppBytes::~KVDBStatAppBytes() {}
// end KVDBStatAppBytes

// begin KVDBStatRate
std::unique_ptr<KVDBStatRate::RateThread> KVDBStatRate::_rateThread{};

KVDBStatRate::KVDBStatRate(const string name, bool enableOverride) : KVDBStat(name) {
    _enabled = statsEnabled || enableOverride;
    _enableOverride = enableOverride;
    _lastUpdated = steady_clock::now();
    gHseStatRateList.push_back(this);
}

void KVDBStatRate::init() {
    // start thread
    _rateThread = mongo::stdx::make_unique<KVDBStatRate::RateThread>();
    invariantHse(_rateThread);
    _rateThread->go();
}

void KVDBStatRate::appendTo(BSONObjBuilder& bob) const {
    if (!isStatEnabled()) {
        bob.append(_name, "DISABLED");
        return;
    }

    bob.append(_name, _rate.load(memory_order::memory_order_relaxed));
}

void KVDBStatRate::update(uint64_t incr) {
    _count.fetch_add(incr, memory_order::memory_order_relaxed);
}

void KVDBStatRate::calculateRate() {
    auto dt = chrono::duration_cast<chrono::milliseconds>(gHseStatTime - _lastUpdated);

    uint64_t count = _count.load(memory_order::memory_order_relaxed);
    uint64_t rate = (count * 1000) / dt.count();

    _count.fetch_sub(count, memory_order::memory_order_relaxed);
    _rate.store(rate, memory_order::memory_order_relaxed);
    _lastUpdated = gHseStatTime;
}

int64_t KVDBStatRate::getRate() {
    return _rate.load(memory_order::memory_order_relaxed);
}

void KVDBStatRate::finish() {
    // stop thread
    if (_rateThread) {
        _rateThread->shutdown();
        _rateThread->wait();
    }
}

KVDBStatRate::~KVDBStatRate() {}

KVDBStatRate::RateThread::RateThread() : BackgroundJob(false /*deleteSelf */) {}

std::string KVDBStatRate::RateThread::name() const {
    return "KVDBStatRateThread";
}

void KVDBStatRate::RateThread::run() {
    mongo::Client::initThread(name().c_str());

    while (!_shuttingDown.load()) {
        gHseStatTime = chrono::steady_clock::now();

        for (auto st : gHseStatRateList) {
            if (st->isStatEnabled()) {
                KVDBStatRate* rateP = static_cast<KVDBStatRate*>(st);
                rateP->calculateRate();
            }
        }

        mongo::sleepmillis(1000);
    }
    mongo::log() << "stopping " << name() << " thread";
}

void KVDBStatRate::RateThread::shutdown() {
    _shuttingDown.store(true);
    wait();
}
// end KVDBStatRate

// Start Stats declarations
// Versions
KVDBStatVersion _hseVersion{"hseVersion", hse::K_HSE_VERSION};
KVDBStatVersion _hseConnectorVersion{"hseConnectorVersion", hse::K_HSE_CONNECTOR_VERSION};
KVDBStatVersion _hseConnectorGitSha{"hseConnectorGitSha", hse::K_HSE_CONNECTOR_GIT_SHA};

// Counters
KVDBStatCounter _hseKvsGetCounter{"hseKvsGet"};
KVDBStatCounter _hseKvsPutCounter{"hseKvsPut"};
KVDBStatCounter _hseKvsDeleteCounter{"hseKvsDelete"};
KVDBStatCounter _hseKvsPrefixDeleteCounter{"hseKvsPrefixDelete"};
KVDBStatCounter _hseKvsProbeCounter{"hseKvsProbe"};
KVDBStatCounter _hseKvdbSyncCounter{"hseKvdbSync"};
KVDBStatCounter _hseKvsCursorCreateCounter{"hseKvsCursorCreate"};
KVDBStatCounter _hseKvsCursorDestroyCounter{"hseKvsCursorDestroy"};
KVDBStatCounter _hseKvsCursorReadCounter{"hseKvsCursorRead"};
KVDBStatCounter _hseKvsCursorUpdateCounter{"hseKvsCursorUpdate"};
KVDBStatCounter _hseOplogCursorCreateCounter{"hseOplogCursorCreate"};

// Latencies

// histogram parameters based on sysbench small db run

KVDBStatLatency _hseKvsGetLatency{"hseKvsGet", 32, 1000};
KVDBStatLatency _hseKvsPutLatency{"hseKvsPut", 32, 1000};
KVDBStatLatency _hseKvsDeleteLatency{"hseKvsDelete", 16, 1000};
KVDBStatLatency _hseKvsPrefixDeleteLatency{"hseKvsPrefixDelete", 16, 100 * 1000};
KVDBStatLatency _hseKvsProbeLatency{"hseKvsProbe", 32, 1000};
KVDBStatLatency _hseKvdbSyncLatency{"hseKvdbSync", 32, 2 * 1000 * 1000};
KVDBStatLatency _hseKvsCursorCreateLatency{"hseKvsCursorCreate", 32, 1000};
KVDBStatLatency _hseKvsCursorDestroyLatency{"hseKvsCursorDestroy", 32, 1000};
KVDBStatLatency _hseKvsCursorReadLatency{"hseKvsCursorRead", 32, 1000};
KVDBStatLatency _hseKvsCursorUpdateLatency{"hseKvsCursorUpdate", 32, 1000};

// App bytes counters
KVDBStatAppBytes _hseAppBytesReadCounter{"hseAppBytesRead"};
KVDBStatAppBytes _hseAppBytesWrittenCounter{"hseAppBytesWritten"};

// Rate stats
KVDBStatRate _hseOplogCursorReadRate{"hseOplogCursorRead"};

// End Stats declarations

}  // hse_stat
