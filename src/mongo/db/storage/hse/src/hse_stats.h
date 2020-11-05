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
#include <chrono>
#include <string>
#include <vector>

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/util/background.h"

using namespace std;
using mongo::BackgroundJob;

namespace hse_stat {

/**
 * How to add and use a new stat?
 *   1) First identify the type of stat -
 *      available types are version, counter, latency, appBytes.
 *      It should also be fairly easy to add new types
 *   2) Declare the stat in both hse_stat.h and hse_stat.cpp, see below.
 *   3) Use the stat in any connector source after including hse_stat.h.
 */

using mongo::BSONObjBuilder;

using LatencyToken = std::chrono::time_point<std::chrono::steady_clock>;


class HistogramBucket {
public:
    HistogramBucket() : total{0}, hits{0} {};

    HistogramBucket(const HistogramBucket& n) {
        this->total.store(n.total.load());
        this->hits.store(n.hits.load());
    }

    HistogramBucket(const HistogramBucket&& n) {
        this->total.store(n.total.load());
        this->hits.store(n.hits.load());
    }

    HistogramBucket& operator=(const HistogramBucket& rhs) {
        this->total.store(rhs.total.load());
        this->hits.store(rhs.hits.load());
        return *this;
    }

    atomic<int64_t> total{0};
    atomic<int64_t> hits{0};
};

class KVDBStat {
public:
    KVDBStat(const string name);
    virtual void appendTo(BSONObjBuilder& bob) const;
    static void enableStatsGlobally(bool enable);
    static bool isStatsEnabledGlobally();

    bool isStatEnabled() const {
        return _enabled;
    }

    virtual ~KVDBStat();

protected:
    static bool statsEnabled;     // for global behaviour.
    bool _enabled;                // cached (statsEnabled || _enableOverride)
    bool _enableOverride{false};  // override global behaviour
    string _name;
};

class KVDBStatCounter : public KVDBStat {
public:
    KVDBStatCounter(const string name);
    virtual void appendTo(BSONObjBuilder& bob) const override;
    virtual ~KVDBStatCounter();

    void add(int64_t incr = 1) {
        if (isStatEnabled())
            add_impl(incr);
    }

private:
    void add_impl(int64_t incr);

    atomic<int64_t> *_counterv;
};

class KVDBStatLatency final : public KVDBStat {
public:
    KVDBStatLatency(const string name, int32_t buckets = 1000, int64_t interval = 1000000000);
    virtual ~KVDBStatLatency();

    virtual void appendTo(BSONObjBuilder& bob) const override;

    LatencyToken begin() const {
        if (MONGO_likely( !isStatEnabled() ))
            return chrono::time_point<chrono::steady_clock>(chrono::nanoseconds(0));

        return chrono::steady_clock::now();
    }

    void end(LatencyToken bTime) {
        if (bTime != chrono::time_point<chrono::steady_clock>(chrono::nanoseconds(0)) )
            end_impl(bTime);
    }

private:
    void end_impl(LatencyToken token);

    int32_t _buckets{1000};
    int64_t _interval{100000000};  // 1 ms
    int64_t _minLatency{INT64_MAX};
    int64_t _maxLatency{0};
    vector<HistogramBucket> _histogram;
    atomic<int32_t> _histogramOverflow{0};
};

class KVDBStatVersion final : public KVDBStat {
public:
    KVDBStatVersion(const string name, const string ver);
    virtual void appendTo(BSONObjBuilder& bob) const override;
    virtual ~KVDBStatVersion();

private:
    string _ver{"UNKNOWN"};
};

// this is always enabled.
class KVDBStatAppBytes final : public KVDBStat {
public:
    KVDBStatAppBytes(const string name, bool enableOverride = true);  // this is always enabled.
    virtual void appendTo(BSONObjBuilder& bob) const override;
    virtual ~KVDBStatAppBytes();
    void add(int64_t incr);

private:
    atomic<int64_t> *_counterv;
};

class KVDBStatRate final : public KVDBStat {
public:
    KVDBStatRate(const string name, bool enableOverride = false);
    static void init();
    virtual void appendTo(BSONObjBuilder& bob) const override;
    void update(uint64_t incr = 1);
    void calculateRate();
    int64_t getRate();
    static void finish();
    virtual ~KVDBStatRate();

    class RateThread : public BackgroundJob {
    public:
        explicit RateThread();
        virtual string name() const;
        virtual void run();
        void shutdown();

    private:
        std::atomic<bool> _shuttingDown{false};
    };

private:
    atomic<int64_t> _rate{0};
    atomic<uint64_t> _count{0};
    std::chrono::time_point<std::chrono::steady_clock> _lastUpdated;
    static std::unique_ptr<RateThread> _rateThread;
};

// HSE_REVISIT - TODO class KVDBStatGeneral - constructor takes register list as arg

// Global hse stat lists
extern vector<KVDBStat*> gHseStatVersionList;
extern vector<KVDBStat*> gHseStatCounterList;
extern vector<KVDBStat*> gHseStatLatencyList;
extern vector<KVDBStat*> gHseStatAppBytesList;
extern vector<KVDBStat*> gHseStatRateList;

extern std::chrono::time_point<std::chrono::steady_clock> gHseStatTime;

// Start Stats extern declarations
// Versions
extern KVDBStatVersion _hseVersion;
extern KVDBStatVersion _hseConnectorVersion;
extern KVDBStatVersion _hseGitSha;
extern KVDBStatVersion _hseConnectorGitSha;

// Counters
extern KVDBStatCounter _hseKvsGetCounter;
extern KVDBStatCounter _hseKvsCursorCreateCounter;
extern KVDBStatCounter _hseKvsCursorReadCounter;
extern KVDBStatCounter _hseKvsCursorUpdateCounter;
extern KVDBStatCounter _hseKvsCursorDestroyCounter;
extern KVDBStatCounter _hseKvsPutCounter;
extern KVDBStatCounter _hseKvsProbeCounter;
extern KVDBStatCounter _hseKvdbSyncCounter;
extern KVDBStatCounter _hseKvsDeleteCounter;
extern KVDBStatCounter _hseKvsPrefixDeleteCounter;
extern KVDBStatCounter _hseOplogCursorCreateCounter;

// Latencies
extern KVDBStatLatency _hseKvsGetLatency;
extern KVDBStatLatency _hseKvsCursorCreateLatency;
extern KVDBStatLatency _hseKvsCursorReadLatency;
extern KVDBStatLatency _hseKvsCursorUpdateLatency;
extern KVDBStatLatency _hseKvsCursorDestroyLatency;
extern KVDBStatLatency _hseKvsPutLatency;
extern KVDBStatLatency _hseKvsProbeLatency;
extern KVDBStatLatency _hseKvdbSyncLatency;
extern KVDBStatLatency _hseKvsDeleteLatency;
extern KVDBStatLatency _hseKvsPrefixDeleteLatency;

// App bytes counters
extern KVDBStatAppBytes _hseAppBytesReadCounter;
extern KVDBStatAppBytes _hseAppBytesWrittenCounter;

// Rate stats
extern KVDBStatRate _hseOplogCursorReadRate;

/* Use the rollup macro to reduce contention on highly contended KVDB
 * stat counters.
 */
#define KVDBStatCounterRollup(_stat, _bytes, _rollup)                   \
do {                                                                    \
    static thread_local uint64_t calls;                                 \
    static thread_local int64_t accum;                                  \
                                                                        \
    accum += (_bytes);                                                  \
                                                                        \
    if (calls++ % (_rollup) == 0) {                                     \
        _stat.add(accum);                                               \
        accum = 0;                                                      \
    }                                                                   \
} while (0)

// End Stats declarations

}  // hse_stat
