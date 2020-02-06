/**
 * TBD: HSE license
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
