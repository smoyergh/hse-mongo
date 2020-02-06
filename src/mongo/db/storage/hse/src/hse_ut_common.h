/**
 * TODO: LICENSE
 */
#include "mongo/platform/basic.h"

#include "hse_impl.h"
#include <iostream>
#include <sstream>

#include "mongo/unittest/unittest.h"

using namespace std;
using namespace hse;


namespace hse {
class KVDBTestSuiteFixture {

public:
    KVDBTestSuiteFixture();
    ~KVDBTestSuiteFixture();

    KVDB& getDb();
    string getDbName();
    static KVDBTestSuiteFixture& getFixture();

    void closeDb();
    void reset();

private:
    string _kvdbName{"mp1"};
    string _mpoolName{"mp1"};

    struct hse_params* _kvdbCfg;
    struct hse_params* _kvdbRnCfg;
    unsigned long _snapId = 0;

    bool _kvdbPerUt = true;

    KVDBImpl _db{};
    bool _dbClosed = true;
};
}
