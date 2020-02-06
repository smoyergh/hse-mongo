/**
 * TODO: HSE License
 */
#pragma once

#include "hse.h"

#include <deque>
#include <mutex>
#include <vector>

using namespace std;

extern "C" {

struct hse_kvdb;
struct hse_kvdb_txn;
}

// KVDB interface
namespace hse {

class TxnCache {
public:
    ~TxnCache();

    void set_kvdb(hse_kvdb* kvdb);

    void release();

    hse_kvdb_txn* alloc();

    void free(hse_kvdb_txn* txn);

private:
    hse_kvdb* _kvdb{0};
};

extern TxnCache* g_txn_cache;

class ClientTxn {
public:
    ClientTxn(struct hse_kvdb* kvdb);

    virtual ~ClientTxn();

    Status begin();

    Status commit();

    Status abort();

    struct hse_kvdb_txn* get_kvdb_txn() {
        return _txn;
    }

private:
    static TxnCache _txn_cache;
    struct hse_kvdb* _kvdb;
    struct hse_kvdb_txn* _txn;
};
}
