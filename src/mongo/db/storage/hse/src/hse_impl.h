/**
 * TODO: HSE License
 */
#pragma once

#include "hse.h"
#include "hse_clienttxn.h"

using namespace std;

// KVDB interface
namespace hse {

// KVDB Implementation
// TODO: HSE
// Consider protecting handle
class KVDBImpl : public KVDB {
public:
    virtual Status kvdb_init();

    virtual Status kvdb_fini();

    virtual Status kvdb_make(const char* mp_name, const char* kvdb_name, struct hse_params* params);

    virtual Status kvdb_open(const char* mp_name,
                             const char* kvdb_name,
                             struct hse_params* params,
                             unsigned long snapshot_id);

    virtual Status kvdb_kvs_open(const char* kvs_name,
                                 struct hse_params* params,
                                 KVSHandle& kvs_out);

    virtual Status kvdb_kvs_close(KVSHandle handle);

    virtual struct hse_kvdb* kvdb_handle() {
        return _handle;
    }

    virtual Status kvdb_kvs_count(unsigned int* count);

    virtual Status kvdb_get_names(unsigned int* count, char*** kvs_list);

    virtual Status kvdb_free_names(char** kvsv);

    virtual Status kvdb_kvs_make(const char* kvs_name, struct hse_params* params);

    virtual Status kvdb_kvs_drop(const char* kvs_name);

    virtual Status kvdb_close();

    virtual Status kvs_put(KVSHandle handle,
                           ClientTxn* txn,
                           const KVDBData& key,
                           const KVDBData& val);

    virtual Status kvs_put(KVSHandle handle, const KVDBData& key, const KVDBData& val);

    virtual Status kvs_get(
        KVSHandle handle, ClientTxn* txn, const KVDBData& key, KVDBData& val, bool& found);

    virtual Status kvs_probe_key(KVSHandle handle,
                                 ClientTxn* txn,
                                 const KVDBData& key,
                                 bool& found);

    virtual Status kvs_delete(KVSHandle handle, ClientTxn* txn, const KVDBData& key);

    virtual Status kvs_prefix_probe(KVSHandle handle,
                                    ClientTxn* txn,
                                    const KVDBData& prefix,
                                    KVDBData& key,
                                    KVDBData& val,
                                    hse_kvs_pfx_probe_cnt& found);

    virtual Status kvs_probe_len(
        KVSHandle handle, ClientTxn* txn, const KVDBData& key, KVDBData& val, bool& found);

    virtual Status kvs_prefix_delete(KVSHandle handle, ClientTxn* txn, const KVDBData& prefix);

    virtual Status kvs_iter_delete(KVSHandle handle, ClientTxn* txn, const KVDBData& prefix);

    virtual Status kvdb_sync();

    virtual Status kvdb_cparams_parse(int argc,
                                      char** argv,
                                      struct hse_params* params,
                                      int* next_arg);

    virtual Status kvdb_rparams_parse(int argc,
                                      char** argv,
                                      struct hse_params* params,
                                      int* next_arg);

    virtual Status kvs_cparams_parse(int argc,
                                     char** argv,
                                     struct hse_params* params,
                                     int* next_arg);

    virtual Status kvs_rparams_parse(int argc,
                                     char** argv,
                                     struct hse_params* params,
                                     int* next_arg);

    virtual Status kvdb_get_c1_info(struct ikvdb_c1_info* info);

private:
    struct hse_kvdb* _handle = nullptr;
};
}
