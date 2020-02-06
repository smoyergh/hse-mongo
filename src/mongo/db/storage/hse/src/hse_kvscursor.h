/**
 * TODO: HSE License
 */
#pragma once

#include "hse_clienttxn.h"
#include "hse_util.h"

#include <mutex>
#include <set>

using namespace std;

#ifdef __cplusplus
extern "C" {
#endif

struct hse_kvs_cursor;

#ifdef __cplusplus
}
#endif


// KVDB interface
namespace hse {

class KvsCursor;

KvsCursor* create_cursor(KVSHandle kvs,
                         KVDBData& prefix,
                         bool forward,
                         const struct CompParms& compparms,
                         ClientTxn* lnkd_txn = 0,
                         bool enableRa = false);

class KvsCursor {
public:
    KvsCursor(KVSHandle kvs,
              KVDBData& prefix,
              bool forward,
              ClientTxn* lnkd_txn,
              const struct CompParms& compparms,
              bool enableRa = false);

    virtual ~KvsCursor();

    virtual ClientTxn* get_txn() {
        return _lnkd_txn;
    }

    virtual Status update(ClientTxn* lnkd_txn = 0);

    virtual Status seek(const KVDBData& key, const KVDBData* kmax, KVDBData* posKey);

    virtual Status read(KVDBData& key, KVDBData& val, bool& eof);

    virtual Status save();

    virtual Status restore();

protected:
    int _read_kvs();
    bool _kvs_is_next() {
        return true;
    }
    bool _is_eof() {
        return _kvs_eof;
    }

    struct hse_kvs* _kvs;  // not owned
    KVDBData _pfx;
    bool _forward{true};
    bool _enableRa{false};
    ClientTxn* _lnkd_txn;  // not owned
    std::mutex _mutex;
    bool _is_ready;

    struct hse_kvs_cursor* _cursor;
    int _start;
    int _end;
    int _curr;

    bool _kvs_stale;
    const void* _kvs_key;
    size_t _kvs_klen;

    const void* _kvs_val;
    // Because we are in the context of a cursor if the value is multi chunks,
    // _kvs_val applies to the first chunk only.
    //
    // If the value was not compressed, first byte of the first chunk stored in kvs,
    // may be the 4 bytes length header if multichunk or the first byte user data if one
    // chunk. _kvs_vlen is the length of the first chunk.
    //
    // If the value was compressed and was multichunks, points on the first byte of
    // the first chunk stored in kvs (aka points 4 bytes length header followed by
    // compression headers and then compressed value.
    // kvs_len is the length of that first chunk (containing headers and
    // compressed value first chunk).
    //
    // If the value was compressed and < 1 chunk, _kvs_val and _kvs_vlen do NOT
    // reflect the value as stored in kvs. _kvs_val points on the first
    // byte of user data (decompressed). _kvs_vlen is the length of the value
    // uncompressed.

    size_t _kvs_vlen;
    unique_ptr<unsigned char[]> _uncompressed_val{};
    // Used to free automatically the buffer allocated to hold the de-compression output.
    // If the compression is not used, it doesn't point on any buffer.
    // If compression is used, it points on the same buffer as _kvs_val point to.

    bool _kvs_eof;
    unsigned int _kvs_num_chunks;
    unsigned int
        _offset;  // Offset of the first byte of the user value [un compressed] in the final
                  // buffer passed to mongo.
    unsigned long
        _total_len;  // Always the length of the whole user value [un compressed]. If the value
                     // was chunked, that includes all the chunks.

    unsigned long _kvs_total_len_comp;
    // Length of the compressed whole value. It is also the length of the value as stored
    // in kvs. Does not contain the 4 bytes length header but contains the compression
    // headers. If compression is off, it is the same as _total_len.

    struct CompParms _compparms;

    bool _eof;
    int _next_ret;
};
}
