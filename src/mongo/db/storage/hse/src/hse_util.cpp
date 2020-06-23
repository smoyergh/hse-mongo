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

#include "hse_util.h"
#include "hse.h"
#include "hse_oplog_block.h"
#include "hse_recovery_unit.h"

#include "lz4.h"
#include <string>

#include "mongo/util/log.h"

using hse::VALUE_META_SIZE;

namespace hse {

mongo::Status hseToMongoStatus_slow(const Status& status, const char* prefix) {
    if (status.ok()) {
        return mongo::Status::OK();
    }

    return mongo::Status(mongo::ErrorCodes::InternalError, status.toString());
}

hse::Status _cursorRead(mongo::KVDBRecoveryUnit* ru,
                        shared_ptr<mongo::KVDBOplogBlockManager> opBlkMgr,
                        KvsCursor* cursor,
                        KVDBData& key,
                        KVDBData& val,
                        bool& eof) {
    if (opBlkMgr) {
        return opBlkMgr->cursorRead(ru, cursor, key, val, eof);
    } else {
        return ru->cursorRead(cursor, key, val, eof);
    }
}

hse::Status encodeLeb128(uint64_t val,
                         uint8_t* out_buf,
                         uint32_t outbuf_len,
                         uint32_t* codedlen_bytes) {
    uint8_t byte;

    *codedlen_bytes = 0;
    do {
        if (*codedlen_bytes >= outbuf_len) {
            mongo::log() << "encodeLeb128: failed";
            return Status{ENOSPC};
        }
        byte = val & 0x7F;
        val >>= 7;
        if (val) {
            byte |= 0x80;
        }
        out_buf[(*codedlen_bytes)++] = byte;
    } while (val);
    return Status{};
}

hse::Status decodeLeb128(const uint8_t* buf,
                         int max_encoded_bytes,
                         uint64_t* val,
                         uint32_t* codedlen_bytes) {
    int i;
    int shift;
    uint8_t byte;

    *codedlen_bytes = 0;
    for (*val = 0, i = 0, shift = 0; i < max_encoded_bytes; i++, shift += 7) {
        byte = buf[i];
        *val |= ((uint64_t)byte & 0x7F) << shift;
        if (!(byte & 0x80)) {
            *codedlen_bytes = i + 1;
            return Status{};
        }
    }
    mongo::log() << "decodeLeb128: failed";
    return Status{EINVAL};
}

// Used when getting a value from KVS.
// Compute the total (across chunks) length of the value (uncompressed), the offset this data will
// be in
// buffer passed to mongodb, the number of chunks.
//
// Note: assuming the recordstore is compressed, when the compression is done,
// the compression of the user value (all of it) is done first,
// then the chunking is done. In other words the chunking applies to/on compressed data.
//
// The input "buf" is the start of the buffer holding the first chunk.
// It is the buffer coming from KVS.
// If the recordstore is compressed, the buffer should contain the compressed data (aka data
// coming from kvs). Note that de-compression removes the length fields at the beginning of the
// buffer.
// The input "len" is the length of the value (first chunk) as returned by kvs.
//
// Format of buf:
//   If not compressed and small value (aka "len" < VALUE_META_THRESHOLD_LEN)
//   | user data whose length is "len" |
//   If not compressed and large value (aka "len" >= VALUE_META_THRESHOLD_LEN)
//   | VALUE_META_SIZE bytes=total length user data |
//   | user data in first chunk (VALUE_META_THRESHOLD_LEN of it) |
//
//   Below we assume the recordstore is compressed.
//
//   If total user data length  (uncompressed) <= compminsize
//   | 1byte=comp algo=ALGO_NONE | user data uncompressed, ("len" -  1) bytes of it |
//
//   Below we also assume that the total size of the user data (uncompressed) is > compminsize
//   Let be "leb128" the leb128 bytes (1 to 5 bytes) used to encode the length of the
//   total user data uncompressed.
//
//   If size of compressed total user data is < VALUE_META_THRESHOLD_LEN - 1 - size(leb128)
//   | 1byte=comp algo!=ALGO_NONE | leb128 bytes |
//   | user data compressed, ("len" -  1 - size(leb128) bytes of it |
//   If size of compressed total user data is >= VALUE_META_THRESHOLD_LEN - 1 - size(leb128)
//   | 1byte=comp algo!=ALGO_NONE | VALUE_META_SIZE bytes=length of compressed total user data |
//   leb128 by |
//   | user data compressed, (VALUE_META_THRESHOLD_LEN -  1 - VALUE_META_SIZE - size(leb128) bytes
//   of it |
//
void computeFraming(const uint8_t* buf,
                    unsigned long len,
                    const CompParms& compparms,
                    unsigned long* totallen,
                    unsigned long* totallencomp,
                    unsigned int* num_chunks,
                    unsigned int* offset,      // Offset where the user data
                                               // will be placed in the final
                                               // buffer passed to mongo.
                    unsigned int* off_comp) {  // offset in "buf" of the first byte
                                               // of the compression header.

    KVDBData val{buf, len};
    const uint8_t* buf1;
    unsigned long len1;

    *totallencomp = _getValueLength(val);
    *num_chunks = _getNumChunks(*totallencomp);
    *off_comp = 0;

    *off_comp = _getValueOffset(val);
    buf1 = buf + *off_comp;  // advance buf past VALUE_META_SIZE
    len1 = len - (buf1 - buf);

    if (compparms.compdoit) {
        // compressed value.

        enum CompAlgo compalgo = (enum CompAlgo)(*buf1);

        buf1++;  // advance past the algo byte
        len1--;

        *offset = 0;  // de-compression will place first byte user data at beginning of buffer.

        if (compalgo == ALGO_NONE) {
            *totallen = len1;
        } else {
            unsigned int codedlen_bytes;

            auto hseSt = hse::decodeLeb128(
                buf1, std::min(MAX_BYTES_LEB128, len1), totallen, &codedlen_bytes);
            invariantHse(hseSt.ok());
        }
    } else {
        *offset = buf1 - buf;
        *totallen = *totallencomp;
    }
}

void updateFraming(const CompParms& compparms, KVDBData& val) {
    unsigned long total_len;
    unsigned long total_len_comp;
    unsigned int num_chunks;
    unsigned int offset;
    unsigned int off_comp;

    computeFraming(val.data(),
                   val.len(),
                   compparms,
                   &total_len,
                   &total_len_comp,
                   &num_chunks,
                   &offset,
                   &off_comp);
    val.setFraming(total_len, total_len_comp, num_chunks, offset);
}

// Determine if this recordstore needs to be compressed or not. And record it in compdoit.
// Compressing the oplog is not yet implemented.
void compressneeded(bool oplog, CompParms& compparms) {

    compparms.compdoit = false;
    if ((compparms.compalgo != CompAlgo::ALGO_NONE) && !oplog)
        compparms.compdoit = true;
}

/**
 * compressdata()
 * If the size of data is smaller <= compminsize, the data is not compressed
 * but copied.
 * If the size of the data is > compminsize the data is compressed with
 * LZ4. The maximum size is LZ4_MAX_INPUT_SIZE.
 * The output buffer contains one byte indicating if the data is compressed
 * or not (and if yes, what algorithm was used). If the data is compressed
 * this first byte is followed by the length of the uncompressed data (in_len)
 * encoded with leb128. Then follows the data passed in (in_data) compressed .
 */
hse::Status compressdata(const struct CompParms& compparms,
                         const char* in_data,
                         const int in_len,
                         KVDBData& comp) {
    int comp_max;
    uint32_t leb128_bytes;
    int out_buf_len;

    // Only LZ4 supported for now.
    if (compparms.compalgo != ALGO_LZ4) {
        mongo::log() << "compressdata: invalid compression algo "
                     << static_cast<std::underlying_type<CompAlgo>::type>(compparms.compalgo);
        return hse::Status{EINVAL};
    }

    if (in_len <= (int)compparms.compminsize) {
        out_buf_len = 1 + in_len;
    } else {
        if (in_len > LZ4_MAX_INPUT_SIZE) {
            mongo::log() << "compressdata: size to compress too large " << in_len;
            return hse::Status{EINVAL};
        }

        // Get the worst case size of the compressed output add one byte to hold
        // the compression algo and allocate memory for the output buffer.
        comp_max = LZ4_compressBound(in_len);
        out_buf_len = 1 + MAX_BYTES_LEB128 + comp_max;
    }

    comp.createOwned(out_buf_len);

    if (in_len <= (int)compparms.compminsize) {
        *(comp.data()) = ALGO_NONE;

        memcpy(comp.data() + 1, in_data, in_len);
        comp.adjustLen(1 + in_len);
    } else {
        *(comp.data()) = compparms.compalgo;

        // Copy the encoded(LEB128) length of the uncompressed data at the
        // beginning of the output buffer after the first byte.
        auto hseSt = hse::encodeLeb128(in_len, comp.data() + 1, MAX_BYTES_LEB128, &leb128_bytes);
        invariantHse(hseSt.ok());

        // Place the compressed data just after the encoded length.
        auto comp_out = LZ4_compress_default(
            (const char*)in_data, (char*)comp.data() + 1 + leb128_bytes, in_len, comp_max);
        if (!comp_out) {
            mongo::log() << "compressdata: LZ4_compress_default() failed";
            invariantHse(comp_out);
        }

        comp.adjustLen(1 + leb128_bytes + comp_out);
    }


    return hse::Status{};
}

/**
 * decompressdata() decompresses data.
 * The first byte of "comp.data() + off_comp" must be the algo byte.
 */
hse::Status decompressdata(const struct CompParms& compparms,
                           KVDBData& comp,
                           unsigned int off_comp,
                           KVDBData& unc) {
    uint64_t unc_len;
    uint32_t codedlen_bytes;
    int comp_read;
    CompAlgo compalgo;
    uint8_t* algo_byte = comp.data() + off_comp;
    uint64_t hdr_comp_len = comp.len() - off_comp;  // compression headers +
                                                    // compressed data

    // Get the algorithm used to compress.
    compalgo = (enum CompAlgo) * (algo_byte);

    // Get the size of the uncompressed data.
    if (compalgo == ALGO_NONE) {
        // must have been a small size value
        unc_len = hdr_comp_len - 1;  // remove the algo byte.
    } else {
        invariantHse(compalgo == ALGO_LZ4);
        auto hseSt = hse::decodeLeb128(
            algo_byte + 1, std::min(MAX_BYTES_LEB128, hdr_comp_len), &unc_len, &codedlen_bytes);
        invariantHse(hseSt.ok());
    }

    // Allocate a buffer to hold the uncompressed data.
    unc.createOwned(unc_len);


    if (compalgo == ALGO_NONE) {
        // copy
        memcpy(unc.data(), algo_byte + 1, unc_len);
    } else {
        // Decompress.
        comp_read = LZ4_decompress_fast(
            (const char*)algo_byte + 1 + codedlen_bytes, (char*)unc.data(), unc_len);
        if ((uint64_t)comp_read != hdr_comp_len - 1 - codedlen_bytes) {
            mongo::log() << "decompressdata: failed" << comp_read << hdr_comp_len << codedlen_bytes;
            return hse::Status{EINVAL};
        }
    }

    unc.adjustLen(unc_len);

    return hse::Status{};
}

/**
 * decompressdata1() decompresses data.
 * Same thing as decompressdata() but pointers parameters instead of KVDBData.
 */
hse::Status decompressdata1(const struct CompParms& compparms,
                            const void* comp_buf,
                            size_t comp_len,
                            unsigned int off_comp,
                            void** unc_buf,
                            size_t* unc_len) {
    uint32_t codedlen_bytes;
    int comp_read;
    CompAlgo compalgo;
    uint8_t* algo_byte = (uint8_t*)comp_buf + off_comp;
    uint64_t hdr_comp_len = comp_len - off_comp;  // compression headers +

    // Get the algorithm used to compress.
    compalgo = (enum CompAlgo) * (algo_byte);

    // Get the size of the uncompressed data.
    if (compalgo == ALGO_NONE) {
        // must have been a small size value
        *unc_len = hdr_comp_len - 1;  // remove the algo byte.
    } else {
        invariantHse(compalgo == ALGO_LZ4);
        auto hseSt = hse::decodeLeb128(
            algo_byte + 1, std::min(MAX_BYTES_LEB128, hdr_comp_len), unc_len, &codedlen_bytes);
        invariantHse(hseSt.ok());
    }

    // Allocate a buffer to hold the uncompressed data.
    *unc_buf = new uint8_t[*unc_len];

    // Decompress.
    if (compalgo == ALGO_NONE) {
        // copy
        memcpy(*unc_buf, algo_byte + 1, *unc_len);
    } else {
        // Decompress
        comp_read = LZ4_decompress_fast(
            (const char*)algo_byte + 1 + codedlen_bytes, (char*)*unc_buf, *unc_len);
        if ((uint64_t)comp_read != hdr_comp_len - 1 - codedlen_bytes) {
            mongo::log() << "decompressdata1: failed" << comp_read << hdr_comp_len
                         << codedlen_bytes;
            free(*unc_buf);
            return hse::Status{EINVAL};
        }
    }

    return hse::Status{};
}

}  // namespace hse
