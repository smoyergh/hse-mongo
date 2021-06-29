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
}  // namespace hse
