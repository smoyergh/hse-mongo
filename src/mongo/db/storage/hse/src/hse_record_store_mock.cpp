/**
 * TODO: HSE License
 */

#include "mongo/platform/basic.h"

#include "mongo/base/init.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/service_context.h"
#include "mongo/db/service_context_noop.h"
#include "mongo/stdx/memory.h"

#include "hse_engine.h"

namespace mongo {

// static
bool KVDBEngine::initOplogStoreThread(StringData ns) {
    return NamespaceString::oplog(ns);
}

MONGO_INITIALIZER(SetGlobalEnvironment)(InitializerContext* context) {
    setGlobalServiceContext(stdx::make_unique<ServiceContextNoop>());
    return Status::OK();
}

}  // namespace mongo
