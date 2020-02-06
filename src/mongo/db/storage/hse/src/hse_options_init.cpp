/**
 * TBD: HSE license
 */


#include <iostream>

#include "mongo/util/exit_code.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

#include "hse_global_options.h"

namespace mongo {

MONGO_MODULE_STARTUP_OPTIONS_REGISTER(KVDBOptions)(InitializerContext* context) {
    return kvdbGlobalOptions.add(&moe::startupOptions);
}

MONGO_STARTUP_OPTIONS_VALIDATE(KVDBOptions)(InitializerContext* context) {
    return Status::OK();
}

MONGO_STARTUP_OPTIONS_STORE(KVDBOptions)(InitializerContext* context) {
    Status ret = kvdbGlobalOptions.store(moe::startupOptionsParsed, context->args());
    if (!ret.isOK()) {
        std::cerr << ret.toString() << std::endl;
        std::cerr << "try '" << context->args()[0] << " --help' for more information" << std::endl;
        ::_exit(EXIT_BADOPTIONS);
    }
    return Status::OK();
}
}
