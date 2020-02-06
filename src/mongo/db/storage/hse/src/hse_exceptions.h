/**
 * TODO: HSE License
 */
#pragma once

#include <stdexcept>
#include <string>

using namespace std;

namespace hse {

class KVDBException : public runtime_error {
public:
    explicit KVDBException(const string& what_arg) : runtime_error(what_arg) {}
};

class KVDBNotImplementedError : public KVDBException {
public:
    KVDBNotImplementedError() : KVDBException("Not Implemented") {}
    KVDBNotImplementedError(const string& what_arg) : KVDBException(what_arg) {}
};

class KVDBNoMemError : public KVDBException {
public:
    KVDBNoMemError() : KVDBException("Not Enough Memory") {}
};
}
