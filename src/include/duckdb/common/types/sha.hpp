//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/sha.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

#include <openssl/sha.h>

namespace duckdb {

using sha_t = unsigned char[SHA_DIGEST_LENGTH];

//! The Sha class is a static class that holds helper functions for the Sha type.
class Sha {
public:
    //! Convert a sha object to a string
    static string ToString(const sha_t value);

    //! Convert an unsigned char array to printable hex string.
    static void UCharToHexStringstream(const unsigned char * md, std::string & result, size_t size);
};

} // namespace duckdb