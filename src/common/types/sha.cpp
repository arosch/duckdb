#include "duckdb/common/types/sha.hpp"

#include "duckdb/common/exception.hpp"

#include <iomanip>
#include <iostream>
#include <sstream>
#include <duckdb/common/printer.hpp>

using namespace duckdb;
using namespace std;

string Sha::ToString(const sha_t value){
    std::string buffer;
    UCharToHexStringstream(value, buffer, SHA_DIGEST_LENGTH);
    return buffer;
}

void Sha::UCharToHexStringstream(const unsigned char * md, std::string & result, size_t size) {
    stringstream ss;
    ss << "0x" << std::hex;
    for(auto i=0u; i<size; i++)
        ss << std::setw(2) << std::setfill('0') << (int) md[i];
    result = ss.str();
}