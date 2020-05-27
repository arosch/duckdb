//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/null_value.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

#include <cstring>

namespace duckdb {

//! This is no longer used in regular vectors, however, hash tables use this
//! value to store a NULL
template <class T> inline T NullValue() {
	return std::numeric_limits<T>::min();
}

constexpr const char str_nil[2] = {'\200', '\0'};

template <> inline const char *NullValue() {
	assert(str_nil[0] == '\200' && str_nil[1] == '\0');
	return str_nil;
}

template <> inline char *NullValue() {
	return (char *)NullValue<const char *>();
}

constexpr const unsigned char ustr_nil[1] = {'0'};

template <> inline const unsigned char *NullValue() {
    return ustr_nil;
}

template <> inline unsigned char *NullValue() {
    return (unsigned char *)NullValue<const unsigned char *>();
}



template <class T> inline bool IsNullValue(T value) {
	return value == NullValue<T>();
}

template <> inline bool IsNullValue(const char *value) {
	return *value == str_nil[0];
	// return std::strcmp(value, NullValue<const char *>()) == 0;
}

template <> inline bool IsNullValue(char *value) {
	return IsNullValue<const char *>(value);
}

template <> inline bool IsNullValue(const int32_t *value) {
    return *value == 0;
}

template <> inline bool IsNullValue(int32_t *value) {
    return IsNullValue<const int32_t *>(value);
}

inline bool IsNullValueSHA(const sha_t value) {
    sha_t nulls;
    memset(nulls, ustr_nil[0], sizeof(sha_t));
    return  memcmp(value, nulls, sizeof(sha_t)) == 0 ;
}

//! Compares a specific memory region against the types NULL value
bool IsNullValue(data_ptr_t ptr, TypeId type);

//! Writes NullValue<T> value of a specific type to a memory address
void SetNullValue(data_ptr_t ptr, TypeId type);

} // namespace duckdb
