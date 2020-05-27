#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/exception.hpp"

#include <functional>

using namespace std;

namespace duckdb {

template <> uint64_t Hash(uint64_t val) {
	return murmurhash64(val);
}

template <> uint64_t Hash(int64_t val) {
	return murmurhash64((uint64_t)val);
}

template <> uint64_t Hash(float val) {
	return std::hash<float>{}(val);
}

template <> uint64_t Hash(double val) {
	return std::hash<double>{}(val);
}

template <> uint64_t Hash(const char *str) {
	uint64_t hash = 5381;
	uint64_t c;

	while ((c = *str++)) {
		hash = ((hash << 5u) + hash) + c;
	}

	return hash;
}

template <> uint64_t Hash(char *val) {
	return Hash<const char *>(val);
}

template <> uint64_t Hash(const unsigned char *sha) {
    uint64_t c;
    // todo: is the first 8 bytes enough?
    memcpy(&c, sha, sizeof(uint64_t));
    return c;
}

template <> uint64_t Hash(unsigned char *val) {
    return Hash<const unsigned char *>(val);
}

template <> uint64_t Hash(const int32_t *val) {
    int32_t size = *val;
    return Hash(val+1, size);
}

template <> uint64_t Hash(int32_t *val) {
    return Hash<const int32_t *>(val);
}

uint64_t Hash(const char *val, size_t size) {
	uint64_t hash = 5381;

	for (size_t i = 0; i < size; i++) {
		hash = ((hash << 5u) + hash) + val[i];
	}

	return hash;
}

uint64_t Hash(char *val, size_t size) {
	return Hash((const char *)val, size);
}

uint64_t Hash(const unsigned char *val, size_t size) {
    uint64_t hash = 5381;

    for (size_t i = 0; i < size; i++) {
        hash = ((hash << 5u) + hash) + val[i];
    }

    return hash;
}

uint64_t Hash(uint8_t *val, size_t size) {
	return Hash((const char *)val, size);
}

uint64_t Hash(const int32_t *val, size_t size) {
    assert(size > 0);
    // check for valid length
    assert(size < 10000);
    auto values = (const unsigned char *) val;
    uint64_t hash = 5381;

    for (size_t i = 0; i < size * sizeof(int32_t); i++) {
        hash = ((hash << 5u) + hash) + values[i];
    }

    return hash;
}

} // namespace duckdb
