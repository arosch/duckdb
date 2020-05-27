#include "duckdb/common/types/string_heap.hpp"

#include "duckdb/common/exception.hpp"
#include <cstring>
#include <iostream>

using namespace duckdb;
using namespace std;

#define MINIMUM_HEAP_SIZE 4096

StringHeap::StringHeap() : tail(nullptr) {
}

const char *StringHeap::AddString(const char *data, index_t len) {
#ifdef DEBUG
	if (!Value::IsUTF8String(data)) {
		throw Exception("String value is not valid UTF8");
	}
#endif
	if (!chunk || chunk->current_position + len >= chunk->maximum_size) {
		// have to make a new entry
		auto new_chunk = make_unique<StringChunk>(std::max(len + 1, (index_t)MINIMUM_HEAP_SIZE));
		new_chunk->prev = move(chunk);
		chunk = move(new_chunk);
		if (!tail) {
			tail = chunk.get();
		}
	}
	auto insert_pos = chunk->data.get() + chunk->current_position;
	strcpy(insert_pos, data);
	chunk->current_position += len + 1;
	return insert_pos;
}

const char *StringHeap::AddString(const char *data) {
	return AddString(data, strlen(data));
}

const char *StringHeap::AddString(const string &data) {
	return AddString(data.c_str(), data.size());
}

int32_t *StringHeap::AddIntegerarray(const int32_t *data, index_t len) {
    // space for length
    len++;
    Resize(len * sizeof(int32_t));
    auto insert_pos = chunk->data.get() + chunk->current_position;
    memcpy(insert_pos, data, len * sizeof(int32_t));
    chunk->current_position += len * sizeof(int32_t);
    return reinterpret_cast<int32_t *>(insert_pos);
}

int32_t *StringHeap::AddInts(const int32_t *data, int32_t num) {
    Resize((num + 1) * sizeof(int32_t));
    auto insert_pos = chunk->data.get() + chunk->current_position;

    // serialize number of ints
    memcpy(insert_pos, &num, sizeof(int32_t));
    // serialize the ints
    memcpy(insert_pos + sizeof(int32_t), data, num * sizeof(int32_t));

    chunk->current_position += (num + 1) * sizeof(int32_t);
    return reinterpret_cast<int32_t *>(insert_pos);
}

int32_t *StringHeap::AddInts(const int32_t *serialized_data, const int32_t *data, int32_t num) {
    // deserialize number of existing ints
    int32_t old_num = *serialized_data;
    memcpy(&old_num, serialized_data, sizeof(int32_t));

    const auto total = old_num + num;
    Resize((total + 1) * sizeof(int32_t));
    auto insert_pos = chunk->data.get() + chunk->current_position;

    // serialize number of ints
    const auto nbytes_total =  sizeof(int32_t);
    memcpy(insert_pos, &total, nbytes_total);
    insert_pos += nbytes_total;

    // add the previously serialized ints
    const auto nbytes_old =  old_num * sizeof(int32_t);
    memcpy(insert_pos, serialized_data + 1, nbytes_old);
    insert_pos += nbytes_old;

    // serialize the new ints
    const auto nbytes_new =  num * sizeof(int32_t);
    memcpy(insert_pos, data, nbytes_new);

    chunk->current_position += nbytes_total + nbytes_old + nbytes_new;

    // revert position pointer to point to beginning
    insert_pos -= (nbytes_total + nbytes_old);
    return reinterpret_cast<int32_t *>(insert_pos);
}

void StringHeap::MergeHeap(StringHeap &other) {
	if (!other.tail) {
		return;
	}
	other.tail->prev = move(chunk);
	this->chunk = move(other.chunk);
	if (!tail) {
		tail = this->chunk.get();
	}
	other.tail = nullptr;
}

void StringHeap::Resize(size_t size){
    if (!chunk || chunk->current_position + size >= chunk->maximum_size) {
        // have to make a new entry
        auto new_chunk = make_unique<StringChunk>(std::max(size + 1, (index_t)MINIMUM_HEAP_SIZE));
        new_chunk->prev = move(chunk);
        chunk = move(new_chunk);
        if (!tail) {
            tail = chunk.get();
        }
    }
}