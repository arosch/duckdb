#include "storage/buffer/managed_buffer.hpp"
#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

ManagedBuffer::ManagedBuffer(BufferManager &manager, index_t size, bool can_destroy, block_id_t id) :
	Buffer(BufferType::MANAGED_BUFFER), manager(manager), size(size), can_destroy(can_destroy), id(id) {
	assert(id >= MAXIMUM_BLOCK);
	assert(size >= BLOCK_SIZE);

	data = unique_ptr<data_t[]>(new data_t[size]);
}