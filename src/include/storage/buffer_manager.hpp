//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/buffer_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/buffer/buffer_handle.hpp"
#include "storage/buffer/buffer_list.hpp"
#include "storage/buffer/managed_buffer.hpp"
#include "storage/block_manager.hpp"
#include "common/file_system.hpp"
#include "common/unordered_map.hpp"

#include <mutex>

namespace duckdb {

//! The buffer manager is a
class BufferManager {
	friend class BufferHandle;
public:
	BufferManager(FileSystem &fs, BlockManager &manager, string temp_directory, index_t maximum_memory);
	~BufferManager();

	//! Pin a block id, returning a block handle holding a pointer to the block
	unique_ptr<BlockHandle> Pin(block_id_t block);

	//! Allocate a buffer of arbitrary size, as long as it is >= BLOCK_SIZE. can_destroy signifies whether or not the buffer can be freely destroyed when unpinned, or whether or not it needs to be written to a temporary file so it can be reloaded.
	unique_ptr<ManagedBufferHandle> Allocate(index_t alloc_size, bool can_destroy = false);
	//! Pin a managed buffer handle, returning the buffer handle or nullptr if the buffer handle could not be found (because it was destroyed)
	unique_ptr<ManagedBufferHandle> PinBuffer(block_id_t buffer_id, bool can_destroy = false);
	//! Destroy the managed buffer with the specified buffer_id, freeing its memory
	void DestroyBuffer(block_id_t buffer_id);
private:
	//! Unpin a block id, decreasing its reference count and potentially allowing it to be freed.
	void Unpin(block_id_t block);

	//! Evict the least recently used block from the buffer manager, or throws an exception if there are no blocks available to evict
	unique_ptr<Block> EvictBlock();

	//! Add a reference to the refcount of a buffer entry
	void AddReference(BufferEntry *entry);

	//! Write a temporary buffer to disk
	void WriteTemporaryBuffer(ManagedBuffer &buffer);
	//! Read a temporary buffer from disk
	unique_ptr<ManagedBufferHandle> ReadTemporaryBuffer(block_id_t id);
	//! Get the path of the temporary buffer
	string GetTemporaryPath(block_id_t id);
private:
	FileSystem &fs;
	//! The block manager
	BlockManager &manager;
	//! The current amount of memory that is occupied by the buffer manager (in bytes)
	index_t current_memory;
	//! The maximum amount of memory that the buffer manager can keep (in bytes)
	index_t maximum_memory;
	//! The directory name where temporary files are stored
	string temp_directory;
	//! The lock for the set of blocks
	std::mutex block_lock;
	//! A mapping of block id -> BufferEntry
	unordered_map<block_id_t, BufferEntry*> blocks;
	//! A linked list of buffer entries that are in use
	BufferList used_list;
	//! LRU list of unused blocks
	BufferList lru;
	//! The temporary id used for managed buffers
	block_id_t temporary_id;

};
} // namespace duckdb