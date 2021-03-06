//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/table/persistent_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/uncompressed_segment.hpp"

namespace duckdb {

class PersistentSegment : public ColumnSegment {
public:
	PersistentSegment(BufferManager &manager, block_id_t id, index_t offset, TypeId type, index_t start, index_t count);

	//! The buffer manager
	BufferManager &manager;
	//! The block id that this segment relates to
	block_id_t block_id;
	//! The offset into the block
	index_t offset;
	//! The uncompressed segment that the data of the persistent segment is loaded into
	unique_ptr<UncompressedSegment> data;

public:
	void InitializeScan(ColumnScanState &state) override;
	//! Scan one vector from this transient segment
	void Scan(Transaction &transaction, ColumnScanState &state, index_t vector_index, Vector &result) override;
	//! Scan one vector from this transient segment, throwing an exception if there are any outstanding updates
	void IndexScan(ColumnScanState &state, Vector &result) override;

	//! Fetch the base table vector index that belongs to this row
	void Fetch(ColumnScanState &state, index_t vector_index, Vector &result) override;
	//! Fetch a value of the specific row id and append it to the result
	void FetchRow(ColumnFetchState &state, Transaction &transaction, row_t row_id, Vector &result) override;

	//! Perform an update within the segment
	void Update(ColumnData &column_data, Transaction &transaction, Vector &updates, row_t *ids) override;
};

} // namespace duckdb
