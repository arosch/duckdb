#pragma once

#include <list>

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

    class PhysicalQualifyingFilterOperatorState;
//! PhysicalQualifyingFilter represents a specialized filter operator for process data (case | activity | timestamp).
//! It removes tuples from the result if the case does not qualify. It qualifies if any of the timestamps > x
//! Does not physically remove, only sets selection vector
    class PhysicalQualifyingFilter : public PhysicalOperator {
    public:
        PhysicalQualifyingFilter(LogicalOperator &op, vector<unique_ptr<Expression>> select_list)
                : PhysicalOperator(PhysicalOperatorType::QUALIFYING_FILTER, op.types), expressions(std::move(select_list)) {
        }
        //! Contains the values to determine qualification
        vector<unique_ptr<Expression>> expressions;

        void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
        unique_ptr<PhysicalOperatorState> GetOperatorState() override;

    private:
        //! Splits the child chunk into respective data for processing.
        bool Qualify(DataChunk &chunk, PhysicalQualifyingFilterOperatorState *state);
    };

    class PhysicalQualifyingFilterOperatorState : public PhysicalOperatorState {
    public:
        PhysicalQualifyingFilterOperatorState(PhysicalQualifyingFilter *parent, PhysicalOperator *child);

        //! Indicator whether it is the first child chunk processed
        bool firstchild;

	    bool hasBufferedSameGrpChunks() const {
		    return !same_grp_buffer.empty();
	    }
        //! Qualifies the child chunk and updates the previous selection buffer if necessary
        bool QualifyingGreaterThan(DataChunk &chunk, const Vector &left, const Vector &right, const Vector &groups);
        //! Get the data for the next buffered child chunk. Also sets the selection vector.
        void RetrieveSameGrpChunk(DataChunk &chunk);
        //! Set the chunk to the current selection buffer and clear the buffer
        void SetSelectionVector(DataChunk &chunk);

    private:
        //! Buffered selection vectors for buffered child chunks
        Vector selection_buffer;
        //! Buffered child chunks that contain a single grp
        std::list<DataChunk> same_grp_buffer;
        //! Stores the last grp of the child chunk
        int32_t last_grp;
        //! Stores the front index of the child chunk
        index_t last_grp_begin = 0;
        //! The accumulated qualifier for last grp (overlap candidate)
        bool overlap_qualifier;

        //! Buffer the child chunk differently if it is all the same grp. Might have to buffer more than one.
        void BufferChildChunkSameGrp();
    };
} // namespace duckdb
