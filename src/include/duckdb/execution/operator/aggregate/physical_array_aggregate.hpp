#pragma once

#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

    class PhysicalArrayAggregateOperatorState;

//! PhysicalArrayAggregate is a group-by and aggregate implementation that that does not use a hash table to perform
//! grouping, but utilizes adjacency of data. The Operator is implemented for process data (case | activity | timestamp).
//! It groups by case and aggregates all the activities into an dynamic-sized array
    class PhysicalArrayAggregate : public PhysicalOperator {
    public:
        PhysicalArrayAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> expressions,
                             PhysicalOperatorType type = PhysicalOperatorType::PROCESS_AGGREGATE);
        PhysicalArrayAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> expressions,
                             vector<unique_ptr<Expression>> groups,
                             PhysicalOperatorType type = PhysicalOperatorType::PROCESS_AGGREGATE);

        //! The groups
        vector<unique_ptr<Expression>> groups;
        //! The aggregates that have to be computed
        vector<unique_ptr<Expression>> aggregates;

    public:
        void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
        unique_ptr<PhysicalOperatorState> GetOperatorState() override;

    private:
        bool Aggregate(PhysicalArrayAggregateOperatorState *state);
    };

    class PhysicalArrayAggregateOperatorState : public PhysicalOperatorState {
    public:
        PhysicalArrayAggregateOperatorState(PhysicalArrayAggregate *parent, PhysicalOperator *child);

        //! The src (child) group chunk, only used while filling the Materialized Chunk
        DataChunk child_group_chunk;
        //! The src (child) payload chunk, only used while filling the Materialized Chunk
        DataChunk payload_chunk;

        //! The materialized group chunk
        DataChunk group_chunk;
        //! The materialized aggregate chunk
        DataChunk aggregate_chunk;

        //! Contains additional functions for additional aggregates
        vector<std::function<uint64_t(const int32_t *, const index_t)>> agg_functions;
        //! Indicates the fillstatus of the parent chunk
        index_t chunk_begin;
        //! Indicates the processing status of the child chunk
        index_t child_begin;

        bool HasUnfinishedChildChunk() const {
            return child_begin != 0;
        }
	    void Reset() {
		    chunk_begin = 0;
		    group_chunk.Reset();
		    aggregate_chunk.Reset();
	    }
        //! Aggregates as much as possible into chunk
        bool DoAggregation(Vector &group_agg, Vector &aggregate, const Vector &group, const Vector &payload);
        //! Aggregates as much as possible into chunk. With group and payload having a selection vector
        bool DoAggregate_Sel(Vector &group_agg, Vector &aggregate, const Vector &group, const Vector &payload);

        //! Additional aggregate count function specific for process agg
        static int64_t Count(const int32_t *, index_t count);
        //! Additional aggregate sum function specific for process agg
        static int64_t Sum(const int32_t *data, index_t count);

    private:
        //! Stores the last grp of the child
        int32_t last_grp;

        //! Append grp and the dynamic array to the chunk
        void AppendValue(int32_t *out_groups, int32_t **out_aggregates, const int32_t *src_payload, index_t count);

        //! Updates the array of the prev chunk value with the new found elements
        void UpdateLastValue(int32_t **out_aggregates, const int32_t *src_payload, index_t count);
    };
} // namespace duckdb