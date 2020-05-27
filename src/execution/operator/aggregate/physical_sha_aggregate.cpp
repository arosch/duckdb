#include "duckdb/execution/operator/aggregate/physical_sha_aggregate.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"

using namespace duckdb;

PhysicalShaAggregate::PhysicalShaAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> expressions,
                                           PhysicalOperatorType type)
        : PhysicalShaAggregate(move(types), move(expressions), {}, type) {
}

PhysicalShaAggregate::PhysicalShaAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> expressions,
                                           vector<unique_ptr<Expression>> groups, PhysicalOperatorType type)
        : PhysicalOperator(type, move(types)), groups(move(groups)) {
    // get a list of all aggregates to be computed
    if (this->groups.empty()) {
        assert(false); // no aggregation without groups handle
    }
    for (auto &expr : expressions) {
        assert(expr->expression_class == ExpressionClass::BOUND_AGGREGATE);
        assert(expr->IsAggregate());
        aggregates.push_back(move(expr));
    }
}

void PhysicalShaAggregate::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
    auto state = reinterpret_cast<PhysicalShaAggregateOperatorState *>(state_);
    state->Reset();

    bool chunk_full = false;
    if(state->HasUnfinishedChildChunk()) {
        chunk_full = Aggregate(state);
    }

    while (!chunk_full) {
        if (!children.empty()) {
            // resolve the child chunk if there is one
            children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
            state->child_begin = 0;
            if (state->child_chunk.size() == 0) {
                if(state->ctx.num != 0){
                    auto out_aggregates = (sha_t *)state->aggregate_chunk.GetVector(0).data;
                    state->FinalizeLastValue(out_aggregates);
                }
                break;
            }
        }
        chunk_full = Aggregate(state);
    }
    // fully populated chunks or finished the child chunk(s)
    // actually compute the final projection list now
    index_t chunk_index = 0;
    if (state->group_chunk.column_count + state->aggregate_chunk.column_count == chunk.column_count) {
        for (index_t col_idx = 0; col_idx < state->group_chunk.column_count; col_idx++) {
            chunk.data[chunk_index++].Reference(state->group_chunk.data[col_idx]);
        }
    } else {
        assert(state->aggregate_chunk.column_count == chunk.column_count);
    }
    for (index_t col_idx = 0; col_idx < state->aggregate_chunk.column_count; col_idx++) {
        chunk.data[chunk_index++].Reference(state->aggregate_chunk.data[col_idx]);
    }
}

bool PhysicalShaAggregate::Aggregate(PhysicalShaAggregateOperatorState *state) {
    ExpressionExecutor executor(state->child_chunk);
    // populate the group chunk
    DataChunk &group_chunk = state->child_group_chunk;
    group_chunk.Reset();
    executor.Execute(groups, group_chunk);
    // populate the payload chunk
    DataChunk &payload_chunk = state->payload_chunk;
    payload_chunk.Reset();
    index_t payload_idx = 0;
    for (const auto& agg: aggregates) {
        auto &aggr = (BoundAggregateExpression &)*agg;
        if (!aggr.children.empty()) {
            executor.ExecuteExpression(*aggr.children[0], payload_chunk.data[payload_idx]);
            break;
        }
    }
    payload_chunk.sel_vector = group_chunk.sel_vector;
    // retrieve record vectors
    auto &group_agg = state->group_chunk.GetVector(0);
    auto &aggregate = state->aggregate_chunk.GetVector(0);

    bool chunk_full;
    if(group_chunk.sel_vector) {
		chunk_full = state->DoAggregate_Sel(group_agg, aggregate, group_chunk.GetVector(0), payload_chunk.GetVector(0));
    } else {
		chunk_full = state->DoAggregation(group_agg, aggregate, group_chunk.GetVector(0), payload_chunk.GetVector(0));
    }

    // set all the vectors of chunk to respective count, b/c we don't increment on each append
    for(auto j = 0u; j < group_chunk.column_count; j++) {
        state->group_chunk.GetVector(j).count = state->chunk_begin;
    }
    for(auto j = 0u; j < state->aggregate_chunk.column_count; j++) {
        state->aggregate_chunk.GetVector(j).count = state->chunk_begin;
    }
    return chunk_full;
}

unique_ptr<PhysicalOperatorState> PhysicalShaAggregate::GetOperatorState() {
    auto state =
            make_unique<PhysicalShaAggregateOperatorState>(this, children.empty() ? nullptr : children[0].get());
    vector<TypeId> group_types, payload_types;
    for (auto &expr : groups) {
        group_types.push_back(expr->return_type);
    }
    state->child_group_chunk.Initialize(group_types);
    for (auto &expr : aggregates) {
        assert(expr->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
        auto &aggr = (BoundAggregateExpression &) *expr;

        // initialization for additional aggregate functions, apart from array_aggregate
        const auto &fname = aggr.function.name;
        if(fname == "process_agg_sha") {
            // do nothing

            // hijacking... only processing the payload for the first aggregate
            payload_types.push_back(aggr.children[0]->return_type);
        } else if(fname == "count") {
            state->agg_functions.emplace_back(PhysicalArrayAggregateOperatorState::Count);
        } else if(fname == "sum") {
            state->agg_functions.emplace_back(PhysicalArrayAggregateOperatorState::Sum);
        } else if(fname == "process_agg_array") {
            state->readable = true;
        } else {
            throw NotImplementedException("This aggregate function is not implemented for process_agg_array");
        }
    }
    if (!payload_types.empty()) {
        state->payload_chunk.Initialize(payload_types);
    }
    return move(state);
}

PhysicalShaAggregateOperatorState::PhysicalShaAggregateOperatorState(PhysicalShaAggregate *parent,
                                                                     PhysicalOperator *child)
        : PhysicalOperatorState(child), ctx(), chunk_begin(0), child_begin(0), readable(false), last_grp(-1) {
    vector<TypeId> group_types, aggregate_types;
    for (auto &expr : parent->groups) {
        group_types.push_back(expr->return_type);
    }
    group_chunk.Initialize(group_types);
    for (auto &expr : parent->aggregates) {
        aggregate_types.push_back(expr->return_type);
    }
    if (!aggregate_types.empty()) {
        aggregate_chunk.Initialize(aggregate_types);
    }
}

bool PhysicalShaAggregateOperatorState::DoAggregation(Vector &group_agg, Vector &aggregate, const Vector &group, const Vector &payload) {
    // the data for source/child chunks
    auto src_groups = (const int32_t *)group.data;
    auto src_payload = (const int32_t *)payload.data;
    // the data for materialized chunks
    auto out_groups = (int32_t *)group_agg.data;
    auto out_aggregates = (sha_t *)aggregate.data;

    // if the last group of the previous child chunk contained a different group
    auto first_grp = src_groups[child_begin];

    index_t update_length = 0;

    // finish data assignment of previous child chunk. Note: no fillstatus check, b/c only update
    if(last_grp == first_grp) {
        // seek the end of last_grp
        for(index_t i = 0; i < payload.count; i++) {
            const auto grp = src_groups[i];
            if(grp == last_grp) {
                update_length++;
                continue;
            }
            UpdateLastValue(out_aggregates, src_payload + child_begin, update_length);
            // reset
            child_begin = i;
            if(chunk_begin == STANDARD_VECTOR_SIZE) {
                return true;
            }
            update_length = 0;
            last_grp = grp;
            // we only seek until last_grp is updated
            break;
        }
        if(last_grp == first_grp) { // handle for same-grp chunks since update never performed
            UpdateLastValue(out_aggregates, src_payload + child_begin, payload.count);
            return false;
        }
    } else {
        if(last_grp != -1) // skip for the first chunk
            FinalizeLastValue(out_aggregates);
        if(chunk_begin == STANDARD_VECTOR_SIZE) {
            return true;
        }
        last_grp = first_grp;
    }

    // process the complete or remaining payload
    for(index_t i = child_begin; i < payload.count; i++) {
        const auto grp = src_groups[i];
        if(grp == last_grp) {
            update_length++;
            continue;
        }
        AppendValue(out_groups, out_aggregates, src_payload + child_begin, update_length);
        // reset
        child_begin = i;
        if(chunk_begin == STANDARD_VECTOR_SIZE) {
            return true;
        }
        // if chunk full, we want to start with a different grp, b/c no update required
        last_grp = grp;
        update_length = 1;

    }
    // update remaining end of chunk payload data
    AppendValue(out_groups, out_aggregates, src_payload + child_begin, update_length, false);

    // b/c the next child chunk can contain the same grp: updates do not append
    return false;
}

bool PhysicalShaAggregateOperatorState::DoAggregate_Sel(Vector &group_agg, Vector &aggregate, const Vector &group, const Vector &payload) {
    // the data for source/child chunks
    auto src_groups = (const int32_t *)group.data;
    auto src_payload = (const int32_t *)payload.data;
    auto sel = payload.sel_vector;
    // the data for materialized chunks
    auto out_groups = (int32_t *)group_agg.data;
    auto out_aggregates = (sha_t *)aggregate.data;

    // if the last group of the previous child chunk contained a different group
    auto first_grp = src_groups[group.sel_vector[child_begin]];

    index_t update_length = 0;

    // finish data assignment of previous child chunk. Note: no fillstatus check, b/c only update
    if(last_grp == first_grp) {
        // seek the end of last_grp
        for(index_t k = 0; k < payload.count; k++) {
            const auto grp = src_groups[group.sel_vector[k]];
            if(grp == last_grp) {
                update_length++;
                continue;
            }
            UpdateLastValue(out_aggregates, src_payload + sel[child_begin], update_length);
            // reset
            child_begin = k;
            if(chunk_begin == STANDARD_VECTOR_SIZE) {
                return true;
            }
            update_length = 0;
            last_grp = grp;
            // we only seek until last_grp is updated
            break;
        }
        if(last_grp == first_grp) { // handle for same-grp chunks since update never performed
            UpdateLastValue(out_aggregates, src_payload + sel[child_begin], payload.count);
            return false;
        }
    } else {
        if(last_grp != -1) // skip for the first chunk
            FinalizeLastValue(out_aggregates);
        if(chunk_begin == STANDARD_VECTOR_SIZE) {
            return true;
        }
        last_grp = first_grp;
    }

    // process the complete or remaining payload
    for(index_t k = child_begin; k < payload.count; k++) {
        const auto i = group.sel_vector[k];
        const auto grp = src_groups[i];
        if(grp == last_grp) {
            update_length++;
            continue;
        }
        AppendValue(out_groups, out_aggregates, src_payload + sel[child_begin], update_length);
        // reset
        child_begin = k;
        if(chunk_begin == STANDARD_VECTOR_SIZE) {
            return true;
        }
        // if chunk full, we want to start with a different grp, b/c no update required
        last_grp = grp;
        update_length = 1;
    }
    // update remaining end of chunk payload data
    AppendValue(out_groups, out_aggregates, src_payload + sel[child_begin], update_length, false);

    // b/c the next child chunk can contain the same grp: updates do not append
    return false;
}

int64_t PhysicalShaAggregateOperatorState::Count(const int32_t *, const index_t count) {
    return count;
}

int64_t PhysicalShaAggregateOperatorState::Sum(const int32_t *data, const index_t count) {
    int64_t sum = 0;
    for(auto i=0u; i<count; i++) {
        sum += data[i];
    }
    return sum;
}

void PhysicalShaAggregateOperatorState::AppendValue(int32_t *out_groups, sha_t *out_aggregates, const int32_t *src_payload, const index_t count, const bool finalize) {
    assert(chunk_begin < STANDARD_VECTOR_SIZE);
    out_groups[chunk_begin] = last_grp;
    // compute the aggregate
    SHA1_Init(&ctx);
    SHA1_Update(&ctx, reinterpret_cast<const unsigned char *>(src_payload), count * sizeof(int32_t));
    if(finalize)
	    SHA1_Final(out_aggregates[chunk_begin], &ctx);

    index_t chunk_idx = 1;
    // set other aggregates values if there are any
    for(const auto &func : agg_functions) {
        auto &aggr = aggregate_chunk.GetVector(chunk_idx++);
        auto aggr_data = (int64_t *)aggr.data;
        aggr_data[chunk_begin] = func(src_payload, count);
    }
    if(readable) {
        auto array_data = (int32_t **)aggregate_chunk.GetVector(chunk_idx).data;
        array_data[chunk_begin] = aggregate_chunk.heap.AddInts(src_payload, count);
    }

    chunk_begin++;
}

void PhysicalShaAggregateOperatorState::UpdateLastValue(sha_t *out_aggregates, const int32_t *src_payload, const index_t count) {
    const auto index = chunk_begin == 0 ? 0 : chunk_begin - 1;
    assert(index < STANDARD_VECTOR_SIZE);
    // grp is already assigned, -1 to update data from previous child chunk
    SHA1_Update(&ctx, reinterpret_cast<const unsigned char *>(src_payload), count * sizeof(int32_t));
    SHA1_Final(out_aggregates[index], &ctx);

    index_t chunk_idx = 1;
    // update other aggregates values if there are any
    for(const auto &func : agg_functions) {
        auto &aggr = aggregate_chunk.GetVector(chunk_idx++);
        auto aggr_data = (uint64_t *)aggr.data;
        aggr_data[index] += func(src_payload, count);
    }
    if(readable) {
        auto array_data = (int32_t **)aggregate_chunk.GetVector(chunk_idx).data;
        array_data[index] = aggregate_chunk.heap.AddInts(array_data[index],src_payload, count);
    }
}

void PhysicalShaAggregateOperatorState::FinalizeLastValue(sha_t *out_aggregates) {
    const auto index = chunk_begin == 0 ? 0 : chunk_begin - 1;
    assert(index < STANDARD_VECTOR_SIZE);
    // group is already assigned to output chunk
    SHA1_Final(out_aggregates[index], &ctx);
}