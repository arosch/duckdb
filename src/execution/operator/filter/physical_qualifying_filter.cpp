#include "duckdb/execution/operator/filter/physical_qualifying_filter.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/execution/expression_executor.hpp"

using namespace duckdb;

void PhysicalQualifyingFilter::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
    auto state = reinterpret_cast<PhysicalQualifyingFilterOperatorState *>(state_);

    if(state->hasBufferedSameGrpChunks()) {
        state->RetrieveSameGrpChunk(chunk);
        return; // if we have same grp chunks here, the qualifier will not change
    } else if (state->child_chunk.size() != 0) {
        chunk.Swap(state->child_chunk);
        // continue, the last grp might continue in next data chunk
    }
    bool finished;
    do {
        children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
        if (state->child_chunk.size() == 0) {
            if(chunk.size() != 0)
                state->SetSelectionVector(chunk);
            return;
        }
        finished = Qualify(chunk, state);

		// prevents chunks with not a single qualifying group to be returned.
		if(chunk.size() == 0) {
            chunk.Swap(state->child_chunk);
            if(state->firstchild)
                state->firstchild = false;
			finished = false;
			continue;
		}
    } while (!finished);
}

bool PhysicalQualifyingFilter::Qualify(DataChunk &chunk, PhysicalQualifyingFilterOperatorState *state) {
    // Split child chunk into left, right, grp
    assert(expressions.size() == 1);
    ExpressionExecutor executor(state->child_chunk);
    auto &expr = (BoundQualifyingExpression &) *expressions[0];
    Vector right;
    executor.Execute(*expr.right, right);
    assert(state->child_chunk.column_count == 3);

    auto &left = state->child_chunk.GetVector(0);
    auto &grp = state->child_chunk.GetVector(1);

    return state->QualifyingGreaterThan(chunk, left, right, grp);
}

unique_ptr<PhysicalOperatorState> PhysicalQualifyingFilter::GetOperatorState() {
    return make_unique<PhysicalQualifyingFilterOperatorState>(this, children.empty() ? nullptr : children[0].get());;
}

PhysicalQualifyingFilterOperatorState::PhysicalQualifyingFilterOperatorState(PhysicalQualifyingFilter *, PhysicalOperator *child)
    :PhysicalOperatorState(child), firstchild(true), selection_buffer(TypeId::BOOLEAN, true, false),
     last_grp(-1), overlap_qualifier(false) { }

bool PhysicalQualifyingFilterOperatorState::QualifyingGreaterThan(DataChunk &chunk, const Vector &left, const Vector &right, const Vector &groups) {
    const auto ldata = (const int32_t *)left.data;
    const auto rdata = (const int32_t *)right.data;
    const int32_t constant = rdata[0];

    const auto gdata = (const int32_t *)groups.data;

    auto result_data = (bool *)selection_buffer.data;

    index_t child_begin = 0;
    bool qualifier = false;

    const auto first_grp = gdata[child_begin];
    if(last_grp == first_grp) { // overlap
        // seek the end of last_grp
        for(index_t i = 0; i < groups.count; i++) {
            const auto grp = gdata[i];
            if(grp == last_grp) {
                qualifier = qualifier || GreaterThan::Operation(ldata[i], constant);
                continue;
            }
            if(qualifier != overlap_qualifier) { // update if necessary
                overlap_qualifier = overlap_qualifier || qualifier;
                for(index_t j = last_grp_begin; j < selection_buffer.count; j++) {
                    result_data[j] = overlap_qualifier;
                }
            }
            SetSelectionVector(chunk);
            // set data for new child chunk
            for(index_t j = 0; j < i; j++) {
                result_data[j] = overlap_qualifier;
            }
            // reset
            child_begin = i;
            last_grp = grp;
            qualifier = GreaterThan::Operation(ldata[i], constant);
            // we only seek until last_grp is updated
            break;
        }
        if(last_grp == first_grp) { // handle for same-grp chunks since update never performed
            if(qualifier != overlap_qualifier) { // update if necessary
                overlap_qualifier = overlap_qualifier || qualifier;
                for(index_t j = last_grp_begin; j < selection_buffer.count; j++) {
                    result_data[j] = overlap_qualifier;
                }
            }
            BufferChildChunkSameGrp();
            // buffer child chunk in same_grp list
            return false;
        }
    } else { // no overlap
        SetSelectionVector(chunk);
        last_grp = first_grp;
    }

    // process the complete or remaining payload
    for(index_t i = child_begin; i < groups.count; i++) {
        const auto grp = gdata[i];
        if(grp == last_grp) {
            qualifier = qualifier || GreaterThan::Operation(ldata[i], constant);
            continue;
        }
        for(index_t j = child_begin; j < i; j++) {
            result_data[j] = qualifier;
        }
        // reset
        child_begin = i;
        last_grp = grp;
        qualifier = GreaterThan::Operation(ldata[i], constant);
    }
    // postprocess last grp
    last_grp_begin = child_begin;
    for(index_t j = child_begin; j < groups.count; j++) {
        result_data[j] = qualifier;
    }
    overlap_qualifier = qualifier;
    selection_buffer.count = groups.count;
    return true;
}

void PhysicalQualifyingFilterOperatorState::BufferChildChunkSameGrp(){
    same_grp_buffer.emplace_back();
    child_chunk.Move(same_grp_buffer.back());

    auto types = same_grp_buffer.back().GetTypes();
    child_chunk.Initialize(types);
}

void PhysicalQualifyingFilterOperatorState::RetrieveSameGrpChunk(DataChunk &chunk) {
    same_grp_buffer.front().Move(chunk);
    same_grp_buffer.pop_front();

    Vector result(TypeId::BOOLEAN, true, true);
    const auto count = chunk.GetVector(0).count;
    if(overlap_qualifier) { //initialized with false, so only change if true
        auto result_data = (bool *)result.data;
        for(index_t j = 0; j < count; j++) {
            result_data[j] = overlap_qualifier;
        }
    }
    result.count = count;
    chunk.SetSelectionVector(result);
}

void PhysicalQualifyingFilterOperatorState::SetSelectionVector(DataChunk &chunk) {
    if(selection_buffer.count == 0)
        return;
    chunk.SetSelectionVector(selection_buffer);
    selection_buffer.Flatten();
}