#include "duckdb/function/aggregate/distributive_functions.hpp"

namespace duckdb {

//! This is just a dummy function to parse a logical_aggregate operator to the specialized physical_sha_aggregate operator
void process_agg_sha_update(Vector * , index_t , Vector &) {
    assert(false);
}

void ProcessAggShaFun::RegisterFunction(BuiltinFunctions &set) {
    set.AddFunction(
            AggregateFunction("process_agg_sha", {SQLType::INTEGER}, SQLType::SHA,
                              get_return_type_size,
                              null_state_initialize, process_agg_sha_update, nullptr, gather_finalize));
}

} // namespace duckdb
