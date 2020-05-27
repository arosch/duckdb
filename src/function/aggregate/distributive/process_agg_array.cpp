#include "duckdb/function/aggregate/distributive_functions.hpp"

namespace duckdb {

//! This is just a dummy function to parse a logical_aggregate operator to the specialized physical_array_aggregate operator
void process_agg_array_update(Vector * , index_t , Vector & ) {
    assert(false);
}

void ProcessAggArrayFun::RegisterFunction(BuiltinFunctions &set) {
    set.AddFunction(
            AggregateFunction("process_agg_array", {SQLType::INTEGER}, SQLType::INTEGERARRAY,
                              get_return_type_size,
                              null_state_initialize, process_agg_array_update, nullptr, gather_finalize));
}

} // namespace duckdb
