add_library_unity(duckdb_operator_aggregate
                  OBJECT
                  physical_array_aggregate.cpp
                  physical_hash_aggregate.cpp
                  physical_sha_aggregate.cpp
                  physical_simple_aggregate.cpp
                  physical_window.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_operator_aggregate> PARENT_SCOPE)
