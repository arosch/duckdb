add_library_unity(duckdb_aggr_distr
				OBJECT
				process_agg_array.cpp
				process_agg_sha.cpp
				count.cpp
				first.cpp
				max.cpp
				min.cpp
				string_agg.cpp
				sum.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_aggr_distr> PARENT_SCOPE)
