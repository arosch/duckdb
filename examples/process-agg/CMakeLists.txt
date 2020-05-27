cmake_minimum_required(VERSION 2.8.7)
project(process-agg)

set(CMAKE_CXX_STANDARD 11)

include_directories(../../src/include
        src/include
        ../../third_party/perfevent)
link_directories(../../build/debug/src)

add_executable(process-agg
        src/main.cpp
        src/query_builder.cpp
        src/benchmark_pa.cpp)
target_link_libraries(process-agg duckdb)
