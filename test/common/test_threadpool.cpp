#include "catch.hpp"
#include "common/thread_pool.hpp"
#include <iostream>
#include "duckdb.hpp"

#include <vector>

using namespace duckdb;
using namespace std;

#define NUM_INTS 10

TEST_CASE("Basic Threadpool Tests", "[threadpool]") {
    std::vector< std::future<int> > results;

    DuckDB db;

    ThreadPool pool(db.n_worker_threads);

    for(int i = 0; i < 8; ++i) {
        results.emplace_back(
            pool.enqueue([i] {
                return i*i;
            })
        );
    }

    for(auto && result: results)
        std::cout << result.get() << ' ';
    std::cout << std::endl;
}
