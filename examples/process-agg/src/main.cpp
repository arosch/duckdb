#include <fstream>

#include "duckdb.hpp"
#include "query_builder.hpp"
#include "benchmark_pa.hpp"

using namespace duckdb;
using namespace processagg;

void stringQ1(Connection &con){
    const auto result = con.Query("WITH activity_sequences as (SELECT case_id, STRING_AGG (activity, '|') AS activity_sequence FROM eventlog GROUP BY case_id) SELECT count(*) AS occurrences, activity_sequence FROM activity_sequences GROUP BY activity_sequence");
    result->Print();
}

void stringQ2(Connection &con){
    const auto result = con.Query("WITH qualifying_cases as (SELECT DISTINCT case_id FROM eventlog WHERE timestamp > 2), activity_sequences as (SELECT el.case_id, STRING_AGG (el.activity, ',') AS activity_sequence FROM eventlog el, qualifying_cases qc WHERE qc.case_id = el.case_id GROUP BY el.case_id) SELECT count(*) AS occurences, activity_sequence FROM activity_sequences GROUP BY activity_sequence");
    result->Print();
}

int main(int argc, char ** argv) {

    if(argc < 2 || argc > 7) {
        std::cerr << "Usage:\n";
        std::cerr << "./process-agg benchmark <type> <versions> <nruns> [<output-file>]\n";
        std::cerr << "with:\n";
        std::cerr << "\t<type> = time | stats | groups | select | read\n";
        std::cerr << "\t<versions> = all | opt\n";
        std::cerr << "or\n";
        std::cerr << "./process-agg single <query> <approach> <input-file> <delimiter> [<qualifier>]\n";
        std::cerr << "with:\n";
        std::cerr << "\t<query> = full | sub\n";
        std::cerr << "\t<approach> = StringAgg | ArrayAgg | ShaAgg | QStringAgg | QArrayAgg | QShaAgg\n";
        std::cerr << "or\n";
        std::cerr << "./process-agg test\n";
        return 0;
    }

    const std::string mode(argv[1]);


    DuckDB db(nullptr);
    Connection con(db);
    const std::vector<unsigned> tuple_range = {100000, 1000000, 5000000, 10000000, 20000000, 40000000, 60000000, 80000000,
	                                           100000000, 130000000, 160000000, 200000000, 300000000, 400000000, 500000000,
	                                           600000000};

    if(mode == "benchmark") {

        const std::string type(argv[2]);
        const std::string version(argv[3]);
        bool v;
        if(version == "all")
            v = true;
        else if(version == "opt")
            v = false;
        else
            throw NotImplementedException("Not implemented version for benchmark mode");

        auto nruns = std::atoi(argv[4]);
        const std::string ofile(argc == 6 ? argv[5] : "");

        Benchmark bench(ofile, con, v, nruns);

        if(type == "time") {
            bench.MeasureTime(tuple_range);
        } else if(type == "stats") {
            bench.MeasureCPUStats(200000000);
        }  else if(type == "groups") {
            const vector<unsigned> means = {5, 10, 20, 30, 40, 50, 60, 70, 80, 90};
            bench.MeasureGroupLength({100000000}, means);
        } else if(type == "select") {
            const std::vector<std::pair<int, double>> qualifiers = {{10,95.6}, {12,90.7},{14,82.7},
                {16,72.6},{18,60.7},{20,48.1},{21,41.3},{23,28.5},{25,18.5},
                {28,10.9},{34,4.8}};
            bench.MeasureSelectivity({100000000}, qualifiers);
        } else if(type == "read") {
            bench.MeasureReadableSha(tuple_range);
        } else
            throw NotImplementedException("Not implemented type for benchmark mode");

    } else if (mode == "single") {

        const std::string query(argv[2]);
        QueryType approach = QueryBuilder::StringToQueryType(argv[3]);
        const std::string inFile(argv[4]);
        const std::string delimiter(argv[5][0] == 'w' ? " " : argv[5]);
        const int qualifier(argc > 6 ? std::atoi(argv[6]) : 0);

        con.Query("CREATE TABLE eventlog(case_id INTEGER, activity INTEGER, timestamp INTEGER)");
        std::stringstream ss;
        ss << "COPY eventlog FROM '" << inFile << "'( DELIMITER '"<< delimiter << "')";
        con.Query(ss.str());

        QueryBuilder queryBuilder;

        vector<unique_ptr<SQLStatement>> stmts;

        if(query == "full")
            stmts = queryBuilder.GetQuery(approach, AggType::None, qualifier);
        else if(query == "sub")
            stmts = queryBuilder.GetSubquery(approach, AggType::None, qualifier);
        else
            throw NotImplementedException("Not implemented query for single mode");

        queryBuilder.Execute(con, move(stmts), true);

    } else if (mode == "test") {

        Benchmark bench("", con, true, 1);
        bench.TestSelectivities({100000000}, 0,100,1);

    } else {
            throw NotImplementedException("Not implemented mode");
    }
}
