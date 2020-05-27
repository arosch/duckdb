#include "query_builder.hpp"
#include "benchmark_pa.hpp"

using namespace processagg;

Benchmark::Benchmark(const string &ofile, Connection &con, const bool version, const unsigned nruns) : con(con), out(ofile.c_str(),std::ios::binary), version(version), nruns(nruns) {
    if(out){
        coutbuf = std::cout.rdbuf();
        std::cout.rdbuf(out.rdbuf()); //redirect std::cout
    }
}

Benchmark::~Benchmark() {
    if(out)
        std::cout.rdbuf(coutbuf);
}

void Benchmark::MeasureTime(const vector<unsigned> & ntuples) const {
    for(const auto ntuple: ntuples){
        LoadData(ntuple);
        BenchmarkAll(ntuple, ntuple == ntuples.front());
    }
}

void Benchmark::MeasureCPUStats(const unsigned ntuple) const {
    LoadData(ntuple);
    BenchmarkAll(ntuple, true);
}

void Benchmark::MeasureGroupLength(const vector<unsigned> & ngroups, const vector<unsigned> & nmeans) const {
    for(const auto ngroup: ngroups) {
        for(const auto nmean: nmeans) {
            LoadGroupData(ngroup, nmean);
            vector<std::pair<string,string>> params = {std::make_pair("groups",std::to_string(ngroup)), std::make_pair("mean",std::to_string(nmean))};
            BenchmarkAll(ngroup, ngroup == ngroups.front() && nmean == nmeans.front(), params);
        }
    }
}

void Benchmark::MeasureSelectivity(const vector<unsigned> & ntuples, const vector<std::pair<int, double>> & qualifiers) const {
    auto printHeader = true;
    for(const auto ntuple: ntuples) {
        LoadData(ntuple);
        for(const auto qualifier:qualifiers) {
            vector<std::pair<string,string>> params = {std::make_pair("selectivity",std::to_string(qualifier.second))};
            BenchmarkQ2(qualifier.first, ntuple, printHeader, params);
            if(printHeader) printHeader = false;
        }
    }
}

void Benchmark::TestSelectivities(const vector<unsigned> & ntuples, const int qual_start, const int qual_end,
        const int increment) const {
    std::cout << "tuples, approach, qualifier, selectivity\n";
    for(const auto ntuple: ntuples) {
        LoadData(ntuple);
        index_t ngroups;
        {
            auto stmts = builder.GetQuery(QueryType::QStringAgg, AggType::None, 0);
            ngroups = builder.Execute(con, move(stmts), false);
        }

        for(auto qualifier = qual_start; qualifier <= qual_end; qualifier += increment) {
			for(auto type:{QueryType::QStringAgg, QueryType::QArrayAgg, QueryType::QShaAgg}){
                auto stmts = builder.GetQuery(type, AggType::None, qualifier);
                const auto kgroups = builder.Execute(con, move(stmts), false);
                std::cout <<  ntuple << ", " << QueryBuilder::QueryTypeToString(type) << ", "<< qualifier <<", "<< static_cast<double>(kgroups)/ngroups << "\n";
			}
        }
    }
}

void Benchmark::MeasureReadableSha(const vector<unsigned> & ntuples) const {
    for(const auto tuple_count: ntuples){
        vector<std::pair<string,string>> params;
        LoadData(tuple_count);
        MeasureQ1(tuple_count, params, QueryType::ArrayAgg, AggType::None, tuple_count == ntuples.front());
        MeasureQ1(tuple_count, params, QueryType::ShaAgg, AggType::None);
        MeasureQ1(tuple_count, params, QueryType::ShaAgg, AggType::Readable);
    }
}

void Benchmark::BenchmarkAll(const unsigned tuple_count, const bool printHeader,
        vector<std::pair<string,string>> params) const {
    auto printH = printHeader;
    if(tuple_count <= 200000000 && version) {
        MeasureQ1(tuple_count, params, QueryType::StringAgg, AggType::None, printH);
        printH = false;
    }
    MeasureQ1(tuple_count, params, QueryType::ArrayAgg, AggType::None, printH);
    MeasureQ1(tuple_count, params, QueryType::ShaAgg, AggType::None);
    if(tuple_count <= 200000000 && version)
        MeasureQ2(0, tuple_count, params, QueryType::QStringAgg, AggType::None);
    MeasureQ2(0, tuple_count, params, QueryType::QArrayAgg, AggType::None);
    MeasureQ2(0, tuple_count, params, QueryType::QShaAgg, AggType::None);
}

void Benchmark::BenchmarkQ2(const int qualifier, const unsigned tuple_count, const bool printHeader,
        vector<std::pair<string,string>> params) const {
    auto printH = printHeader;
    if(tuple_count <= 200000000 && version) {
        MeasureQ2(qualifier, tuple_count, params, QueryType::QStringAgg, AggType::None, printH);
        printH = false;
    }
    MeasureQ2(qualifier, tuple_count, params, QueryType::QArrayAgg, AggType::None, printH);
    MeasureQ2(qualifier, tuple_count, params, QueryType::QShaAgg, AggType::None);
}

void Benchmark::LoadData(const unsigned tuple_count) const {
    con.Query("DROP TABLE eventlog");

    con.Query("CREATE TABLE eventlog(case_id INTEGER, activity INTEGER, timestamp INTEGER)");
    con.Query("COPY eventlog FROM 'data/data-" + std::to_string(tuple_count) + ".csv'( DELIMITER ' ')");
}

void Benchmark::LoadGroupData(const unsigned tuple_count, const unsigned mean) const {
    con.Query("DROP TABLE eventlog");

    con.Query("CREATE TABLE eventlog(case_id INTEGER, activity INTEGER, timestamp INTEGER)");
    con.Query("COPY eventlog FROM 'data/groups/data-" + std::to_string(tuple_count) + "-" + std::to_string(mean) + ".csv'( DELIMITER ' ')");
}

void Benchmark::MeasureQ1(unsigned tuple_count, vector<std::pair<string,string>> & params, QueryType type,
                          AggType agg, bool printHeader) const {
    params.emplace_back("query", "Q1");
    params.emplace_back("approach", QueryBuilder::QueryTypeToApproachString(type, agg));
    for(auto i=0u; i<nruns; i++) {
        std::lock_guard<std::mutex> client_guard(con.context->context_lock);
        auto stmts = builder.GetQuery(type, agg);
        //auto stmts = builder.GetSubquery(type, agg);
        auto result = con.context->BenchmarkStatement(move(stmts.front()), params, tuple_count, printHeader && i == 0);
        assert(!result->success);
    }
}

void Benchmark::MeasureQ2(int qualifier, unsigned tuple_count, vector<std::pair<string,string>> & params,
                          QueryType type, AggType agg, bool printHeader) const {
    params.emplace_back("query", "Q2");
    params.emplace_back("approach", QueryBuilder::QueryTypeToApproachString(type, agg));
    for(auto i=0u; i<nruns; i++) {
        std::lock_guard<std::mutex> client_guard(con.context->context_lock);
        auto stmts = builder.GetQuery(type, agg, qualifier);
        //auto stmts = builder.GetSubquery(type, agg, qualifier);
        auto result = con.context->BenchmarkStatement(move(stmts.front()), params, tuple_count, printHeader && i == 0);
		assert(!result->success);
    }
}