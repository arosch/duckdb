#pragma once

#include <string>
#include <fstream>

#include "duckdb.hpp"
#include "PerfEvent.hpp"

using namespace duckdb;

namespace processagg {

    //! Utility class to compare different query execution times. Output is a .csv file.
    class Benchmark {
    public:
        Benchmark(const string &ofile, Connection &con, bool version, unsigned nruns);
        ~Benchmark();

        //! Measures the execution time for all Types and Queries with tuple range
        void MeasureTime(const vector<unsigned> & ntuples) const;
        //! Measures the execution time for all Types and Queries with top tuples
        void MeasureCPUStats(unsigned ntuple) const;
        //! Measures the execution time for all Types and Queries with tuple range and different group lengths
        void MeasureGroupLength(const vector<unsigned> & ngroups, const vector<unsigned> & nmeans) const;
        //! Measures the execution time for all Types and Queries with tuple range and different predicate selectivity
        void MeasureSelectivity(const vector<unsigned> & ntuples, const vector<std::pair<int, double>> & qualifiers) const;
        //! Measure the execution time for
        void MeasureReadableSha(const vector<unsigned> & ntuples) const;

        //! Test the selectivity for different
        void TestSelectivities(const vector<unsigned> & ntuples, int qual_start, int qual_end, int increment) const;

    private:
        //! Load the respective data and benchmark all QueryTypes
        void BenchmarkAll(unsigned tuple_count, bool printHeader, vector<std::pair<string,string>> params={}) const;
        //! Load the respective data and benchmark all QueryType for Q2: qualifying happy trace
        void BenchmarkQ2(int qualifier, unsigned tuple_count, bool printHeader, vector<std::pair<string,string>> params={}) const;

        //! Loads the respective file according to tuple_count
        void LoadData(unsigned tuple_count) const;
        //! Loads the respective group file according to tuple_count and mean
        void LoadGroupData(unsigned tuple_count, unsigned mean) const;

        //! Measures the Query defined by processAgg and qualifying
        void MeasureQ1(unsigned tuple_count, vector<std::pair<string,string>> & params, QueryType type, AggType agg, bool printHeader= false) const;
        //! Measures the Query Q2 with qualifier in predicate
        void MeasureQ2(int qualifier, unsigned tuple_count, vector<std::pair<string,string>> & params, QueryType type, AggType agg, bool printHeader= false) const;

        //! To build the queries
        QueryBuilder builder;
        //! The connection for querying and loading data
        Connection &con;
        //! The file to write final benchmark data as .csv
        std::ofstream out;
        //! buffer cout content
        std::streambuf * coutbuf;
        //! Indicator whether to measure the string versions as well
        const bool version;
        //! Measure each query execution time x times
        const unsigned nruns;
    };
}