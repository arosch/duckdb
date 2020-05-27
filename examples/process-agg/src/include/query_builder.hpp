#pragma once

#include "duckdb.hpp"

using namespace duckdb;

/** Naive Non-Qualifying Query:
 *
 *  WITH activity_sequences as
 *      (SELECT case_id, STRING_AGG (activity, '') AS activity_sequence FROM eventlog GROUP BY case_id)
 *      SELECT activity_sequence, count(*) AS occurrences FROM activity_sequences GROUP BY activity_sequence */

/** Naive Qualifying Query:
 *
 *  WITH qualifying_cases as
 *      (SELECT DISTINCT case_id FROM eventlog WHERE timestamp > 2),
 *      activity_sequences as
 *      (SELECT el.case_id, STRING_AGG (el.activity, '') AS activity_sequence FROM qualifying_cases qc, eventlog el
 *          WHERE qc.case_id = el.case_id GROUP BY el.case_id)
 *      SELECT activity_sequence, count(*) AS occurences FROM activity_sequences GROUP BY activity_sequence */

namespace processagg {

//! The types of available queries
enum class QueryType {
    ArrayAgg,
    QArrayAgg,
    ShaAgg,
    QShaAgg,
    StringAgg,
    QStringAgg
};
//! The types of available aggregates for ProcessAgg variants
enum class AggType {
    None,
    Count,
    Sum,
    Readable
};

//! Utility class that enables retrieval of query stmts for execution
class QueryBuilder {

public:
    QueryBuilder();

    //! Executes the given query in connection
    index_t Execute(Connection &con, vector<unique_ptr<SQLStatement>> query, bool print_full = true) const;

    //! Get the full count query of type with aggregates. The qualifier for Q2: timestamp > qualifier
    vector<unique_ptr<SQLStatement>> GetQuery(QueryType queryT, AggType aggT, int qualifier=-1) const;
    //! Get the aggregate subquery of type with aggregates
    vector<unique_ptr<SQLStatement>> GetSubquery(QueryType queryT, AggType aggT, int qualifier=-1) const;

    //! Get the approach type string for a given QueryType
    static string QueryTypeToString(QueryType type);
    //! Get the approach type string for a given QueryType
    static string QueryTypeToApproachString(QueryType type, AggType agg);
    //! Get the query string for a given QueryType
    static string QueryTypeToQueryString(QueryType type);
    //! Get the QueryType for a given string
    static QueryType StringToQueryType(const string& type);

private:
    static unique_ptr<SelectNode> GetCountNode(AggType aggT);

    unique_ptr<SelectNode> GetProcessAggNode(const string &type, AggType aggT, int qualfier=-1) const;

    unique_ptr<SelectNode> GetStringAggNode() const;

    unique_ptr<SelectNode> GetQualifyingCasesNode(int qualifier) const;
    unique_ptr<SelectNode> GetQStringAggNode() const;

    //!
    //! Delimiter for string aggregation
    const string delimiter;
};


} // namespace processagg