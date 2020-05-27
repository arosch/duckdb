#include <duckdb/planner/planner.hpp>
#include <duckdb/execution/operator/projection/physical_projection.hpp>
#include <duckdb/parser/expression/qualifying_expression.hpp>
#include "duckdb/parser/tableref/list.hpp"
#include "duckdb/parser/expression/list.hpp"

#include "query_builder.hpp"

using namespace duckdb;
using namespace processagg;

QueryBuilder::QueryBuilder() : delimiter(1,',') { }

index_t QueryBuilder::Execute(Connection &con, vector<unique_ptr<SQLStatement>> query, bool print_full) const{
    std::lock_guard<std::mutex> client_guard(con.context->context_lock);
    const auto result = con.context->ExecuteStatementsInternal("", query, false);

    if(print_full)
        result->Print();

    const auto rpointer = (MaterializedQueryResult *) result.get();
    return rpointer->collection.count;
}

vector<unique_ptr<SQLStatement>> QueryBuilder::GetQuery(QueryType queryT, AggType aggT, const int qualifier) const {
    auto stmt = make_unique<SelectStatement>();

    // retrieve the nodes
    switch (queryT) {
        case QueryType::ArrayAgg:
            stmt->cte_map["activity_sequences"] = GetProcessAggNode("process_agg_array", aggT);
            break;
        case QueryType::QArrayAgg:
            assert(qualifier >= 0);
            stmt->cte_map["activity_sequences"] = GetProcessAggNode("process_agg_array", aggT, qualifier);
            break;
        case QueryType::ShaAgg:
            stmt->cte_map["activity_sequences"] = GetProcessAggNode("process_agg_sha", aggT);
            break;
        case QueryType::QShaAgg:
            assert(qualifier >= 0);
            stmt->cte_map["activity_sequences"] = GetProcessAggNode("process_agg_sha", aggT, qualifier);
            break;
        case QueryType::StringAgg:
            stmt->cte_map["activity_sequences"] = GetStringAggNode();
            break;
        case QueryType::QStringAgg:
            assert(qualifier >= 0);
            stmt->cte_map["qualifying_cases"] = GetQualifyingCasesNode(qualifier);
            stmt->cte_map["activity_sequences"] = GetQStringAggNode();
            break;
        default:
            throw NotImplementedException("Not implemented Query Type");
    }

    stmt->node = GetCountNode(aggT);

    vector<unique_ptr<SQLStatement>> statements;
    statements.push_back(move(stmt));
    return statements;
}

vector<unique_ptr<SQLStatement>> QueryBuilder::GetSubquery(QueryType queryT, AggType aggT, const int qualifier) const {
    auto stmt = make_unique<SelectStatement>();

    // retrieve the nodes
    switch (queryT) {
        case QueryType::ArrayAgg:
            stmt->node = GetProcessAggNode("process_agg_array", aggT);
            break;
        case QueryType::QArrayAgg:
            stmt->node = GetProcessAggNode("process_agg_array", aggT, qualifier);
            break;
        case QueryType::ShaAgg:
            stmt->node = GetProcessAggNode("process_agg_sha", aggT);
            break;
        case QueryType::QShaAgg:
            stmt->node = GetProcessAggNode("process_agg_sha", aggT, qualifier);
            break;
        case QueryType::StringAgg:
            stmt->node = GetStringAggNode();
            break;
        case QueryType::QStringAgg:
            stmt->cte_map["qualifying_cases"] = GetQualifyingCasesNode(qualifier);
            stmt->node = GetQStringAggNode();
            break;
        default:
            throw NotImplementedException("Not implemented Query Type");
    }

    vector<unique_ptr<SQLStatement>> statements;
    statements.push_back(move(stmt));
    return statements;
}

string QueryBuilder::QueryTypeToString(const QueryType type) {
    switch (type) {
        case QueryType::ArrayAgg:
            return "ArrayAgg";
        case QueryType::QArrayAgg:
            return "QArrayAgg";
        case QueryType::ShaAgg:
            return "ShaAgg";
        case QueryType::QShaAgg:
            return "QShaAgg";
        case QueryType::StringAgg:
            return "StringAgg";
        case QueryType::QStringAgg:
            return "QStringAgg";
        default:
            throw NotImplementedException("Not implemented Query Type in toString");
    }
}

string QueryBuilder::QueryTypeToApproachString(const QueryType type, const AggType agg) {
    std::stringstream ss;
    switch (type) {
        case QueryType::ArrayAgg:
        case QueryType::QArrayAgg:
            ss << "ArrayAgg";
            break;
        case QueryType::ShaAgg:
        case QueryType::QShaAgg:
            ss << "ShaAgg";
            break;
        case QueryType::StringAgg:
        case QueryType::QStringAgg:
            ss << "StringAgg";
            break;
        default:
            throw NotImplementedException("Not implemented Query Type in toString");
    }

    switch (agg) {
        case AggType::None:
            break;
        case AggType::Count:
            ss <<"-Count";
            break;
        case AggType::Sum:
            ss <<"-Sum";
            break;
        case AggType::Readable:
            ss <<"-Readable";
            break;
        default:
            throw NotImplementedException("Not implemented Query Aggregate in QueryTypeToApproachString");
    }

    return ss.str();
}

string QueryBuilder::QueryTypeToQueryString(const QueryType type) {
    switch (type) {
        case QueryType::StringAgg:
        case QueryType::ArrayAgg:
        case QueryType::ShaAgg:
            return "Q1";
        case QueryType::QStringAgg:
        case QueryType::QArrayAgg:
        case QueryType::QShaAgg:
            return "Q2";
        default:
            throw NotImplementedException("Not implemented Query Type in toString");
    }
}

QueryType QueryBuilder::StringToQueryType(const string &type){
    if(type == "ArrayAgg")
        return QueryType::ArrayAgg;
    else if(type == "QArrayAgg")
        return QueryType::QArrayAgg;
    else if(type == "ShaAgg")
        return QueryType::ShaAgg;
    else if(type == "QShaAgg")
        return QueryType::QShaAgg;
    else if(type == "StringAgg")
        return QueryType::StringAgg;
    else if(type == "QStringAgg")
        return QueryType::QStringAgg;
    else
        throw NotImplementedException("Not implemented Query Type for given type");
}

unique_ptr<SelectNode> QueryBuilder::GetCountNode(const AggType aggT) {
    auto select_node = make_unique<SelectNode>();

    // FROM
    auto from = make_unique<BaseTableRef>();
    from->schema_name = DEFAULT_SCHEMA;
    from->table_name = "activity_sequences";

    select_node->from_table = move(from);

    // GROUP BY
    select_node->groups.emplace_back(make_unique<ColumnRefExpression>("activity_sequence", ""));

    // SELECT
    vector<unique_ptr<ParsedExpression>> children;
    auto func = make_unique<FunctionExpression>(DEFAULT_SCHEMA, "count", children, false);
    func->alias = "occurrences";
    select_node->select_list.emplace_back(move(func));
    select_node->select_list.emplace_back(make_unique<ColumnRefExpression>("activity_sequence", ""));

    // Add aggregates
    switch (aggT) {
        case AggType::None:
            break;
        case AggType::Count: {
            select_node->groups.emplace_back(make_unique<ColumnRefExpression>("case_length", ""));
            select_node->select_list.emplace_back(make_unique<ColumnRefExpression>("case_length", ""));
            break;
        }
        case AggType::Sum: {
            select_node->groups.emplace_back(make_unique<ColumnRefExpression>("activities_sum", ""));
            select_node->select_list.emplace_back(make_unique<ColumnRefExpression>("activities_sum", ""));
            break;
        }
        case AggType::Readable: {
            select_node->groups.emplace_back(make_unique<ColumnRefExpression>("trace", ""));
            select_node->select_list.emplace_back(make_unique<ColumnRefExpression>("trace", ""));
            break;
        }
        default:
            throw NotImplementedException("Not implemented Aggregation Type");

    }

    //
    return select_node;
}

unique_ptr<SelectNode> QueryBuilder::GetProcessAggNode(const string &type, const AggType aggT, const int qualifier) const {
    auto cte_node = make_unique<SelectNode>();

    // FROM
    auto from = make_unique<BaseTableRef>();
    from->schema_name = DEFAULT_SCHEMA;
    from->table_name = "eventlog";
    cte_node->from_table = move(from);

    if (qualifier != -1) { // WHERE
        auto left = make_unique<ColumnRefExpression>("timestamp", "");
        auto right = make_unique<ConstantExpression>(SQLType::INTEGER, qualifier);
        auto toBeQualfied = make_unique<ColumnRefExpression>("case_id", "");
        cte_node->where_clause = make_unique<QualifyingExpression>(
                ExpressionType::COMPARE_GREATERTHANQUALIFYING, move(left), move(right), move(toBeQualfied));
    }
    // GROUP BY
    cte_node->groups.emplace_back(make_unique<ColumnRefExpression>("case_id", ""));

    cte_node->select_list.emplace_back(make_unique<ColumnRefExpression>("case_id", ""));

    vector<unique_ptr<ParsedExpression>> children;
    children.emplace_back(make_unique<ColumnRefExpression>("activity", ""));
    auto func = make_unique<FunctionExpression>(DEFAULT_SCHEMA, type, children, false);
    func->alias = "activity_sequence";
    cte_node->select_list.emplace_back(move(func));

    children.clear();
    // Add aggregates
    switch (aggT) {
        case AggType::None:
            break;
        case AggType::Count: {
            children.emplace_back(make_unique<ColumnRefExpression>("case_id", ""));
            func = make_unique<FunctionExpression>(DEFAULT_SCHEMA, "count", children, false);
            func->alias = "case_length";
            cte_node->select_list.emplace_back(move(func));
            break;
        }
        case AggType::Sum: {
            children.emplace_back(make_unique<ColumnRefExpression>("case_id", ""));
            func = make_unique<FunctionExpression>(DEFAULT_SCHEMA, "sum", children, false);
            func->alias = "activities_sum";
            cte_node->select_list.emplace_back(move(func));
            break;
        }
        case AggType::Readable: {
            children.emplace_back(make_unique<ColumnRefExpression>("activity", ""));
            func = make_unique<FunctionExpression>(DEFAULT_SCHEMA, "process_agg_array", children, false);
            func->alias = "trace";
            cte_node->select_list.emplace_back(move(func));
            break;
        }
        default:
            throw NotImplementedException("Not implemented Aggregation Type");

    }
    return cte_node;
}

unique_ptr<SelectNode> QueryBuilder::GetStringAggNode() const {
    auto cte_node = make_unique<SelectNode>();

    // FROM
    auto from = make_unique<BaseTableRef>();
    from->schema_name = DEFAULT_SCHEMA;
    from->table_name = "eventlog";

    cte_node->from_table = move(from);

    // GROUP BY
    cte_node->groups.emplace_back(make_unique<ColumnRefExpression>("case_id", ""));

    // SELECT
    cte_node->select_list.emplace_back(make_unique<ColumnRefExpression>("case_id", ""));

    vector<unique_ptr<ParsedExpression>> children;
    children.emplace_back(make_unique<ColumnRefExpression>("activity", ""));
    children.emplace_back(make_unique<ConstantExpression>(SQLType::VARCHAR, Value(delimiter)));
    auto func = make_unique<FunctionExpression>(DEFAULT_SCHEMA, "string_agg", children, false);
    func->alias = "activity_sequence";
    cte_node->select_list.emplace_back(move(func));

    return cte_node;
}

unique_ptr<SelectNode> QueryBuilder::GetQualifyingCasesNode(const int qualifier) const {
    auto cte_node = make_unique<SelectNode>();

    // FROM
    auto from = make_unique<BaseTableRef>();
    from->schema_name = DEFAULT_SCHEMA;
    from->table_name = "eventlog";
    cte_node->from_table = move(from);

    // WHERE
    auto left = make_unique<ColumnRefExpression>("timestamp", "");
    auto right = make_unique<ConstantExpression>(SQLType::INTEGER, qualifier);
    cte_node->where_clause = make_unique<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, move(left),
                                                               move(right));

    // SELECT
    cte_node->select_list.emplace_back(make_unique<ColumnRefExpression>("case_id", ""));
    cte_node->select_distinct = true;

    return cte_node;
}

unique_ptr<SelectNode> QueryBuilder::GetQStringAggNode() const {
    auto cte_node = make_unique<SelectNode>();

    {   // FROM
        auto from = make_unique<CrossProductRef>();

        auto left = make_unique<BaseTableRef>();
        left->schema_name = DEFAULT_SCHEMA;
        left->table_name = "eventlog";
        left->alias = "el";
        from->left = move(left);

        auto right = make_unique<BaseTableRef>();
        right->schema_name = DEFAULT_SCHEMA;
        right->table_name = "qualifying_cases";
        right->alias = "qc";
        from->right = move(right);

        cte_node->from_table = move(from);
    }
    // GROUP BY
    cte_node->groups.emplace_back(make_unique<ColumnRefExpression>("case_id", "el"));

    {   // WHERE
        auto left = make_unique<ColumnRefExpression>("case_id", "qc");
        auto right = make_unique<ColumnRefExpression>("case_id", "el");
        cte_node->where_clause = make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, move(left),
                                                                   move(right));
    }
    {   // SELECT
        cte_node->select_list.emplace_back(make_unique<ColumnRefExpression>("case_id", "el"));

        vector<unique_ptr<ParsedExpression>> children;
        children.emplace_back(make_unique<ColumnRefExpression>("activity", "el"));
        children.emplace_back(make_unique<ConstantExpression>(SQLType::VARCHAR, Value(delimiter)));
        auto func = make_unique<FunctionExpression>(DEFAULT_SCHEMA, "string_agg", children, false);
        func->alias = "activity_sequence";
        cte_node->select_list.emplace_back(move(func));
    }
    return cte_node;
}
