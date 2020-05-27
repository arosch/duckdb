//===----------------------------------------------------------------------===//
//                         DuckDB
//
// main/client_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/transaction/transaction_context.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include <random>

namespace duckdb {
class Catalog;
class DuckDB;

//! The ClientContext holds information relevant to the current client session
//! during execution
class ClientContext {
public:
	ClientContext(DuckDB &database);

	//! Query profiler
	QueryProfiler profiler;
	//! The database that this client is connected to
	DuckDB &db;
	//! Data for the currently running transaction
	TransactionContext transaction;
	//! Whether or not the query is interrupted
	bool interrupted;
	//! Whether or not the ClientContext has been invalidated because the underlying database is destroyed
	bool is_invalidated = false;
	//! Lock on using the ClientContext in parallel
	std::mutex context_lock;

	ExecutionContext execution_context;

	Catalog &catalog;
	unique_ptr<SchemaCatalogEntry> temporary_objects;
	unique_ptr<CatalogSet> prepared_statements;

	// Whether or not aggressive query verification is enabled
	bool query_verification_enabled = false;
	//! Enable the running of optimizers
	bool enable_optimizer = true;

	//! The random generator used by random(). Its seed value can be set by setseed().
	std::mt19937 random_engine;

public:
	Transaction &ActiveTransaction() {
		return transaction.ActiveTransaction();
	}

	//! Interrupt execution of a query
	void Interrupt();
	//! Enable query profiling
	void EnableProfiling();
	//! Disable query profiling
	void DisableProfiling();

	//! Issue a query, returning a QueryResult. The QueryResult can be either a StreamQueryResult or a
	//! MaterializedQueryResult. The StreamQueryResult will only be returned in the case of a successful SELECT
	//! statement.
	unique_ptr<QueryResult> Query(string query, bool allow_stream_result);
	//! Fetch a query from the current result set (if any)
	unique_ptr<DataChunk> Fetch();
	//! Cleanup the result set (if any).
	void Cleanup();
	//! Invalidate the client context. The current query will be interrupted and the client context will be invalidated,
	//! making it impossible for future queries to run.
	void Invalidate();

	//! Prepare a query
	unique_ptr<PreparedStatement> Prepare(string query);
	//! Execute a prepared statement with the given name and set of parameters
	unique_ptr<QueryResult> Execute(string name, vector<Value> &values, bool allow_stream_result = true);
	//! Removes a prepared statement from the set of prepared statements in the client context
	void RemovePreparedStatement(PreparedStatement *statement);

public:
	//! Perform aggressive query verification of a SELECT statement. Only called when query_verification_enabled is
	//! true.
	string VerifyQuery(string query, unique_ptr<SQLStatement> statement);

	//! Internal clean up, does not lock. Caller must hold the context_lock.
	void CleanupInternal();
	string FinalizeQuery(bool success);
	//! Internal fetch, does not lock. Caller must hold the context_lock.
	unique_ptr<DataChunk> FetchInternal();
	//! Internally execute a set of SQL statement. Caller must hold the context_lock.
	unique_ptr<QueryResult> ExecuteStatementsInternal(string query, vector<unique_ptr<SQLStatement>> &statements,
	                                                  bool allow_stream_result);
	//! Internally execute a SQL statement. Caller must hold the context_lock.
	unique_ptr<QueryResult> ExecuteStatementInternal(string query, unique_ptr<SQLStatement> statement,
	                                                 bool allow_stream_result);

    //! Benchmark functions to measure the execution of a SQL statement. Caller must hold the context_lock.
    unique_ptr<QueryResult> BenchmarkStatement(unique_ptr<SQLStatement> statement, const vector<std::pair<string,string>> & params, double ntuples, bool printHeader);
    unique_ptr<QueryResult> BenchmarkStatementInternal(unique_ptr<SQLStatement> statement, const vector<std::pair<string,string>> & params, double ntuples,  bool printHeader);

private:
	index_t prepare_count = 0;
	//! The currently opened StreamQueryResult (if any)
	StreamQueryResult *open_result = nullptr;
	//! Prepared statement objects that were created using the ClientContext::Prepare method
	unordered_set<PreparedStatement *> prepared_statement_objects;
};
} // namespace duckdb