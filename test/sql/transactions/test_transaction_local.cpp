#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test operations on transaction local data", "[transactions]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// perform different operations on the same data within one transaction
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)"));

	// append
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 3), (2, 3)"));

	result = con.Query("SELECT * FROM integers ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 3}));

	// update
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET j=5 WHERE i=2"));

	result = con.Query("SELECT * FROM integers ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 5}));

	// delete
	REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i=2"));

	result = con.Query("SELECT * FROM integers ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));

	// commit
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	// we can still read the table now
	result = con.Query("SELECT * FROM integers ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
}

TEST_CASE("Test appends on transaction local data with unique indices", "[transactions]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, j INTEGER)"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 3)"));

	// append only
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (1, 2)"));
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	// if we delete we can insert that value again
	REQUIRE_NO_FAIL(con.Query("DELETE FROM integers"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 2)"));

	// // delete + append in same transaction should work as well
	// FIXME: should this work? or can we get away with this not working?
	// REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	// REQUIRE_NO_FAIL(con.Query("DELETE FROM integers"));
	// REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 2)"));
	// REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
}

TEST_CASE("Test appends with multiple transactions", "[transactions]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)"));

	// begin two transactions
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));

	// append a tuple, con2 cannot see this tuple yet
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 3)"));
	result = con2.Query("SELECT COUNT(*) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con2.Query("SELECT COUNT(*) FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));

	// after committing, con2 still cannot see this tuple
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
	result = con2.Query("SELECT COUNT(*) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con2.Query("SELECT COUNT(*) FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));

	// after con2 commits, it can see this tuple
	REQUIRE_NO_FAIL(con2.Query("COMMIT"));
	result = con2.Query("SELECT COUNT(*) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con2.Query("SELECT COUNT(*) FROM integers WHERE i=1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// now both transactions append one tuple
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 3)"));
	REQUIRE_NO_FAIL(con2.Query("INSERT INTO integers VALUES (1, 3)"));

	// they cannot see each others tuple yet
	result = con.Query("SELECT COUNT(*) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	result = con2.Query("SELECT COUNT(*) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	// until they both commit
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
	REQUIRE_NO_FAIL(con2.Query("COMMIT"));

	result = con.Query("SELECT COUNT(*) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	result = con2.Query("SELECT COUNT(*) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 3, 3}));
}

TEST_CASE("Test operations on transaction local data with unique indices", "[transactions]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// perform different operations on the same data within one transaction
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, j INTEGER)"));

	// append
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 3), (2, 3)"));

	result = con.Query("SELECT * FROM integers ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 3}));

	// appending the same value again fails
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (1, 2)"));
	// updating also fails if there is a conflict
	REQUIRE_FAIL(con.Query("UPDATE integers SET i=1 WHERE i=2"));
	// but not if there is no conflict
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=3 WHERE i=2"));

	result = con.Query("SELECT * FROM integers ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 3}));

	// if we delete, we can insert the value again
	REQUIRE_NO_FAIL(con.Query("DELETE FROM integers WHERE i=1"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 3)"));

	result = con.Query("SELECT * FROM integers ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 3}));

	// commit
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	// we can still read the table now
	result = con.Query("SELECT * FROM integers ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 3}));
}