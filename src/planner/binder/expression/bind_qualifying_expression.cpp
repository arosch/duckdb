#include "duckdb/parser/expression/qualifying_expression.hpp"
#include "duckdb/planner/expression/bound_qualifying_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

using namespace duckdb;
using namespace std;

BindResult ExpressionBinder::BindExpression(QualifyingExpression &expr, index_t depth) {
    // first try to bind the children of the case expression
    string error;
    BindChild(expr.left, depth, error);
    BindChild(expr.right, depth, error);
    BindChild(expr.toBeQualfied, depth, error);
    if (!error.empty()) {
        return BindResult(error);
    }
    // the children have been successfully resolved
    auto &left = (BoundExpression &)*expr.left;
    auto &right = (BoundExpression &)*expr.right;
    auto &toBeQualified = (BoundExpression &)*expr.toBeQualfied;
    // cast the input types to the same type
    // now obtain the result type of the input types
    auto input_type = MaxSQLType(left.sql_type, right.sql_type);
    if (input_type.id == SQLTypeId::UNKNOWN) {
        throw BinderException("Could not determine type of parameters: try adding explicit type casts");
    }
    // add casts (if necessary)
    left.expr = AddCastToType(move(left.expr), left.sql_type, input_type);
    right.expr = AddCastToType(move(right.expr), right.sql_type, input_type);
    //toBeQualified.expr = AddCastToType(move(toBeQualified.expr), toBeQualified.sql_type, input_type);

    // now create the bound comparison expression
    return BindResult(make_unique<BoundQualifyingExpression>(expr.type, move(left.expr), move(right.expr), move(toBeQualified.expr)),
                      SQLType(SQLTypeId::BOOLEAN));
}
