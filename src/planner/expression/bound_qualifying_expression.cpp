#include "duckdb/planner/expression/bound_qualifying_expression.hpp"

using namespace duckdb;
using namespace std;

BoundQualifyingExpression::BoundQualifyingExpression(ExpressionType type, unique_ptr<Expression> left,
                                                     unique_ptr<Expression> right,
                                                     unique_ptr<Expression> toBeQualified)
        : Expression(type, ExpressionClass::BOUND_QUALIFYING, TypeId::BOOLEAN), left(move(left)), right(move(right)), toBeQualfied(move(toBeQualified)) {
}

string BoundQualifyingExpression::ToString() const {
    return left->GetName() + ExpressionTypeToOperator(type) + right->GetName() + " qualifies " + toBeQualfied->ToString();
}

bool BoundQualifyingExpression::Equals(const BaseExpression *other_) const {
    if (!BaseExpression::Equals(other_)) {
        return false;
    }
    auto other = (BoundQualifyingExpression *)other_;
    if (!Expression::Equals(left.get(), other->left.get())) {
        return false;
    }
    if (!Expression::Equals(right.get(), other->right.get())) {
        return false;
    }
    if (!Expression::Equals(toBeQualfied.get(), other->toBeQualfied.get())) {
        return false;
    }
    return true;
}

unique_ptr<Expression> BoundQualifyingExpression::Copy() {
    auto copy = make_unique<BoundQualifyingExpression>(type, left->Copy(), right->Copy(), toBeQualfied->Copy());
    copy->CopyProperties(*this);
    return move(copy);
}
