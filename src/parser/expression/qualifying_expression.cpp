#include "duckdb/parser/expression/qualifying_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"

using namespace duckdb;
using namespace std;

QualifyingExpression::QualifyingExpression(ExpressionType type, unique_ptr<ParsedExpression> left,
                                           unique_ptr<ParsedExpression> right,
                                           unique_ptr<ParsedExpression> toBeQualified)
        : ParsedExpression(type, ExpressionClass::QUALIFYING) {
    this->left = move(left);
    this->right = move(right);
    this->toBeQualfied = move(toBeQualified);
}

string QualifyingExpression::ToString() const {
    return left->ToString() + ExpressionTypeToOperator(type) + right->ToString() + " qualifies " + toBeQualfied->ToString();
}

bool QualifyingExpression::Equals(const QualifyingExpression *a, const QualifyingExpression *b) {
    if (!a->left->Equals(b->left.get())) {
        return false;
    }
    if (!a->right->Equals(b->right.get())) {
        return false;
    }
    if(!a->toBeQualfied->Equals(b->toBeQualfied.get())){
        return false;
    }
    return true;
}

unique_ptr<ParsedExpression> QualifyingExpression::Copy() const {
    auto copy = make_unique<QualifyingExpression>(type, left->Copy(), right->Copy(), toBeQualfied->Copy());
    copy->CopyProperties(*this);
    return move(copy);
}

void QualifyingExpression::Serialize(Serializer &serializer) {
    ParsedExpression::Serialize(serializer);
    left->Serialize(serializer);
    right->Serialize(serializer);
    toBeQualfied->Serialize(serializer);
}

unique_ptr<ParsedExpression> QualifyingExpression::Deserialize(ExpressionType type, Deserializer &source) {
    auto left_child = ParsedExpression::Deserialize(source);
    auto right_child = ParsedExpression::Deserialize(source);
    auto qualifying_child = ParsedExpression::Deserialize(source);
    return make_unique<QualifyingExpression>(type, move(left_child), move(right_child), move(qualifying_child));
}


