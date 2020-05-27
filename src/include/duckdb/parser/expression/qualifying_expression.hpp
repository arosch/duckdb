//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/qualifying_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {
//! QualifyingExpression represents a boolean comparison (e.g. =, >=, <>). Always returns a boolean
//! and has three children.
    class QualifyingExpression : public ParsedExpression {
    public:
        QualifyingExpression(ExpressionType type, unique_ptr<ParsedExpression> left, unique_ptr<ParsedExpression> right, unique_ptr<ParsedExpression> toBeQualified);

        unique_ptr<ParsedExpression> left;
        unique_ptr<ParsedExpression> right;
        unique_ptr<ParsedExpression> toBeQualfied;

    public:
        string ToString() const override;

        static bool Equals(const QualifyingExpression *a, const QualifyingExpression *b);

        unique_ptr<ParsedExpression> Copy() const override;

        void Serialize(Serializer &serializer) override;
        static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, Deserializer &source);
    };
} // namespace duckdb

