//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression/bound_qualifying_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"

namespace duckdb {

    class BoundQualifyingExpression : public Expression {
    public:
        BoundQualifyingExpression(ExpressionType type, unique_ptr<Expression> left, unique_ptr<Expression> right, unique_ptr<Expression> toBeQualfied);

        unique_ptr<Expression> left;
        unique_ptr<Expression> right;
        unique_ptr<Expression> toBeQualfied;
    public:
        string ToString() const override;

        bool Equals(const BaseExpression *other) const override;

        unique_ptr<Expression> Copy() override;
    };
} // namespace duckdb
