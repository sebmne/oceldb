from __future__ import annotations

from dataclasses import dataclass

from oceldb.ast.base import AggregateExpr, ExprVisitor, ScalarExpr, T


@dataclass(frozen=True, eq=False)
class CountAgg(AggregateExpr):
    def accept(self, visitor: ExprVisitor[T]) -> T:
        visit = getattr(visitor, "visit_count")
        return visit(self)


@dataclass(frozen=True, eq=False)
class CountDistinctAgg(AggregateExpr):
    expr: ScalarExpr

    def accept(self, visitor: ExprVisitor[T]) -> T:
        visit = getattr(visitor, "visit_count_distinct")
        return visit(self)


@dataclass(frozen=True, eq=False)
class MinAgg(AggregateExpr):
    expr: ScalarExpr

    def accept(self, visitor: ExprVisitor[T]) -> T:
        visit = getattr(visitor, "visit_min")
        return visit(self)


@dataclass(frozen=True, eq=False)
class MaxAgg(AggregateExpr):
    expr: ScalarExpr

    def accept(self, visitor: ExprVisitor[T]) -> T:
        visit = getattr(visitor, "visit_max")
        return visit(self)


@dataclass(frozen=True, eq=False)
class SumAgg(AggregateExpr):
    expr: ScalarExpr

    def accept(self, visitor: ExprVisitor[T]) -> T:
        visit = getattr(visitor, "visit_sum")
        return visit(self)


@dataclass(frozen=True, eq=False)
class AvgAgg(AggregateExpr):
    expr: ScalarExpr

    def accept(self, visitor: ExprVisitor[T]) -> T:
        visit = getattr(visitor, "visit_avg")
        return visit(self)
