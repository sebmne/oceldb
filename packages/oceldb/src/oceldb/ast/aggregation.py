from dataclasses import dataclass

from oceldb.ast.base import AggregateExpr, Expr


@dataclass(frozen=True)
class CountAgg(AggregateExpr):
    pass


@dataclass(frozen=True)
class CountDistinctAgg(AggregateExpr):
    expr: Expr


@dataclass(frozen=True)
class MinAgg(AggregateExpr):
    expr: Expr


@dataclass(frozen=True)
class MaxAgg(AggregateExpr):
    expr: Expr


@dataclass(frozen=True)
class SumAgg(AggregateExpr):
    expr: Expr


@dataclass(frozen=True)
class AvgAgg(AggregateExpr):
    expr: Expr
