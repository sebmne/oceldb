from oceldb.ast.aggregation import (
    AvgAgg,
    CountAgg,
    CountDistinctAgg,
    MaxAgg,
    MinAgg,
    SumAgg,
)
from oceldb.ast.base import Expr


def count() -> CountAgg:
    return CountAgg()


def count_distinct(expr: Expr) -> CountDistinctAgg:
    return CountDistinctAgg(expr)


def min_(expr: Expr) -> MinAgg:
    return MinAgg(expr)


def max_(expr: Expr) -> MaxAgg:
    return MaxAgg(expr)


def sum_(expr: Expr) -> SumAgg:
    return SumAgg(expr)


def avg(expr: Expr) -> AvgAgg:
    return AvgAgg(expr)
