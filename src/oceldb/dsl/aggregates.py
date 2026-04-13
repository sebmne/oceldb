from oceldb.ast.aggregation import (
    AvgAgg,
    CountAgg,
    CountDistinctAgg,
    MaxAgg,
    MinAgg,
    SumAgg,
)
from oceldb.ast.base import ScalarExpr


def count() -> CountAgg:
    return CountAgg()


def count_distinct(expr: ScalarExpr) -> CountDistinctAgg:
    return CountDistinctAgg(expr)


def min_(expr: ScalarExpr) -> MinAgg:
    return MinAgg(expr)


def max_(expr: ScalarExpr) -> MaxAgg:
    return MaxAgg(expr)


def sum_(expr: ScalarExpr) -> SumAgg:
    return SumAgg(expr)


def avg(expr: ScalarExpr) -> AvgAgg:
    return AvgAgg(expr)
