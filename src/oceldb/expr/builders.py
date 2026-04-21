"""User-facing expression builders.

These functions produce AST nodes from ``oceldb.expr.nodes``. Builders with a
natural left-hand side (``.is_null``, ``.cast``, ``.str.upper``) are methods
on ``Expr``; everything else lives here as a module-level function.
"""

from __future__ import annotations

from typing import Any

from oceldb.expr.nodes import (
    AvgAgg,
    CaseBuilder,
    ColumnExpr,
    CountAgg,
    Expr,
    FunctionExpr,
    LiteralExpr,
    Literal_,
    MaxAgg,
    MinAgg,
    SortExpr,
    SumAgg,
    WindowBuilder,
    lift_expr,
)


def col(name: str) -> ColumnExpr:
    return ColumnExpr(name=name)


def lit(value: Literal_) -> LiteralExpr:
    return LiteralExpr(value=value)


def _coerce(value: Any) -> Expr:
    if isinstance(value, Expr):
        return value
    return lift_expr(value)


def coalesce(*values: Expr | Literal_) -> FunctionExpr:
    if not values:
        raise TypeError("coalesce(...) requires at least one argument")
    return FunctionExpr(name="COALESCE", args=tuple(_coerce(v) for v in values))


def abs_(expr: Expr) -> FunctionExpr:
    return FunctionExpr(name="ABS", args=(expr,))


def round_(expr: Expr, digits: int = 0) -> FunctionExpr:
    return FunctionExpr(name="ROUND", args=(expr, LiteralExpr(value=digits)))


def when(predicate: Expr) -> CaseBuilder:
    return CaseBuilder(branches=(), default=None, pending=predicate)


# --- aggregates -------------------------------------------------------------


def count(expr: Expr | None = None) -> CountAgg:
    return CountAgg(expr=expr, distinct=False)


def count_distinct(expr: Expr) -> CountAgg:
    return CountAgg(expr=expr, distinct=True)


def sum_(expr: Expr) -> SumAgg:
    return SumAgg(expr=expr)


def avg(expr: Expr) -> AvgAgg:
    return AvgAgg(expr=expr)


def min_(expr: Expr) -> MinAgg:
    return MinAgg(expr=expr)


def max_(expr: Expr) -> MaxAgg:
    return MaxAgg(expr=expr)


# --- windows ---------------------------------------------------------------


def row_number() -> WindowBuilder:
    return WindowBuilder(name="ROW_NUMBER", args=())


# --- sort ------------------------------------------------------------------


def asc(value: str | Expr) -> SortExpr:
    expr = ColumnExpr(name=value) if isinstance(value, str) else value
    return SortExpr(expr=expr, descending=False)


def desc(value: str | Expr) -> SortExpr:
    expr = ColumnExpr(name=value) if isinstance(value, str) else value
    return SortExpr(expr=expr, descending=True)


__all__ = [
    "abs_",
    "asc",
    "avg",
    "coalesce",
    "col",
    "count",
    "count_distinct",
    "desc",
    "lit",
    "max_",
    "min_",
    "round_",
    "row_number",
    "sum_",
    "when",
]
