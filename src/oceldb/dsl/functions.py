from __future__ import annotations

from typing import Any

from oceldb.ast.base import FunctionExpr, LiteralExpr, LiteralValue, ScalarExpr, ScalarValue


def lit(value: Any) -> LiteralExpr:
    """
    Build a literal expression.
    """
    return LiteralExpr(value=value)


def coalesce(*values: ScalarExpr | LiteralValue) -> FunctionExpr:
    """
    Build a COALESCE expression.
    """
    if not values:
        raise ValueError("coalesce(...) requires at least one value")
    return FunctionExpr(name="coalesce", args=tuple(values))


def abs_(expr: ScalarExpr) -> FunctionExpr:
    """
    Build an ABS expression.
    """
    return FunctionExpr(name="abs", args=(expr,))


def round_(expr: ScalarExpr, decimals: ScalarValue = 0) -> FunctionExpr:
    """
    Build a ROUND expression.
    """
    return FunctionExpr(name="round", args=(expr, decimals))
