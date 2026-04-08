from __future__ import annotations

from oceldb.ast.base import Expr, OrderExpr


def asc(expr: Expr | str) -> OrderExpr:
    """
    Build an ascending order specification.
    """
    return OrderExpr(expr=expr, direction="ASC")


def desc(expr: Expr | str) -> OrderExpr:
    """
    Build a descending order specification.
    """
    return OrderExpr(expr=expr, direction="DESC")
