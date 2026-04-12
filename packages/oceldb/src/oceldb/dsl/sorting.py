from __future__ import annotations

from oceldb.ast.base import Expr, SortExpr


def asc(expr: Expr | str) -> SortExpr:
    return SortExpr(expr=expr, descending=False)


def desc(expr: Expr | str) -> SortExpr:
    return SortExpr(expr=expr, descending=True)
