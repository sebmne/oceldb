from __future__ import annotations

from oceldb.ast.base import AliasExpr, ScalarExpr, SortExpr


def asc(expr: ScalarExpr | AliasExpr | str) -> SortExpr:
    return SortExpr(expr=expr, descending=False)


def desc(expr: ScalarExpr | AliasExpr | str) -> SortExpr:
    return SortExpr(expr=expr, descending=True)
