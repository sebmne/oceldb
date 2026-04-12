from __future__ import annotations

from oceldb.ast.field import ColumnExpr


def col(name: str) -> ColumnExpr:
    """
    Build a typed column expression in the current query scope.
    """
    if not name:
        raise ValueError("Column name must not be empty")
    return ColumnExpr(name=name)
