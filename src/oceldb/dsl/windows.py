from __future__ import annotations

from oceldb.ast.base import WindowFunctionExpr


def row_number() -> WindowFunctionExpr:
    """
    Build a ROW_NUMBER window expression.
    """
    return WindowFunctionExpr(name="row_number")
