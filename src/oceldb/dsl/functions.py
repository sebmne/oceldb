from __future__ import annotations

from typing import Any

from oceldb.ast.base import LiteralExpr


def lit(value: Any) -> LiteralExpr:
    """
    Build a literal expression.
    """
    return LiteralExpr(value=value)
