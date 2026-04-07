from __future__ import annotations

from typing import Optional

from oceldb.expressions.context import Context
from oceldb.expressions.scalar import ScalarExpr


def field(name: str, cast: Optional[str] = None) -> "FieldExpr":
    """
    Build an expression for a fixed field in the current scope.

    Examples:
        field("ocel_type") == "Order"
        field("ocel_time").is_not_null()
        field("ocel_time", cast="TIMESTAMP")
    """
    return FieldExpr(name=name, cast=cast)


class FieldExpr(ScalarExpr):
    """
    Expression representing a fixed OCEL field in the current scope.

    Unlike `attr(...)`, this reads a real SQL column directly from the current
    alias rather than from the JSON payload.
    """

    def __init__(self, name: str, cast: Optional[str] = None) -> None:
        if not name:
            raise ValueError("Field name must not be empty")

        self.name = name
        self.cast = cast

    def to_sql(self, ctx: Context) -> str:
        base = f"{ctx.alias}.{self.name}"

        if self.cast is not None:
            return f"TRY_CAST({base} AS {self.cast})"

        return base

    def __repr__(self) -> str:
        if self.cast is None:
            return f"FieldExpr(name={self.name!r})"
        return f"FieldExpr(name={self.name!r}, cast={self.cast!r})"
