from __future__ import annotations

from typing import Any, Optional

from oceldb.expressions._utils import python_type_to_sql_type
from oceldb.expressions.context import Context
from oceldb.expressions.scalar import ScalarExpr


def field(name: str, cast: Optional[type[Any]] = None) -> "FieldExpr":
    """
    Build an expression for a fixed field in the current scope.

    Examples:
        field("ocel_type")
        field("ocel_time", cast=datetime)
        field("ocel_time", cast="TIMESTAMP")
    """
    return FieldExpr(name=name, cast=cast)


class FieldExpr(ScalarExpr):
    """
    Expression representing a fixed OCEL field in the current scope.

    Unlike `attr(...)`, this reads a real SQL column directly from the current
    alias rather than from the JSON payload.

    Examples:
        field("ocel_type")
        field("ocel_time", cast=datetime)
        field("ocel_time", cast="TIMESTAMP")

    Notes:
        - Without `cast`, the extracted value is treated as text.
        - With `cast`, TRY_CAST is used so invalid values become NULL instead of
        raising a conversion error.
    """

    def __init__(self, name: str, cast: Optional[type[Any]] = None) -> None:
        if not name:
            raise ValueError("Field name must not be empty")

        self.name = name
        self.cast = python_type_to_sql_type(cast)

    def to_sql(self, ctx: Context) -> str:
        base = f"{ctx.alias}.{self.name}"

        if self.cast is not None:
            return f"TRY_CAST({base} AS {self.cast})"

        return base

    def __repr__(self) -> str:
        if self.cast is None:
            return f"FieldExpr(name={self.name!r})"
        return f"FieldExpr(name={self.name!r}, cast={self.cast!r})"
