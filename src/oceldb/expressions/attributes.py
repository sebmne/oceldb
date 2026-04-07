from __future__ import annotations

from typing import Any, Optional

from oceldb.expressions._utils import python_type_to_sql_type
from oceldb.expressions.context import Context
from oceldb.expressions.scalar import ScalarExpr


def attr(name: str, cast: Optional[type[Any]] = None) -> AttrExpr:
    """
    Build an attribute expression for the current scope.

    Examples:
        attr("Weight")
        attr("Weight", cast=float)
        attr("created_at", cast=datetime)
        attr("Weight", cast="DOUBLE")
    """
    return AttrExpr(name=name, cast=cast)


class AttrExpr(ScalarExpr):
    """
    Expression representing an attribute in the current scope.

    The expression reads from the `attributes` JSON column of the current row
    alias in the compilation context.

    Examples:
        attr("Weight")
        attr("Weight", cast=float)
        attr("created_at", cast=datetime)
        attr("Weight", cast="DOUBLE")

    Notes:
        - Without `cast`, the extracted value is treated as text.
        - With `cast`, TRY_CAST is used so invalid values become NULL instead of
          raising a conversion error.
    """

    def __init__(self, name: str, cast: Optional[type[Any]] = None) -> None:
        if not name:
            raise ValueError("Attribute name must not be empty")

        self.name = name
        self.cast = python_type_to_sql_type(cast)

    def to_sql(self, ctx: Context) -> str:
        """
        Compile this attribute access into a SQL scalar expression.

        Uses the current row alias from the context and reads from the
        `attributes` JSON column.
        """
        escaped_name = self.name.replace("'", "''")
        base = f"{ctx.alias}.attributes->>'{escaped_name}'"

        if self.cast is not None:
            return f"TRY_CAST({base} AS {self.cast})"

        return base

    def __repr__(self) -> str:
        if self.cast is None:
            return f"AttrExpr(name={self.name!r})"
        return f"AttrExpr(name={self.name!r}, cast={self.cast!r})"
