from __future__ import annotations

from typing import Iterable

from oceldb.expressions.base import Expr
from oceldb.expressions.context import Context


def _sql_literal(value: object) -> str:
    """Render a Python value as a SQL literal."""

    match value:
        case Expr():
            raise TypeError("_sql_literal() does not accept Expr values")
        case None:
            return "NULL"
        case bool():
            return "TRUE" if value else "FALSE"
        case str():
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        case _:
            return str(value)


class CompareExpr(Expr):
    """Binary comparison such as a = b or a > b."""

    def __init__(self, left: Expr, op: str, right: object) -> None:
        self.left = left
        self.op = op
        self.right = right

    def to_sql(self, ctx: Context) -> str:
        if isinstance(self.right, Expr):
            right_sql = self.right.to_sql(ctx)
        else:
            right_sql = _sql_literal(self.right)

        # SQL null semantics
        if self.right is None:
            if self.op == "=":
                return IsNullExpr(self.left).to_sql(ctx)
            if self.op == "!=":
                return NotNullExpr(self.left).to_sql(ctx)

        return f"({self.left.to_sql(ctx)} {self.op} {right_sql})"


class IsNullExpr(Expr):
    def __init__(self, expr: Expr) -> None:
        self.expr = expr

    def to_sql(self, ctx: Context) -> str:
        return f"({self.expr.to_sql(ctx)} IS NULL)"


class NotNullExpr(Expr):
    def __init__(self, expr: Expr) -> None:
        self.expr = expr

    def to_sql(self, ctx: Context) -> str:
        return f"({self.expr.to_sql(ctx)} IS NOT NULL)"


class InExpr(Expr):
    def __init__(self, expr: Expr, values: Iterable[object]) -> None:
        self.expr = expr
        self.values = tuple(values)

    def to_sql(self, ctx: Context) -> str:
        if not self.values:
            return "(FALSE)"

        literals = ", ".join(_sql_literal(v) for v in self.values)
        return f"({self.expr.to_sql(ctx)} IN ({literals}))"


class BetweenExpr(Expr):
    def __init__(self, expr: Expr, low: object, high: object) -> None:
        self.expr = expr
        self.low = low
        self.high = high

    def to_sql(self, ctx: Context) -> str:
        return (
            f"({self.expr.to_sql(ctx)} BETWEEN "
            f"{_sql_literal(self.low)} AND {_sql_literal(self.high)})"
        )
