from __future__ import annotations

from abc import ABC, abstractmethod

from oceldb.expressions.context import Context


class Expr(ABC):
    """Base class for all expressions."""

    @abstractmethod
    def to_sql(self, ctx: Context) -> str:
        """Compile this expression into a SQL fragment."""
        raise NotImplementedError

    def __and__(self, other: "Expr") -> BinaryExpr:
        return BinaryExpr(self, "AND", other)

    def __or__(self, other: "Expr") -> BinaryExpr:
        return BinaryExpr(self, "OR", other)

    def __invert__(self) -> UnaryExpr:
        return UnaryExpr("NOT", self)


class UnaryExpr(Expr):
    """Unary logical expression such as NOT expr."""

    def __init__(self, op: str, expr: Expr) -> None:
        self.op = op
        self.expr = expr

    def to_sql(self, ctx: Context) -> str:
        return f"({self.op} {self.expr.to_sql(ctx)})"


class BinaryExpr(Expr):
    """Binary logical expression such as expr1 AND expr2."""

    def __init__(self, left: Expr, op: str, right: Expr) -> None:
        self.left = left
        self.op = op
        self.right = right

    def to_sql(self, ctx: Context) -> str:
        return f"({self.left.to_sql(ctx)} {self.op} {self.right.to_sql(ctx)})"
