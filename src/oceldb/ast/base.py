from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Union, final


class Expr(ABC):
    """
    Base class for all DSL expression nodes.
    """

    def as_(self, alias: str) -> AliasedExpr:
        if not alias:
            raise ValueError("Alias must not be empty")
        return AliasedExpr(expr=self, alias=alias)


class ScalarExpr(Expr):
    """
    Base class for scalar expressions.
    """

    def __eq__(self, other: CompareValue) -> CompareExpr:  # type: ignore[override]
        return CompareExpr(self, "=", other)

    def __ne__(self, other: CompareValue) -> CompareExpr:  # type: ignore[override]
        return CompareExpr(self, "!=", other)

    def __gt__(self, other: CompareValue) -> CompareExpr:
        return CompareExpr(self, ">", other)

    def __ge__(self, other: CompareValue) -> CompareExpr:
        return CompareExpr(self, ">=", other)

    def __lt__(self, other: CompareValue) -> CompareExpr:
        return CompareExpr(self, "<", other)

    def __le__(self, other: CompareValue) -> CompareExpr:
        return CompareExpr(self, "<=", other)

    def is_null(self) -> UnaryPredicate:
        return UnaryPredicate(self, "IS NULL")

    def not_null(self) -> UnaryPredicate:
        return UnaryPredicate(self, "IS NOT NULL")


class BoolExpr(Expr):
    """
    Base class for boolean expressions.
    """

    def __and__(self, other: BoolExpr) -> AndExpr:
        return AndExpr(self, other)

    def __or__(self, other: BoolExpr) -> OrExpr:
        return OrExpr(self, other)

    def __invert__(self) -> NotExpr:
        return NotExpr(self)


class AggregateExpr(Expr):
    """
    Base class for aggregate expressions.
    """


@dataclass(frozen=True)
class AliasedExpr(Expr):
    expr: Expr
    alias: str


@dataclass(frozen=True)
class OrderExpr:
    expr: Expr | str
    direction: str


CompareValue = Union[Expr, None, bool, int, float, str]


@final
@dataclass(frozen=True)
class CompareExpr(BoolExpr):
    left: ScalarExpr
    op: str
    right: CompareValue


@final
@dataclass(frozen=True)
class UnaryPredicate(BoolExpr):
    expr: ScalarExpr
    op: str


@final
@dataclass(frozen=True)
class AndExpr(BoolExpr):
    left: BoolExpr
    right: BoolExpr


@final
@dataclass(frozen=True)
class OrExpr(BoolExpr):
    left: BoolExpr
    right: BoolExpr


@final
@dataclass(frozen=True)
class NotExpr(BoolExpr):
    expr: BoolExpr
