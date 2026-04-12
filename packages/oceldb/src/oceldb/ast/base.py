from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Generic, Iterable, TypeVar, Union, final

T = TypeVar("T")


class ExprVisitor(ABC, Generic[T]):
    @abstractmethod
    def visit_column(self, expr) -> T: ...

    @abstractmethod
    def visit_alias(self, expr: "AliasExpr") -> T: ...

    @abstractmethod
    def visit_literal(self, expr: "LiteralExpr") -> T: ...

    @abstractmethod
    def visit_cast(self, expr: "CastExpr") -> T: ...

    @abstractmethod
    def visit_compare(self, expr: "CompareExpr") -> T: ...

    @abstractmethod
    def visit_unary_predicate(self, expr: "UnaryPredicate") -> T: ...

    @abstractmethod
    def visit_and(self, expr: "AndExpr") -> T: ...

    @abstractmethod
    def visit_or(self, expr: "OrExpr") -> T: ...

    @abstractmethod
    def visit_not(self, expr: "NotExpr") -> T: ...

    @abstractmethod
    def visit_in(self, expr: "InExpr") -> T: ...

    @abstractmethod
    def visit_count(self, expr) -> T: ...

    @abstractmethod
    def visit_count_distinct(self, expr) -> T: ...

    @abstractmethod
    def visit_min(self, expr) -> T: ...

    @abstractmethod
    def visit_max(self, expr) -> T: ...

    @abstractmethod
    def visit_sum(self, expr) -> T: ...

    @abstractmethod
    def visit_avg(self, expr) -> T: ...

    @abstractmethod
    def visit_relation_exists(self, expr) -> T: ...

    @abstractmethod
    def visit_relation_count(self, expr) -> T: ...

    @abstractmethod
    def visit_relation_all(self, expr) -> T: ...


class Expr(ABC):
    def alias(self, name: str) -> "AliasExpr":
        if not name:
            raise ValueError("Alias must not be empty")
        return AliasExpr(expr=self, alias=name)

    @abstractmethod
    def accept(self, visitor: ExprVisitor[T]) -> T:
        raise NotImplementedError


class ScalarExpr(Expr):
    def __eq__(self, other: "CompareValue") -> "CompareExpr":  # type: ignore[override]
        return CompareExpr(self, "=", other)

    def __ne__(self, other: "CompareValue") -> "CompareExpr":  # type: ignore[override]
        return CompareExpr(self, "!=", other)

    def __gt__(self, other: "CompareValue") -> "CompareExpr":
        return CompareExpr(self, ">", other)

    def __ge__(self, other: "CompareValue") -> "CompareExpr":
        return CompareExpr(self, ">=", other)

    def __lt__(self, other: "CompareValue") -> "CompareExpr":
        return CompareExpr(self, "<", other)

    def __le__(self, other: "CompareValue") -> "CompareExpr":
        return CompareExpr(self, "<=", other)

    def is_null(self) -> "UnaryPredicate":
        return UnaryPredicate(self, "IS NULL")

    def not_null(self) -> "UnaryPredicate":
        return UnaryPredicate(self, "IS NOT NULL")

    def is_in(self, values: Iterable["CompareValue"]) -> "InExpr":
        values_tuple = tuple(values)
        if not values_tuple:
            raise ValueError("IN predicate requires at least one value")
        return InExpr(self, values_tuple)

    def cast(self, sql_type: str) -> "CastExpr":
        if not sql_type:
            raise ValueError("Cast type must not be empty")
        return CastExpr(self, sql_type)


class BoolExpr(Expr):
    def __and__(self, other: "BoolExpr") -> "AndExpr":
        return AndExpr(self, other)

    def __or__(self, other: "BoolExpr") -> "OrExpr":
        return OrExpr(self, other)

    def __invert__(self) -> "NotExpr":
        return NotExpr(self)


class AggregateExpr(ScalarExpr):
    pass


@final
@dataclass(frozen=True)
class AliasExpr(Expr):
    expr: Expr
    alias: str

    def accept(self, visitor: ExprVisitor[T]) -> T:
        return visitor.visit_alias(self)


@final
@dataclass(frozen=True, eq=False)
class LiteralExpr(ScalarExpr):
    value: "LiteralValue"

    def accept(self, visitor: ExprVisitor[T]) -> T:
        return visitor.visit_literal(self)


@final
@dataclass(frozen=True, eq=False)
class CastExpr(ScalarExpr):
    expr: ScalarExpr
    sql_type: str

    def accept(self, visitor: ExprVisitor[T]) -> T:
        return visitor.visit_cast(self)


@final
@dataclass(frozen=True)
class CompareExpr(BoolExpr):
    left: ScalarExpr
    op: str
    right: "CompareValue"

    def accept(self, visitor: ExprVisitor[T]) -> T:
        return visitor.visit_compare(self)


@final
@dataclass(frozen=True)
class UnaryPredicate(BoolExpr):
    expr: ScalarExpr
    op: str

    def accept(self, visitor: ExprVisitor[T]) -> T:
        return visitor.visit_unary_predicate(self)


@final
@dataclass(frozen=True)
class AndExpr(BoolExpr):
    left: BoolExpr
    right: BoolExpr

    def accept(self, visitor: ExprVisitor[T]) -> T:
        return visitor.visit_and(self)


@final
@dataclass(frozen=True)
class OrExpr(BoolExpr):
    left: BoolExpr
    right: BoolExpr

    def accept(self, visitor: ExprVisitor[T]) -> T:
        return visitor.visit_or(self)


@final
@dataclass(frozen=True)
class NotExpr(BoolExpr):
    expr: BoolExpr

    def accept(self, visitor: ExprVisitor[T]) -> T:
        return visitor.visit_not(self)


@final
@dataclass(frozen=True)
class InExpr(BoolExpr):
    expr: ScalarExpr
    values: tuple["CompareValue", ...]

    def accept(self, visitor: ExprVisitor[T]) -> T:
        return visitor.visit_in(self)


@dataclass(frozen=True)
class SortExpr:
    expr: Expr | str
    descending: bool = False


LiteralValue = Union[None, bool, int, float, Decimal, str, date, datetime]
CompareValue = Union[Expr, LiteralValue]
