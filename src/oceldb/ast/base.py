from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Generic, Iterable, TypeVar, Union, final

if TYPE_CHECKING:
    from oceldb.ast.aggregation import AvgAgg, CountAgg, CountDistinctAgg, MaxAgg, MinAgg, SumAgg
    from oceldb.ast.field import ColumnExpr
    from oceldb.ast.relation import RelationAllExpr, RelationCountExpr, RelationExistsExpr

T = TypeVar("T")


class ExprVisitor(ABC, Generic[T]):
    @abstractmethod
    def visit_column(self, expr: "ColumnExpr") -> T: ...

    @abstractmethod
    def visit_alias(self, expr: "AliasExpr") -> T: ...

    @abstractmethod
    def visit_literal(self, expr: "LiteralExpr") -> T: ...

    @abstractmethod
    def visit_cast(self, expr: "CastExpr") -> T: ...

    @abstractmethod
    def visit_binary_op(self, expr: "BinaryOpExpr") -> T: ...

    @abstractmethod
    def visit_scalar_function(self, expr: "FunctionExpr") -> T: ...

    @abstractmethod
    def visit_predicate_function(self, expr: "PredicateFunctionExpr") -> T: ...

    @abstractmethod
    def visit_case(self, expr: "CaseExpr") -> T: ...

    @abstractmethod
    def visit_window_function(self, expr: "WindowFunctionExpr") -> T: ...

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
    def visit_count(self, expr: "CountAgg") -> T: ...

    @abstractmethod
    def visit_count_distinct(self, expr: "CountDistinctAgg") -> T: ...

    @abstractmethod
    def visit_min(self, expr: "MinAgg") -> T: ...

    @abstractmethod
    def visit_max(self, expr: "MaxAgg") -> T: ...

    @abstractmethod
    def visit_sum(self, expr: "SumAgg") -> T: ...

    @abstractmethod
    def visit_avg(self, expr: "AvgAgg") -> T: ...

    @abstractmethod
    def visit_relation_exists(self, expr: "RelationExistsExpr") -> T: ...

    @abstractmethod
    def visit_relation_count(self, expr: "RelationCountExpr") -> T: ...

    @abstractmethod
    def visit_relation_all(self, expr: "RelationAllExpr") -> T: ...


class Expr(ABC):
    def alias(self, name: str) -> "AliasExpr":
        if not name:
            raise ValueError("Alias must not be empty")
        return AliasExpr(expr=self, name=name)

    def __bool__(self) -> bool:
        raise TypeError(
            "oceldb expressions cannot be used as Python booleans; "
            "use &, |, and ~ to combine predicates"
        )

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

    def fill_null(self, value: "ScalarValue") -> "FunctionExpr":
        return FunctionExpr(name="coalesce", args=(self, value))

    def abs(self) -> "FunctionExpr":
        return FunctionExpr(name="abs", args=(self,))

    def round(self, decimals: "ScalarValue" = 0) -> "FunctionExpr":
        return FunctionExpr(name="round", args=(self, decimals))

    def lag(
        self,
        offset: int = 1,
        *,
        default: ScalarValue | None = None,
    ) -> "WindowFunctionExpr":
        if offset < 1:
            raise ValueError("lag(...) requires a positive offset")
        return WindowFunctionExpr(
            name="lag",
            args=(self,),
            offset=offset,
            default=default,
        )

    def lead(
        self,
        offset: int = 1,
        *,
        default: ScalarValue | None = None,
    ) -> "WindowFunctionExpr":
        if offset < 1:
            raise ValueError("lead(...) requires a positive offset")
        return WindowFunctionExpr(
            name="lead",
            args=(self,),
            offset=offset,
            default=default,
        )

    @property
    def str(self) -> "StringNamespace":
        return StringNamespace(self)

    @property
    def dt(self) -> "DatetimeNamespace":
        return DatetimeNamespace(self)

    def __add__(self, other: "ScalarValue") -> "BinaryOpExpr":
        return BinaryOpExpr(self, "+", other)

    def __radd__(self, other: "ScalarValue") -> "BinaryOpExpr":
        return BinaryOpExpr(other, "+", self)

    def __sub__(self, other: "ScalarValue") -> "BinaryOpExpr":
        return BinaryOpExpr(self, "-", other)

    def __rsub__(self, other: "ScalarValue") -> "BinaryOpExpr":
        return BinaryOpExpr(other, "-", self)

    def __mul__(self, other: "ScalarValue") -> "BinaryOpExpr":
        return BinaryOpExpr(self, "*", other)

    def __rmul__(self, other: "ScalarValue") -> "BinaryOpExpr":
        return BinaryOpExpr(other, "*", self)

    def __truediv__(self, other: "ScalarValue") -> "BinaryOpExpr":
        return BinaryOpExpr(self, "/", other)

    def __rtruediv__(self, other: "ScalarValue") -> "BinaryOpExpr":
        return BinaryOpExpr(other, "/", self)

    def __mod__(self, other: "ScalarValue") -> "BinaryOpExpr":
        return BinaryOpExpr(self, "%", other)

    def __rmod__(self, other: "ScalarValue") -> "BinaryOpExpr":
        return BinaryOpExpr(other, "%", self)


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
    name: str

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
@dataclass(frozen=True, eq=False)
class BinaryOpExpr(ScalarExpr):
    left: "ScalarValue"
    op: str
    right: "ScalarValue"

    def accept(self, visitor: ExprVisitor[T]) -> T:
        return visitor.visit_binary_op(self)


@final
@dataclass(frozen=True, eq=False)
class FunctionExpr(ScalarExpr):
    name: str
    args: tuple["ScalarValue", ...]

    def accept(self, visitor: ExprVisitor[T]) -> T:
        return visitor.visit_scalar_function(self)


@final
@dataclass(frozen=True)
class PredicateFunctionExpr(BoolExpr):
    name: str
    args: tuple["ScalarValue", ...]

    def accept(self, visitor: ExprVisitor[T]) -> T:
        return visitor.visit_predicate_function(self)


@final
@dataclass(frozen=True, eq=False)
class CaseExpr(ScalarExpr):
    branches: tuple[tuple["BoolExpr", "ScalarValue"], ...]
    default: "ScalarValue"

    def accept(self, visitor: ExprVisitor[T]) -> T:
        return visitor.visit_case(self)


@final
@dataclass(frozen=True)
class WindowSpec:
    partition_by: tuple[ScalarExpr, ...] = ()
    order_by: tuple["SortExpr", ...] = ()


@final
@dataclass(frozen=True, eq=False)
class WindowFunctionExpr(ScalarExpr):
    name: str
    args: tuple["ScalarValue", ...] = ()
    offset: int | None = None
    default: ScalarValue | None = None
    window: WindowSpec | None = None

    def over(
        self,
        *,
        partition_by: "WindowPartitionArg" = (),
        order_by: "WindowOrderArg",
    ) -> "WindowFunctionExpr":
        partition_exprs = _coerce_partition_exprs(partition_by)
        order_exprs = _coerce_order_exprs(order_by)
        if not order_exprs:
            raise ValueError("over(...) requires at least one order_by expression")
        return WindowFunctionExpr(
            name=self.name,
            args=self.args,
            offset=self.offset,
            default=self.default,
            window=WindowSpec(
                partition_by=partition_exprs,
                order_by=order_exprs,
            ),
        )

    def accept(self, visitor: ExprVisitor[T]) -> T:
        return visitor.visit_window_function(self)


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


@final
@dataclass(frozen=True)
class StringNamespace:
    expr: ScalarExpr

    def lower(self) -> FunctionExpr:
        return FunctionExpr(name="lower", args=(self.expr,))

    def upper(self) -> FunctionExpr:
        return FunctionExpr(name="upper", args=(self.expr,))

    def contains(self, value: "ScalarValue") -> PredicateFunctionExpr:
        return PredicateFunctionExpr(name="contains", args=(self.expr, value))

    def starts_with(self, value: "ScalarValue") -> PredicateFunctionExpr:
        return PredicateFunctionExpr(name="starts_with", args=(self.expr, value))

    def ends_with(self, value: "ScalarValue") -> PredicateFunctionExpr:
        return PredicateFunctionExpr(name="ends_with", args=(self.expr, value))


@final
@dataclass(frozen=True)
class DatetimeNamespace:
    expr: ScalarExpr

    def year(self) -> FunctionExpr:
        return FunctionExpr(name="year", args=(self.expr,))

    def month(self) -> FunctionExpr:
        return FunctionExpr(name="month", args=(self.expr,))

    def day(self) -> FunctionExpr:
        return FunctionExpr(name="day", args=(self.expr,))

    def date(self) -> FunctionExpr:
        return FunctionExpr(name="date", args=(self.expr,))


@dataclass(frozen=True)
class SortExpr:
    expr: Expr | str
    descending: bool = False


LiteralValue = Union[None, bool, int, float, Decimal, str, date, datetime]
ScalarValue = Union["ScalarExpr", LiteralValue]
CompareValue = Union[Expr, LiteralValue]
WindowPartitionItem = Union["ScalarExpr", str]
WindowPartitionArg = Union[
    WindowPartitionItem,
    Iterable[WindowPartitionItem],
]
WindowOrderItem = Union["ScalarExpr", str, SortExpr]
WindowOrderArg = Union[
    WindowOrderItem,
    Iterable[WindowOrderItem],
]


def _coerce_partition_exprs(value: WindowPartitionArg) -> tuple[ScalarExpr, ...]:
    if isinstance(value, (str, ScalarExpr)):
        items: tuple[WindowPartitionItem, ...] = (value,)
    else:
        items = tuple(value)
    return tuple(_coerce_partition_item(item) for item in items)


def _coerce_order_exprs(value: WindowOrderArg) -> tuple[SortExpr, ...]:
    if isinstance(value, (str, ScalarExpr, SortExpr)):
        items: tuple[WindowOrderItem, ...] = (value,)
    else:
        items = tuple(value)
    result: list[SortExpr] = []
    for item in items:
        if isinstance(item, SortExpr):
            result.append(item)
        elif isinstance(item, str):
            result.append(SortExpr(expr=_column_expr(item)))
        else:
            result.append(SortExpr(expr=item))
    return tuple(result)


def _coerce_partition_item(value: WindowPartitionItem) -> ScalarExpr:
    if isinstance(value, str):
        return _column_expr(value)
    return value


def _column_expr(name: str) -> "ColumnExpr":
    from oceldb.ast.field import ColumnExpr

    return ColumnExpr(name=name)
