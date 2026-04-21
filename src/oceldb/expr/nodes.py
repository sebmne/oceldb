"""Expression AST nodes.

All nodes are frozen dataclasses. Each node implements ``children()`` so that
an ``ExprVisitor`` with a ``generic_visit`` default can recurse automatically.

The node types are also the user-facing expression objects: ``.alias()``,
``.cast()``, ``.is_in(...)``, the ``.str`` / ``.dt`` accessors, operator
overloads, and so on all live directly on ``Expr`` subclasses rather than in a
separate wrapper layer. This keeps the surface flat given oceldb is
DuckDB-only.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import date, datetime
from typing import Any, Generic, Literal, TypeVar

T = TypeVar("T")

Literal_ = int | float | str | bool | date | datetime | None

BinaryOp = Literal["+", "-", "*", "/", "%"]
CompareOp = Literal["=", "!=", "<", "<=", ">", ">="]
BoolOp = Literal["AND", "OR"]


# ---------------------------------------------------------------------------
# Base classes
# ---------------------------------------------------------------------------


class Expr:
    """Base class for all expression nodes.

    This is a plain ``object`` subclass rather than an ABC so the closed-union
    pattern (every subclass is a frozen dataclass) stays lightweight.
    """

    __slots__ = ()

    def children(self) -> Iterable["Expr"]:
        return ()

    def __bool__(self) -> bool:
        raise TypeError(
            "oceldb expressions cannot be used as Python booleans. "
            "Use &, |, ~ for boolean logic on expressions."
        )

    def alias(self, name: str) -> "AliasExpr":
        return AliasExpr(expr=self, name=name)

    def cast(self, sql_type: str) -> "CastExpr":
        return CastExpr(expr=self, sql_type=sql_type)

    def is_null(self) -> "PredicateFunctionExpr":
        return PredicateFunctionExpr(name="IS_NULL", args=(self,))

    def not_null(self) -> "PredicateFunctionExpr":
        return PredicateFunctionExpr(name="IS_NOT_NULL", args=(self,))

    def is_in(self, values: Iterable[Any]) -> "InExpr":
        return InExpr(expr=self, values=tuple(values))

    def fill_null(self, fallback: "Expr | Literal_") -> "FunctionExpr":
        return FunctionExpr(name="COALESCE", args=(self, lift_expr(fallback)))

    def abs(self) -> "FunctionExpr":
        return FunctionExpr(name="ABS", args=(self,))

    def round(self, digits: int = 0) -> "FunctionExpr":
        return FunctionExpr(name="ROUND", args=(self, LiteralExpr(value=digits)))

    @property
    def str(self) -> "_StringNamespace":
        return _StringNamespace(self)

    @property
    def dt(self) -> "_DatetimeNamespace":
        return _DatetimeNamespace(self)

    def lead(self, offset: int = 1) -> "WindowBuilder":
        return WindowBuilder(name="LEAD", args=(self, LiteralExpr(value=offset)))

    def lag(self, offset: int = 1) -> "WindowBuilder":
        return WindowBuilder(name="LAG", args=(self, LiteralExpr(value=offset)))

    def _lift_other(self, other: Any) -> "Expr":
        return lift_expr(other)

    def __add__(self, other: Any) -> "BinaryOpExpr":
        return BinaryOpExpr(left=self, op="+", right=lift_expr(other))

    def __radd__(self, other: Any) -> "BinaryOpExpr":
        return BinaryOpExpr(left=lift_expr(other), op="+", right=self)

    def __sub__(self, other: Any) -> "BinaryOpExpr":
        return BinaryOpExpr(left=self, op="-", right=lift_expr(other))

    def __rsub__(self, other: Any) -> "BinaryOpExpr":
        return BinaryOpExpr(left=lift_expr(other), op="-", right=self)

    def __mul__(self, other: Any) -> "BinaryOpExpr":
        return BinaryOpExpr(left=self, op="*", right=lift_expr(other))

    def __rmul__(self, other: Any) -> "BinaryOpExpr":
        return BinaryOpExpr(left=lift_expr(other), op="*", right=self)

    def __truediv__(self, other: Any) -> "BinaryOpExpr":
        return BinaryOpExpr(left=self, op="/", right=lift_expr(other))

    def __rtruediv__(self, other: Any) -> "BinaryOpExpr":
        return BinaryOpExpr(left=lift_expr(other), op="/", right=self)

    def __mod__(self, other: Any) -> "BinaryOpExpr":
        return BinaryOpExpr(left=self, op="%", right=lift_expr(other))

    def __neg__(self) -> "BinaryOpExpr":
        return BinaryOpExpr(left=LiteralExpr(value=0), op="-", right=self)

    def __eq__(self, other: object) -> "CompareExpr":  # type: ignore[override]
        return CompareExpr(left=self, op="=", right=lift_expr(other))

    def __ne__(self, other: object) -> "CompareExpr":  # type: ignore[override]
        return CompareExpr(left=self, op="!=", right=lift_expr(other))

    def __lt__(self, other: Any) -> "CompareExpr":
        return CompareExpr(left=self, op="<", right=lift_expr(other))

    def __le__(self, other: Any) -> "CompareExpr":
        return CompareExpr(left=self, op="<=", right=lift_expr(other))

    def __gt__(self, other: Any) -> "CompareExpr":
        return CompareExpr(left=self, op=">", right=lift_expr(other))

    def __ge__(self, other: Any) -> "CompareExpr":
        return CompareExpr(left=self, op=">=", right=lift_expr(other))

    def __and__(self, other: Any) -> "BoolOpExpr":
        return BoolOpExpr(op="AND", operands=(self, lift_expr(other)))

    def __or__(self, other: Any) -> "BoolOpExpr":
        return BoolOpExpr(op="OR", operands=(self, lift_expr(other)))

    def __invert__(self) -> "NotExpr":
        return NotExpr(operand=self)

    def __hash__(self) -> int:
        return id(self)


# ---------------------------------------------------------------------------
# Leaf nodes
# ---------------------------------------------------------------------------


@dataclass(frozen=True, eq=False)
class ColumnExpr(Expr):
    name: str


@dataclass(frozen=True, eq=False)
class LiteralExpr(Expr):
    value: Literal_


# ---------------------------------------------------------------------------
# Composite nodes
# ---------------------------------------------------------------------------


@dataclass(frozen=True, eq=False)
class AliasExpr(Expr):
    expr: Expr
    name: str

    def children(self) -> Iterable[Expr]:
        return (self.expr,)


@dataclass(frozen=True, eq=False)
class CastExpr(Expr):
    expr: Expr
    sql_type: str

    def children(self) -> Iterable[Expr]:
        return (self.expr,)


@dataclass(frozen=True, eq=False)
class BinaryOpExpr(Expr):
    left: Expr
    op: BinaryOp
    right: Expr

    def children(self) -> Iterable[Expr]:
        return (self.left, self.right)


@dataclass(frozen=True, eq=False)
class CompareExpr(Expr):
    left: Expr
    op: CompareOp
    right: Expr

    def children(self) -> Iterable[Expr]:
        return (self.left, self.right)


@dataclass(frozen=True, eq=False)
class BoolOpExpr(Expr):
    op: BoolOp
    operands: tuple[Expr, ...]

    def children(self) -> Iterable[Expr]:
        return self.operands


@dataclass(frozen=True, eq=False)
class NotExpr(Expr):
    operand: Expr

    def children(self) -> Iterable[Expr]:
        return (self.operand,)


@dataclass(frozen=True, eq=False)
class InExpr(Expr):
    expr: Expr
    values: tuple[Any, ...]

    def children(self) -> Iterable[Expr]:
        return (self.expr,)


@dataclass(frozen=True, eq=False)
class FunctionExpr(Expr):
    """A generic scalar function call (COALESCE, ABS, ROUND, EXTRACT, UPPER, ...)."""

    name: str
    args: tuple[Expr, ...] = ()
    extra: tuple[Any, ...] = ()

    def children(self) -> Iterable[Expr]:
        return self.args


@dataclass(frozen=True, eq=False)
class PredicateFunctionExpr(Expr):
    """A function call used as a boolean (IS NULL, IS NOT NULL, str.contains)."""

    name: str
    args: tuple[Expr, ...] = ()
    extra: tuple[Any, ...] = ()

    def children(self) -> Iterable[Expr]:
        return self.args


@dataclass(frozen=True, eq=False)
class CaseExpr(Expr):
    """A SQL CASE expression built by ``when(...).then(...).otherwise(...)``."""

    branches: tuple[tuple[Expr, Expr], ...]
    default: Expr | None = None

    def children(self) -> Iterable[Expr]:
        for cond, value in self.branches:
            yield cond
            yield value
        if self.default is not None:
            yield self.default

    def when(self, predicate: Expr) -> "CaseBuilder":
        return CaseBuilder(branches=self.branches, default=self.default, pending=predicate)

    def otherwise(self, value: "Expr | Literal_") -> "CaseExpr":
        return replace(self, default=lift_expr(value))


@dataclass(frozen=True, eq=False)
class CaseBuilder:
    """Intermediate builder returned by ``when(...)``. Not an Expr itself."""

    branches: tuple[tuple[Expr, Expr], ...]
    default: Expr | None
    pending: Expr

    def then(self, value: "Expr | Literal_") -> CaseExpr:
        new_branches = self.branches + ((self.pending, lift_expr(value)),)
        return CaseExpr(branches=new_branches, default=self.default)


# ---------------------------------------------------------------------------
# Aggregates
# ---------------------------------------------------------------------------


class AggregateExpr(Expr):
    """Marker base for aggregate expressions (count, sum, avg, ...)."""

    __slots__ = ()


@dataclass(frozen=True, eq=False)
class CountAgg(AggregateExpr):
    expr: Expr | None = None  # None => COUNT(*)
    distinct: bool = False

    def children(self) -> Iterable[Expr]:
        return (self.expr,) if self.expr is not None else ()


@dataclass(frozen=True, eq=False)
class SumAgg(AggregateExpr):
    expr: Expr

    def children(self) -> Iterable[Expr]:
        return (self.expr,)


@dataclass(frozen=True, eq=False)
class AvgAgg(AggregateExpr):
    expr: Expr

    def children(self) -> Iterable[Expr]:
        return (self.expr,)


@dataclass(frozen=True, eq=False)
class MinAgg(AggregateExpr):
    expr: Expr

    def children(self) -> Iterable[Expr]:
        return (self.expr,)


@dataclass(frozen=True, eq=False)
class MaxAgg(AggregateExpr):
    expr: Expr

    def children(self) -> Iterable[Expr]:
        return (self.expr,)


# ---------------------------------------------------------------------------
# Window functions
# ---------------------------------------------------------------------------


@dataclass(frozen=True, eq=False)
class WindowFunctionExpr(Expr):
    name: str
    args: tuple[Expr, ...] = ()
    partition_by: tuple[Expr, ...] = ()
    order_by: tuple["SortExpr", ...] = ()

    def children(self) -> Iterable[Expr]:
        yield from self.args
        yield from self.partition_by
        for order in self.order_by:
            yield order.expr


@dataclass(frozen=True, eq=False)
class WindowBuilder:
    """Intermediate object produced by ``.lead()`` / ``.lag()`` / ``row_number()``.

    Not an Expr — only ``.over(...)`` yields a usable expression. Trying to use
    one of these in a where/select raises clearly because it has no visitor
    method.
    """

    name: str
    args: tuple[Expr, ...]

    def over(
        self,
        *,
        partition_by: str | Expr | Iterable[str | Expr] = (),
        order_by: "str | SortExpr | Iterable[str | SortExpr]" = (),
    ) -> WindowFunctionExpr:
        return WindowFunctionExpr(
            name=self.name,
            args=self.args,
            partition_by=_coerce_partition(partition_by),
            order_by=_coerce_order(order_by),
        )


# ---------------------------------------------------------------------------
# Sort specifications
# ---------------------------------------------------------------------------


@dataclass(frozen=True, eq=False)
class SortExpr(Expr):
    expr: Expr
    descending: bool = False

    def children(self) -> Iterable[Expr]:
        return (self.expr,)


# ---------------------------------------------------------------------------
# Namespace accessors (.str, .dt)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, eq=False)
class _StringNamespace:
    _parent: Expr

    def upper(self) -> FunctionExpr:
        return FunctionExpr(name="UPPER", args=(self._parent,))

    def lower(self) -> FunctionExpr:
        return FunctionExpr(name="LOWER", args=(self._parent,))

    def contains(self, pattern: str) -> PredicateFunctionExpr:
        return PredicateFunctionExpr(
            name="STR_CONTAINS",
            args=(self._parent, LiteralExpr(value=pattern)),
        )


@dataclass(frozen=True, eq=False)
class _DatetimeNamespace:
    _parent: Expr

    def year(self) -> FunctionExpr:
        return FunctionExpr(name="EXTRACT", args=(self._parent,), extra=("YEAR",))

    def month(self) -> FunctionExpr:
        return FunctionExpr(name="EXTRACT", args=(self._parent,), extra=("MONTH",))

    def day(self) -> FunctionExpr:
        return FunctionExpr(name="EXTRACT", args=(self._parent,), extra=("DAY",))


# ---------------------------------------------------------------------------
# Relation predicates (closed — defined here so ExprVisitor can see them)
# ---------------------------------------------------------------------------


RelationKind = Literal[
    "has_event",
    "has_object",
    "cooccurs_with",
    "linked",
]

LinkedDirection = Literal["any", "incoming", "outgoing"]


@dataclass(frozen=True, eq=False)
class RelationTarget:
    """Normalized description of a relation-predicate target."""

    kind: RelationKind
    type_name: str
    direction: LinkedDirection = "any"
    hop_limit: int | None = 1


@dataclass(frozen=True, eq=False)
class RelationExistsExpr(Expr):
    target: RelationTarget
    predicate: Expr | None = None

    def children(self) -> Iterable[Expr]:
        return (self.predicate,) if self.predicate is not None else ()


@dataclass(frozen=True, eq=False)
class RelationCountExpr(Expr):
    target: RelationTarget
    predicate: Expr | None = None

    def children(self) -> Iterable[Expr]:
        return (self.predicate,) if self.predicate is not None else ()


@dataclass(frozen=True, eq=False)
class RelationAllExpr(Expr):
    target: RelationTarget
    predicate: Expr

    def children(self) -> Iterable[Expr]:
        return (self.predicate,)


# ---------------------------------------------------------------------------
# Visitor base
# ---------------------------------------------------------------------------


class ExprVisitor(Generic[T]):
    """Visitor with ``generic_visit`` default recursion.

    Subclasses override ``visit_<ClassName>`` methods they care about. Anything
    not overridden recurses into children via ``children()``.
    """

    def visit(self, expr: Expr) -> T:
        method = getattr(self, f"visit_{type(expr).__name__}", None)
        if method is None:
            return self.generic_visit(expr)
        return method(expr)  # type: ignore[no-any-return]

    def generic_visit(self, expr: Expr) -> T:
        for child in expr.children():
            self.visit(child)
        return None  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def lift_expr(value: Any) -> Expr:
    if isinstance(value, Expr):
        return value
    if isinstance(value, (int, float, str, bool, date, datetime)) or value is None:
        return LiteralExpr(value=value)
    raise TypeError(f"Cannot lift value to expression: {value!r}")


def _coerce_partition(
    value: str | Expr | Iterable[str | Expr],
) -> tuple[Expr, ...]:
    if isinstance(value, (str, Expr)):
        return (_coerce_partition_one(value),)
    return tuple(_coerce_partition_one(v) for v in value)


def _coerce_partition_one(value: str | Expr) -> Expr:
    if isinstance(value, str):
        return ColumnExpr(name=value)
    return value


def _coerce_order(
    value: str | SortExpr | Iterable[str | SortExpr],
) -> tuple[SortExpr, ...]:
    if isinstance(value, (str, SortExpr)):
        return (_coerce_order_one(value),)
    return tuple(_coerce_order_one(v) for v in value)


def _coerce_order_one(value: str | SortExpr) -> SortExpr:
    if isinstance(value, str):
        return SortExpr(expr=ColumnExpr(name=value))
    return value


def coerce_expr(value: Expr | str | Literal_) -> Expr:
    """Public coercion used by builders and FSM methods."""
    if isinstance(value, Expr):
        return value
    if isinstance(value, str):
        return ColumnExpr(name=value)
    return lift_expr(value)


__all__ = [
    "AggregateExpr",
    "AliasExpr",
    "AvgAgg",
    "BinaryOp",
    "BinaryOpExpr",
    "BoolOp",
    "BoolOpExpr",
    "CaseBuilder",
    "CaseExpr",
    "CastExpr",
    "ColumnExpr",
    "CompareExpr",
    "CompareOp",
    "CountAgg",
    "Expr",
    "ExprVisitor",
    "FunctionExpr",
    "InExpr",
    "LinkedDirection",
    "LiteralExpr",
    "Literal_",
    "MaxAgg",
    "MinAgg",
    "NotExpr",
    "PredicateFunctionExpr",
    "RelationAllExpr",
    "RelationCountExpr",
    "RelationExistsExpr",
    "RelationKind",
    "RelationTarget",
    "SortExpr",
    "SumAgg",
    "WindowFunctionExpr",
    "_StringNamespace",
    "_DatetimeNamespace",
    "WindowBuilder",
    "coerce_expr",
]
