"""Expression system for building OCEL filter predicates.

The expression tree has three node types that cover all SQL predicates:

- :class:`BinOp` — ``(left op right)``: comparisons (``=``, ``>``, ...),
  logical operators (``AND``, ``OR``), ``LIKE``, ``IS NULL``, etc.
- :class:`UnaOp` — ``(op operand)``: prefix unary operators (``NOT``).
- :class:`SetOp` — ``(operand op (v1, v2, ...))``: set operators
  (``IN``, ``NOT IN``).

Expressions are built via the :data:`event` and :data:`obj` sentinel
namespaces::

    from oceldb.expr import event, obj

    event.type == "Create Order"       # equality
    event.time > "2022-01-01"          # comparison
    event.type.is_in("A", "B")        # set membership
    event.type.is_like("%Order%")      # pattern matching
    obj.type.is_null()                 # null check
    (event.type == "A") & (event.time > "2022-01-01")  # logical AND
    ~(event.type == "A")               # logical NOT

Attribute access on the namespaces maps friendly names to OCEL column
names (``type`` -> ``ocel_type``, ``time`` -> ``ocel_time``).  Any other
name passes through unchanged for per-type attribute filtering
(e.g. ``event.amount`` -> ``amount``).

Raw Python values (``str``, ``int``, ``float``, ``bool``, ``None``) on the
right-hand side of :class:`BinOp` and in :class:`SetOp` are auto-wrapped
into :class:`Literal` nodes, so callers never need to construct
:class:`Literal` directly.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum

type LiteralValue = str | int | float | bool | None
"""Python types that can appear as literal values in filter expressions."""


# -- AST base classes ---------------------------------------------------------


class SqlNode(ABC):
    """Base class for all nodes in the expression tree."""

    @abstractmethod
    def to_sql(self) -> str:
        """Render this node as a SQL fragment."""
        ...

    @abstractmethod
    def columns(self) -> frozenset[Attribute]:
        """Return the set of :class:`Attribute` leaves referenced by this node."""
        ...


class Expr(SqlNode):
    """A node that represents a boolean expression.

    Supports combining via ``&`` (AND), ``|`` (OR), and ``~`` (NOT)::

        combined = (expr_a & expr_b) | ~expr_c
    """

    def __and__(self, other: Expr) -> BinOp:
        return BinOp("AND", self, other)

    def __or__(self, other: Expr) -> BinOp:
        return BinOp("OR", self, other)

    def __invert__(self) -> UnaOp:
        return UnaOp("NOT", self)


# -- Concrete AST nodes -------------------------------------------------------


class Literal(SqlNode):
    """A constant value rendered as a SQL literal.

    Constructed automatically by :class:`BinOp` and :class:`SetOp` —
    there is no need to instantiate this class directly.
    """

    def __init__(self, value: LiteralValue) -> None:
        self.value = value

    def to_sql(self) -> str:
        if self.value is None:
            return "NULL"
        if isinstance(self.value, bool):
            return "TRUE" if self.value else "FALSE"
        if isinstance(self.value, (int, float)):
            return str(self.value)
        if isinstance(self.value, str):
            escaped = self.value.replace("'", "''")
            return f"'{escaped}'"
        raise TypeError(f"Unsupported literal type: {type(self.value)}")

    def columns(self) -> frozenset[Attribute]:
        return frozenset()


class BinOp(Expr):
    """Binary operation: ``(left op right)``.

    Covers comparisons (``=``, ``!=``, ``>``, ``>=``, ``<``, ``<=``),
    logical operators (``AND``, ``OR``), pattern matching (``LIKE``),
    and null checks (``IS``, ``IS NOT``).

    *left* must be a :class:`SqlNode` (an :class:`Attribute` or another
    :class:`Expr`).  *right* may be a raw Python value, which is
    automatically wrapped in a :class:`Literal`.
    """

    def __init__(self, op: str, left: SqlNode, right: SqlNode | LiteralValue) -> None:
        self.op = op
        self.left = left
        self.right = right if isinstance(right, SqlNode) else Literal(right)

    def to_sql(self) -> str:
        return f"({self.left.to_sql()} {self.op} {self.right.to_sql()})"

    def columns(self) -> frozenset[Attribute]:
        return self.left.columns() | self.right.columns()


class UnaOp(Expr):
    """Prefix unary operation: ``(op operand)``.  Used for ``NOT``."""

    def __init__(self, op: str, operand: Expr) -> None:
        self.op = op
        self.operand = operand

    def to_sql(self) -> str:
        return f"({self.op} {self.operand.to_sql()})"

    def columns(self) -> frozenset[Attribute]:
        return self.operand.columns()


class SetOp(Expr):
    """Set operation: ``(operand op (v1, v2, ...))``.

    Covers ``IN`` and ``NOT IN``.  Values are stored as :class:`Literal`
    nodes internally.
    """

    def __init__(
        self, op: str, operand: SqlNode, values: tuple[LiteralValue, ...]
    ) -> None:
        self.op = op
        self.operand = operand
        self.values = tuple(Literal(v) for v in values)

    def to_sql(self) -> str:
        items = ", ".join(v.to_sql() for v in self.values)
        return f"({self.operand.to_sql()} {self.op} ({items}))"

    def columns(self) -> frozenset[Attribute]:
        return self.operand.columns()


# -- Column references --------------------------------------------------------


class Domain(Enum):
    """Whether an attribute belongs to an event or an object."""

    EVENT = "event"
    OBJECT = "object"


class Attribute(SqlNode):
    """A reference to a column in an OCEL table.

    Not an :class:`Expr` — cannot be combined with ``&``/``|``/``~``.
    Use comparison operators or predicate methods to obtain an :class:`Expr`::

        event.type == "Order"   # BinOp (an Expr)
        event.time.is_null()    # BinOp (an Expr)
    """

    def __init__(self, domain: Domain, ocel_column: str) -> None:
        self.domain = domain
        self.ocel_column = ocel_column

    def to_sql(self) -> str:
        return self.ocel_column

    def columns(self) -> frozenset[Attribute]:
        return frozenset({self})

    def __repr__(self) -> str:
        return f"{self.domain.value}.{self.ocel_column}"

    def __hash__(self) -> int:
        return id(self)

    # -- Comparison operators -------------------------------------------------

    def __eq__(self, other: LiteralValue) -> BinOp:  # type: ignore[override]
        return BinOp("=", self, other)

    def __ne__(self, other: LiteralValue) -> BinOp:  # type: ignore[override]
        return BinOp("!=", self, other)

    def __gt__(self, other: LiteralValue) -> BinOp:
        return BinOp(">", self, other)

    def __ge__(self, other: LiteralValue) -> BinOp:
        return BinOp(">=", self, other)

    def __lt__(self, other: LiteralValue) -> BinOp:
        return BinOp("<", self, other)

    def __le__(self, other: LiteralValue) -> BinOp:
        return BinOp("<=", self, other)

    # -- Predicates -----------------------------------------------------------

    def is_in(self, *values: LiteralValue) -> SetOp:
        """``column IN (v1, v2, ...)``"""
        return SetOp("IN", self, values)

    def not_in(self, *values: LiteralValue) -> SetOp:
        """``column NOT IN (v1, v2, ...)``"""
        return SetOp("NOT IN", self, values)

    def is_null(self) -> BinOp:
        """``column IS NULL``"""
        return BinOp("IS", self, None)

    def not_null(self) -> BinOp:
        """``column IS NOT NULL``"""
        return BinOp("IS NOT", self, None)

    def is_like(self, value: LiteralValue) -> BinOp:
        """``column LIKE pattern`` — use ``%`` and ``_`` as wildcards."""
        return BinOp("LIKE", self, value)

    def not_like(self, value: LiteralValue) -> BinOp:
        """``column NOT LIKE pattern``"""
        return BinOp("NOT LIKE", self, value)

    def is_between(self, low: LiteralValue, high: LiteralValue) -> BinOp:
        """``column >= low AND column <= high``"""
        return BinOp("AND", BinOp(">=", self, low), BinOp("<=", self, high))

    def not_between(self, low: LiteralValue, high: LiteralValue) -> UnaOp:
        """``NOT (column >= low AND column <= high)``"""
        return UnaOp("NOT", self.is_between(low, high))


# -- Sentinel namespaces ------------------------------------------------------

_ALIASES = {"type": "ocel_type", "id": "ocel_id", "time": "ocel_time"}
"""Friendly names mapped to their ``ocel_``-prefixed column names."""


class _Namespace:
    """Sentinel object that produces :class:`Attribute` refs via dotted access.

    Known names (``type``, ``id``, ``time``) are aliased to their OCEL
    column names.  Any other name passes through as-is, enabling filtering
    on arbitrary per-type attributes (e.g. ``event.amount``).
    """

    def __init__(self, domain: Domain) -> None:
        self._domain = domain

    def __getattr__(self, name: str) -> Attribute:
        if name.startswith("_"):
            raise AttributeError(name)
        return Attribute(self._domain, _ALIASES.get(name, name))


event = _Namespace(Domain.EVENT)
"""Sentinel for referencing event columns: ``event.type``, ``event.time``, etc."""

obj = _Namespace(Domain.OBJECT)
"""Sentinel for referencing object columns: ``obj.type``, ``obj.id``, etc."""

__all__ = [
    "Attribute",
    "BinOp",
    "Domain",
    "Expr",
    "Literal",
    "SetOp",
    "UnaOp",
    "event",
    "obj",
]
