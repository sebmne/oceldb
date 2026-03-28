"""Expression hierarchy — the core of the oceldb filter system.

Every Expr subclass represents a SQL boolean fragment. Attribute predicates
return simple conditions; structural filters return ``ocel_id IN (SELECT ...)``.
Both compose naturally with ``&`` / ``|`` / ``~``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING

from oceldb.expr._types import Op
from oceldb.types import Domain, ScalarValue
from oceldb.utils import sql_literal

if TYPE_CHECKING:
    from oceldb.expr._col import Col
    from oceldb.expr._context import CompilationContext


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------


class Expr(ABC):
    """Abstract base for all boolean SQL expressions in the filter system.

    Every concrete subclass represents a SQL fragment that evaluates to
    ``TRUE`` or ``FALSE``. Subclasses must implement:

    - :attr:`domain` — whether this expression filters events or objects.
    - :meth:`compile` — render the expression to a SQL string.

    Expressions compose via Python's bitwise operators, which are
    rewritten to SQL boolean logic:

    - ``a & b`` → ``(a AND b)``
    - ``a | b`` → ``(a OR b)``
    - ``~a``    → ``NOT (a)``

    Only same-domain expressions can be combined. Cross-domain composition
    raises :class:`ValueError` at construction time, not at query time.

    .. warning::

        Python's ``and`` / ``or`` / ``not`` keywords do **not** work with
        expressions. Using them triggers a :class:`TypeError` via
        :meth:`__bool__`.
    """

    __slots__ = ()

    @property
    @abstractmethod
    def domain(self) -> Domain:
        """The entity domain this expression filters."""
        ...

    @abstractmethod
    def compile(self, ctx: CompilationContext | None = None) -> str:
        """Render this expression as a SQL boolean fragment.

        Args:
            ctx: Schema context providing table references and column
                validation. Simple attribute predicates work without a
                context (``ctx=None``); structural filters that reference
                relationship tables require one.

        Returns:
            A SQL string suitable for use in a ``WHERE`` clause.

        Raises:
            ValueError: If *ctx* is ``None`` but the expression requires
                schema information (e.g. structural filters).
        """
        ...

    def to_sql(self) -> str:
        """Render without a compilation context (convenience for simple expressions).

        Equivalent to ``self.compile(ctx=None)``. Useful for debugging or
        logging attribute predicates that don't need schema information.

        Returns:
            A SQL boolean fragment string.
        """
        return self.compile(ctx=None)

    # -- Boolean composition ---------------------------------------------------

    def __and__(self, other: Expr) -> And:
        """Combine with another expression using SQL ``AND``.

        Raises:
            ValueError: If *other* belongs to a different domain.
        """
        if not isinstance(other, Expr):
            return NotImplemented
        if self.domain != other.domain:
            raise ValueError(
                f"Cannot combine {self.domain.value} and "
                f"{other.domain.value} expressions with &."
            )
        return And(self, other)

    def __or__(self, other: Expr) -> Or:
        """Combine with another expression using SQL ``OR``.

        Raises:
            ValueError: If *other* belongs to a different domain.
        """
        if not isinstance(other, Expr):
            return NotImplemented
        if self.domain != other.domain:
            raise ValueError(
                f"Cannot combine {self.domain.value} and "
                f"{other.domain.value} expressions with |."
            )
        return Or(self, other)

    def __invert__(self) -> Not:
        """Negate this expression using SQL ``NOT``."""
        return Not(self)

    def __bool__(self) -> bool:
        """Prevent accidental use in Python boolean context.

        Raises:
            TypeError: Always. Use ``&``, ``|``, ``~`` instead of
                ``and``, ``or``, ``not``.
        """
        raise TypeError(
            "Expressions cannot be used in boolean context. "
            "Use & instead of 'and', | instead of 'or', ~ instead of 'not'."
        )


# ---------------------------------------------------------------------------
# Leaf predicates
# ---------------------------------------------------------------------------


class Comparison(Expr):
    """A binary comparison: ``column <op> value``.

    Produced by :class:`~oceldb.expr._col.Col` operators (``==``, ``!=``,
    ``>``, ``>=``, ``<``, ``<=``). Compiles to e.g.
    ``"ocel_type" = 'Create Order'``.

    Args:
        col: The column being compared.
        op: The SQL comparison operator.
        value: The scalar value to compare against.
    """

    __slots__ = ("_col", "_op", "_value")

    def __init__(self, col: Col, op: Op, value: ScalarValue) -> None:
        self._col = col
        self._op = op
        self._value = value

    @property
    def domain(self) -> Domain:
        return self._col.domain

    def compile(self, ctx: CompilationContext | None = None) -> str:
        if ctx is not None:
            ctx.validate_column(self._col.domain, self._col.column)
        return f'"{self._col.column}" {self._op.value} {sql_literal(self._value)}'

    def __repr__(self) -> str:
        op_repr = (
            "==" if self._op is Op.EQ else "!=" if self._op is Op.NE else self._op.value
        )
        return f"{self._col.domain.value}.{self._col.column} {op_repr} {self._value!r}"


class InSet(Expr):
    """A set membership test: ``column IN (v1, v2, ...)``.

    Produced by :meth:`Col.is_in() <oceldb.expr._col.Col.is_in>`.

    Args:
        col: The column being tested.
        values: One or more scalar values to test against. Must be non-empty.

    Raises:
        ValueError: If *values* is empty.
    """

    __slots__ = ("_col", "_values")

    def __init__(self, col: Col, values: Sequence[ScalarValue]) -> None:
        if not values:
            raise ValueError("is_in() requires at least one value")
        self._col = col
        self._values = tuple(values)

    @property
    def domain(self) -> Domain:
        return self._col.domain

    def compile(self, ctx: CompilationContext | None = None) -> str:
        if ctx is not None:
            ctx.validate_column(self._col.domain, self._col.column)
        literals = ", ".join(sql_literal(v) for v in self._values)
        return f'"{self._col.column}" IN ({literals})'

    def __repr__(self) -> str:
        return (
            f"{self._col.domain.value}.{self._col.column}.is_in({list(self._values)!r})"
        )


class Between(Expr):
    """A range test: ``column BETWEEN low AND high``.

    Produced by :meth:`Col.is_between() <oceldb.expr._col.Col.is_between>`.

    Args:
        col: The column being tested.
        low: Lower bound (inclusive).
        high: Upper bound (inclusive).
    """

    __slots__ = ("_col", "_low", "_high")

    def __init__(self, col: Col, low: ScalarValue, high: ScalarValue) -> None:
        self._col = col
        self._low = low
        self._high = high

    @property
    def domain(self) -> Domain:
        return self._col.domain

    def compile(self, ctx: CompilationContext | None = None) -> str:
        if ctx is not None:
            ctx.validate_column(self._col.domain, self._col.column)
        return (
            f'"{self._col.column}" BETWEEN '
            f"{sql_literal(self._low)} AND {sql_literal(self._high)}"
        )

    def __repr__(self) -> str:
        return f"{self._col.domain.value}.{self._col.column}.is_between({self._low!r}, {self._high!r})"


# ---------------------------------------------------------------------------
# Combinators
# ---------------------------------------------------------------------------


class And(Expr):
    """Logical conjunction of two same-domain expressions.

    Compiles to ``(left AND right)``. Created via ``expr_a & expr_b``.

    Args:
        left: Left operand.
        right: Right operand (must share *left*'s domain).
    """

    __slots__ = ("_left", "_right")

    def __init__(self, left: Expr, right: Expr) -> None:
        self._left = left
        self._right = right

    @property
    def domain(self) -> Domain:
        return self._left.domain

    def compile(self, ctx: CompilationContext | None = None) -> str:
        return f"({self._left.compile(ctx)} AND {self._right.compile(ctx)})"

    def __repr__(self) -> str:
        return f"({self._left!r} & {self._right!r})"


class Or(Expr):
    """Logical disjunction of two same-domain expressions.

    Compiles to ``(left OR right)``. Created via ``expr_a | expr_b``.

    Args:
        left: Left operand.
        right: Right operand (must share *left*'s domain).
    """

    __slots__ = ("_left", "_right")

    def __init__(self, left: Expr, right: Expr) -> None:
        self._left = left
        self._right = right

    @property
    def domain(self) -> Domain:
        return self._left.domain

    def compile(self, ctx: CompilationContext | None = None) -> str:
        return f"({self._left.compile(ctx)} OR {self._right.compile(ctx)})"

    def __repr__(self) -> str:
        return f"({self._left!r} | {self._right!r})"


class Not(Expr):
    """Logical negation of an expression.

    Compiles to ``NOT (inner)``. Created via ``~expr``.

    Args:
        inner: The expression to negate.
    """

    __slots__ = ("_inner",)

    def __init__(self, inner: Expr) -> None:
        self._inner = inner

    @property
    def domain(self) -> Domain:
        return self._inner.domain

    def compile(self, ctx: CompilationContext | None = None) -> str:
        return f"NOT ({self._inner.compile(ctx)})"

    def __repr__(self) -> str:
        return f"~({self._inner!r})"
