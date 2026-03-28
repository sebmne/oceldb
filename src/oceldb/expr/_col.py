"""Col — a typed column reference that produces Expr nodes via operators."""

from __future__ import annotations

from collections.abc import Sequence

from oceldb.expr._expr import Between, Comparison, InSet, Not
from oceldb.expr._types import Op
from oceldb.types import Domain, ScalarValue


class Col:
    """A typed reference to a column in the OCEL schema.

    ``Col`` is **not** an :class:`~oceldb.expr._expr.Expr` — it is a column
    handle that *produces* expressions via operators. This prevents misuse
    like ``ocel.view().where(event.type)`` (a bare column is not a boolean
    condition).

    Typically created through the :data:`~oceldb.expr.event` or
    :data:`~oceldb.expr.obj` proxy singletons::

        event.type == "Create Order"      # → Comparison
        event.time.is_between("a", "b")   # → Between
        event.type.is_in(["A", "B"])      # → InSet

    Args:
        domain: Whether this column belongs to events or objects.
        column: The raw column name (e.g. ``"ocel_type"``, ``"total_price"``).
    """

    __slots__ = ("_domain", "_column")

    def __init__(self, domain: Domain, column: str) -> None:
        self._domain = domain
        self._column = column

    @property
    def domain(self) -> Domain:
        """The entity domain this column belongs to."""
        return self._domain

    @property
    def column(self) -> str:
        """The raw column name in the OCEL schema."""
        return self._column

    # -- Comparison operators → Expr -------------------------------------------

    def __eq__(self, other: ScalarValue) -> Comparison:  # type: ignore[override]
        """Create an equality comparison (SQL ``=``).

        Args:
            other: The value to compare against.
        """
        return Comparison(self, Op.EQ, other)

    def __ne__(self, other: ScalarValue) -> Comparison:  # type: ignore[override]
        """Create an inequality comparison (SQL ``!=``).

        Args:
            other: The value to compare against.
        """
        return Comparison(self, Op.NE, other)

    def __gt__(self, other: ScalarValue) -> Comparison:
        """Create a greater-than comparison (SQL ``>``).

        Args:
            other: The value to compare against.
        """
        return Comparison(self, Op.GT, other)

    def __ge__(self, other: ScalarValue) -> Comparison:
        """Create a greater-or-equal comparison (SQL ``>=``).

        Args:
            other: The value to compare against.
        """
        return Comparison(self, Op.GE, other)

    def __lt__(self, other: ScalarValue) -> Comparison:
        """Create a less-than comparison (SQL ``<``).

        Args:
            other: The value to compare against.
        """
        return Comparison(self, Op.LT, other)

    def __le__(self, other: ScalarValue) -> Comparison:
        """Create a less-or-equal comparison (SQL ``<=``).

        Args:
            other: The value to compare against.
        """
        return Comparison(self, Op.LE, other)

    # -- Set / range operations → Expr -----------------------------------------

    def is_in(self, values: Sequence[ScalarValue]) -> InSet:
        """Create a set membership test (SQL ``IN``).

        Args:
            values: One or more values to test against.

        Raises:
            ValueError: If *values* is empty.
        """
        return InSet(self, values)

    def not_in(self, values: Sequence[ScalarValue]) -> Not:
        """Create a negated set membership test (SQL ``NOT IN``).

        Args:
            values: One or more values to test against.

        Raises:
            ValueError: If *values* is empty.
        """
        return Not(InSet(self, values))

    def is_between(self, low: ScalarValue, high: ScalarValue) -> Between:
        """Create a range test (SQL ``BETWEEN``).

        Args:
            low: Lower bound (inclusive).
            high: Upper bound (inclusive).
        """
        return Between(self, low, high)

    def not_between(self, low: ScalarValue, high: ScalarValue) -> Not:
        """Create a negated range test (SQL ``NOT BETWEEN``).

        Args:
            low: Lower bound (inclusive).
            high: Upper bound (inclusive).
        """
        return Not(Between(self, low, high))

    # -- Display ---------------------------------------------------------------

    def __repr__(self) -> str:
        return f"Col({self._domain.value}.{self._column})"
