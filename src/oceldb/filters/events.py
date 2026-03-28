"""Structural event filters for OCEL 2.0 process analysis.

These filters express temporal ordering relationships between event types
through shared objects — the building blocks of process mining queries.
They compile to ``ocel_id IN (SELECT ...)`` subqueries that join through the
event-to-object (E2O) table.

Both filters compose naturally with attribute predicates via ``&`` / ``|``::

    from oceldb import event
    from oceldb.filters.events import EventuallyFollows

    expr = EventuallyFollows("Create Order", "Pay Order") & (
        event.payment_method == "credit_card"
    )

Implementing a custom structural filter
----------------------------------------

Subclass :class:`~oceldb.expr._expr.Expr`, set :attr:`domain` to
:attr:`~oceldb.types.Domain.EVENT`, and use the
:class:`~oceldb.expr._context.CompilationContext` in :meth:`compile`::

    class MyFilter(Expr):
        @property
        def domain(self) -> Domain:
            return Domain.EVENT

        def compile(self, ctx):
            e2o = ctx.table_ref("event_object")
            unified = ctx.unified_sql(Domain.EVENT)
            return f"ocel_id IN (SELECT ... FROM {e2o} ... JOIN {unified} ...)"
"""

from __future__ import annotations

from oceldb.expr._context import CompilationContext
from oceldb.expr._expr import Expr
from oceldb.types import Domain
from oceldb.utils import sql_literal


class EventuallyFollows(Expr):
    """Filter events where *source_type* eventually leads to *target_type*.

    Two events satisfy this relation when they share at least one object
    and the target event's timestamp is strictly after the source's. Other
    events may occur in between.

    The result set contains the **target** events (not the sources).

    Example::

        from oceldb.filters.events import EventuallyFollows

        # Keep all "Pay Order" events that were preceded by a "Create Order"
        ocel.view().where(
            EventuallyFollows("Create Order", "Pay Order")
        ).create()

    Args:
        source_type: The OCEL event type that must occur first.
        target_type: The OCEL event type that must occur after the source.
    """

    __slots__ = ("_source", "_target")

    def __init__(self, source_type: str, target_type: str) -> None:
        self._source = source_type
        self._target = target_type

    @property
    def domain(self) -> Domain:
        """Always :attr:`~oceldb.types.Domain.EVENT`."""
        return Domain.EVENT

    def compile(self, ctx: CompilationContext | None = None) -> str:
        """Compile to an ``ocel_id IN (SELECT ...)`` subquery.

        The generated SQL joins the E2O table with itself on shared objects
        and filters by type and temporal ordering.

        Args:
            ctx: Required. Provides table references for the E2O table and
                the unified event subquery.

        Returns:
            A SQL boolean fragment.

        Raises:
            ValueError: If *ctx* is ``None``.
        """
        if ctx is None:
            raise ValueError("EventuallyFollows requires a CompilationContext")
        e2o = ctx.table_ref("event_object")
        unified = ctx.unified_sql(Domain.EVENT)
        src = sql_literal(self._source)
        tgt = sql_literal(self._target)
        return (
            f"ocel_id IN ("
            f"SELECT DISTINCT te.ocel_event_id "
            f"FROM {e2o} se "
            f"JOIN {e2o} te ON se.ocel_object_id = te.ocel_object_id "
            f"JOIN {unified} s ON se.ocel_event_id = s.ocel_id "
            f"JOIN {unified} t ON te.ocel_event_id = t.ocel_id "
            f"WHERE s.ocel_type = {src} "
            f"AND t.ocel_type = {tgt} "
            f"AND t.ocel_time > s.ocel_time"
            f")"
        )

    def __repr__(self) -> str:
        return f"EventuallyFollows({self._source!r}, {self._target!r})"


class DirectlyFollows(Expr):
    """Filter events where *source_type* directly leads to *target_type*.

    Like :class:`EventuallyFollows`, but additionally requires that **no
    event of any type** occurs between source and target for the shared
    object. This is enforced with a ``NOT EXISTS`` subquery.

    The result set contains the **target** events (not the sources).

    Example::

        from oceldb.filters.events import DirectlyFollows

        # Keep "Pay Order" events that immediately follow "Create Order"
        # (no intervening events for the same object)
        ocel.view().where(
            DirectlyFollows("Create Order", "Pay Order")
        ).create()

    Args:
        source_type: The OCEL event type that must occur first.
        target_type: The OCEL event type that must occur immediately after.
    """

    __slots__ = ("_source", "_target")

    def __init__(self, source_type: str, target_type: str) -> None:
        self._source = source_type
        self._target = target_type

    @property
    def domain(self) -> Domain:
        """Always :attr:`~oceldb.types.Domain.EVENT`."""
        return Domain.EVENT

    def compile(self, ctx: CompilationContext | None = None) -> str:
        """Compile to an ``ocel_id IN (SELECT ... AND NOT EXISTS ...)`` subquery.

        Extends the eventually-follows join with a ``NOT EXISTS`` clause
        that eliminates pairs where any event falls between source and
        target for the same object.

        Args:
            ctx: Required. Provides table references for the E2O table and
                the unified event subquery.

        Returns:
            A SQL boolean fragment.

        Raises:
            ValueError: If *ctx* is ``None``.
        """
        if ctx is None:
            raise ValueError("DirectlyFollows requires a CompilationContext")
        e2o = ctx.table_ref("event_object")
        unified = ctx.unified_sql(Domain.EVENT)
        src = sql_literal(self._source)
        tgt = sql_literal(self._target)
        return (
            f"ocel_id IN ("
            f"SELECT DISTINCT te.ocel_event_id "
            f"FROM {e2o} se "
            f"JOIN {e2o} te ON se.ocel_object_id = te.ocel_object_id "
            f"JOIN {unified} s ON se.ocel_event_id = s.ocel_id "
            f"JOIN {unified} t ON te.ocel_event_id = t.ocel_id "
            f"WHERE s.ocel_type = {src} "
            f"AND t.ocel_type = {tgt} "
            f"AND t.ocel_time > s.ocel_time "
            f"AND NOT EXISTS ("
            f"SELECT 1 FROM {e2o} me "
            f"JOIN {unified} m ON me.ocel_event_id = m.ocel_id "
            f"WHERE me.ocel_object_id = se.ocel_object_id "
            f"AND m.ocel_time > s.ocel_time "
            f"AND m.ocel_time < t.ocel_time"
            f")"
            f")"
        )

    def __repr__(self) -> str:
        return f"DirectlyFollows({self._source!r}, {self._target!r})"
