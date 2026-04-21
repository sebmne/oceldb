"""Finite-state classes for the fluent query API.

Each class exposes the operations valid at that point in the chain. The FSM
is enforced by mixin composition: e.g. ``SelectedRows`` does not mix in
``MaterializeMixin`` so ``.ids()`` / ``.to_ocel()`` / ``.write()`` are simply
not methods on it.
"""

from __future__ import annotations

from datetime import date, datetime

from oceldb.api.base import (
    ExecutionMixin,
    GroupByMixin,
    HavingMixin,
    LimitMixin,
    MaterializeMixin,
    RenameMixin,
    SelectMixin,
    SortMixin,
    UniqueMixin,
    WhereMixin,
    WithColumnsMixin,
    coerce_aggregate_exprs,
    coerce_aggregate_named_exprs,
)
from oceldb.core.ocel import OCEL
from oceldb.expr.nodes import AliasExpr, Expr
from oceldb.plan.nodes import GroupPlan, PlanNode, SourcePlan
from oceldb.plan.sources import ObjectStateSource


# ---------------------------------------------------------------------------
# Row queries (row-preserving).
# ---------------------------------------------------------------------------


class RowsQuery(
    GroupByMixin,
    WithColumnsMixin,
    RenameMixin,
    SelectMixin,
    LimitMixin,
    UniqueMixin,
    SortMixin,
    WhereMixin,
    ExecutionMixin,
):
    """Common row-preserving operations."""


class EventRows(MaterializeMixin, RowsQuery):
    """Lazy event-row query with one row per event."""


class ObjectRows(MaterializeMixin, RowsQuery):
    """Lazy logical-object query with one row per object identity."""


class FlatEventRows(RowsQuery):
    """Lazy query over object-timeline event occurrences."""


class ObjectChangeRows(RowsQuery):
    """Lazy query over raw sparse object-history rows."""


class ObjectStateRows(MaterializeMixin, RowsQuery):
    """Lazy query over reconstructed object states with one row per object."""


class EventObjectRows(RowsQuery):
    """Lazy query over event-object incidence edges."""


class ObjectObjectRows(RowsQuery):
    """Lazy query over object-object relation edges."""


# ---------------------------------------------------------------------------
# Projected / grouped states.
# ---------------------------------------------------------------------------


class SelectedRows(
    RenameMixin,
    LimitMixin,
    UniqueMixin,
    SortMixin,
    WhereMixin,
    ExecutionMixin,
):
    """Result of ``.select(...)`` — no identity or materialization methods."""


class AggregatedRows(
    RenameMixin,
    SelectMixin,
    LimitMixin,
    SortMixin,
    HavingMixin,
    ExecutionMixin,
):
    """Result of ``.group_by(...).agg(...)``."""


class GroupedRows:
    def __init__(
        self,
        ocel: OCEL,
        node: PlanNode,
        groupings: tuple[Expr, ...],
    ) -> None:
        self._ocel = ocel
        self._node = node
        self._groupings = groupings

    def agg(
        self,
        *exprs: Expr | AliasExpr,
        **named_exprs: Expr,
    ) -> AggregatedRows:
        aggregations = (
            coerce_aggregate_exprs(exprs, context="agg(...)")
            + coerce_aggregate_named_exprs((), named_exprs, context="agg(...)")
        )
        if not aggregations:
            raise ValueError("agg(...) requires at least one expression")
        return AggregatedRows(
            self._ocel,
            GroupPlan(self._node, self._groupings, aggregations),
        )


# ---------------------------------------------------------------------------
# Object-state seed (pre-projection).
# ---------------------------------------------------------------------------


class ObjectStateSeed:
    """Incomplete object-state query that still requires a temporal projection.

    ``.latest()`` and ``.as_of(...)`` fix the temporal projection and return
    an ``ObjectStateRows`` with the full row-query surface.
    """

    def __init__(self, ocel: OCEL, node: PlanNode) -> None:
        self._ocel = ocel
        self._node = node

    def latest(self) -> ObjectStateRows:
        source = _seed_source(self._node)
        return ObjectStateRows(
            self._ocel,
            SourcePlan(source.with_mode_latest()),
        )

    def as_of(self, value: date | datetime | str) -> ObjectStateRows:
        source = _seed_source(self._node)
        if isinstance(value, str):
            timestamp: date | datetime = datetime.fromisoformat(value)
        else:
            timestamp = value
        return ObjectStateRows(
            self._ocel,
            SourcePlan(source.with_mode_as_of(timestamp)),
        )


def _seed_source(node: PlanNode) -> ObjectStateSource:
    if not isinstance(node, SourcePlan) or not isinstance(
        node.source, ObjectStateSource
    ):
        raise TypeError(
            "Object-state projection is only valid for object_states(...) seeds"
        )
    return node.source


__all__ = [
    "AggregatedRows",
    "EventObjectRows",
    "FlatEventRows",
    "EventRows",
    "GroupedRows",
    "ObjectChangeRows",
    "ObjectObjectRows",
    "ObjectRows",
    "ObjectStateRows",
    "ObjectStateSeed",
    "RowsQuery",
    "SelectedRows",
]
