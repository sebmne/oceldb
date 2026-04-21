"""Log-level query surface.

``Sublog`` is the entry point returned from ``OCEL.query``. It carries the
optional event-type and object-type filters that narrow the log, plus the
``drop_orphan_events`` rule. Grain methods (``events``, ``flatten``, ...)
build ``SourcePlan`` s with those filters threaded in, so downstream row-level
queries run against the restricted log.

Stage 1 scope: filters that a ``Source`` natively carries as ``selected_types``
are applied. Cross-cut propagation (e.g. dropping orphan events, injecting the
type filter into relation-predicate join subqueries, materializing a narrowed
sublog to a new OCEL) lands in Stage 2.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from oceldb.api.states import (
    EventObjectRows,
    EventRows,
    FlatEventRows,
    ObjectChangeRows,
    ObjectObjectRows,
    ObjectRows,
    ObjectStateSeed,
)
from oceldb.plan.nodes import SourcePlan
from oceldb.plan.sources import (
    EventObjectSource,
    EventOccurrenceSource,
    EventSource,
    ObjectChangeSource,
    ObjectObjectSource,
    ObjectSource,
    ObjectStateSource,
    SublogFilter,
)

if TYPE_CHECKING:
    from oceldb.core.ocel import OCEL


# ---------------------------------------------------------------------------
# Sublog
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Sublog:
    """A log-level handle carrying optional type filters.

    Instances are immutable; ``sublog(...)`` returns a new ``Sublog`` with the
    filters intersected. ``OCEL.query`` returns the identity sublog (no
    filters, ``drop_orphan_events=True``).
    """

    ocel: "OCEL"
    event_types: frozenset[str] | None = None
    object_types: frozenset[str] | None = None
    drop_orphan_events: bool = True
    _identity: bool = field(default=True, repr=False, compare=False)

    # -- narrowing -----------------------------------------------------------

    def sublog(
        self,
        *,
        event_types: Iterable[str] | None = None,
        object_types: Iterable[str] | None = None,
        drop_orphan_events: bool | None = None,
    ) -> "Sublog":
        """Return a narrowed sublog.

        Filters intersect with the current ones: passing ``event_types=["A"]``
        on a sublog already restricted to ``{"A", "B"}`` yields ``{"A"}``. An
        empty intersection raises ``ValueError`` because it almost always
        indicates a typo.
        """
        new_event_types = _intersect_types(
            self.event_types, event_types, label="event_types"
        )
        new_object_types = _intersect_types(
            self.object_types, object_types, label="object_types"
        )
        new_drop = (
            self.drop_orphan_events
            if drop_orphan_events is None
            else drop_orphan_events
        )
        return replace(
            self,
            event_types=new_event_types,
            object_types=new_object_types,
            drop_orphan_events=new_drop,
            _identity=False,
        )

    # -- grain roots ---------------------------------------------------------

    def events(self, *event_types: str) -> EventRows:
        """Event-grained row query, optionally narrowed further."""
        types = _resolve_types(self.event_types, event_types, label="event")
        return EventRows(
            self.ocel,
            SourcePlan(EventSource(selected_types=types, sublog=self._filter())),
        )

    def objects(self, *object_types: str) -> ObjectRows:
        """Object-identity-grained row query."""
        types = _resolve_types(self.object_types, object_types, label="object")
        return ObjectRows(
            self.ocel,
            SourcePlan(ObjectSource(selected_types=types, sublog=self._filter())),
        )

    def object_states(self, *object_types: str) -> ObjectStateSeed:
        """Reconstructed object-state seed.

        Requires ``.latest()`` or ``.as_of(t)`` before any row operator.
        """
        types = _resolve_types(self.object_types, object_types, label="object")
        return ObjectStateSeed(
            self.ocel,
            SourcePlan(
                ObjectStateSource(selected_types=types, sublog=self._filter())
            ),
        )

    def object_changes(self, *object_types: str) -> ObjectChangeRows:
        """Raw object-history query."""
        types = _resolve_types(self.object_types, object_types, label="object")
        return ObjectChangeRows(
            self.ocel,
            SourcePlan(
                ObjectChangeSource(selected_types=types, sublog=self._filter())
            ),
        )

    def flatten(self, *object_types: str) -> FlatEventRows:
        """Flattened object-timeline query (one row per (event, object))."""
        types = _resolve_types(self.object_types, object_types, label="object")
        return FlatEventRows(
            self.ocel,
            SourcePlan(
                EventOccurrenceSource(selected_types=types, sublog=self._filter())
            ),
        )

    def event_objects(self) -> EventObjectRows:
        """Raw event-object incidence rows."""
        return EventObjectRows(
            self.ocel, SourcePlan(EventObjectSource(sublog=self._filter()))
        )

    def object_objects(self) -> ObjectObjectRows:
        """Raw object-object link rows."""
        return ObjectObjectRows(
            self.ocel, SourcePlan(ObjectObjectSource(sublog=self._filter()))
        )

    # -- internal -----------------------------------------------------------

    def _filter(self) -> SublogFilter | None:
        """Return the SublogFilter to attach to a Source, or None for identity."""
        if self._identity:
            return None
        return SublogFilter(
            event_types=self.event_types,
            object_types=self.object_types,
            drop_orphan_events=self.drop_orphan_events,
        )

    # -- catalog helpers -----------------------------------------------------

    def event_count(self) -> int:
        """Number of events in the sublog."""
        where = _event_type_where(self.event_types)
        row = self.ocel.sql(
            f'SELECT COUNT(*) FROM "event"{where}'
        ).fetchone()
        return 0 if row is None else int(row[0])

    def object_count(self) -> int:
        """Number of object identities in the sublog."""
        where = _object_type_where(self.object_types)
        row = self.ocel.sql(
            f'SELECT COUNT(*) FROM "object"{where}'
        ).fetchone()
        return 0 if row is None else int(row[0])

    def event_ids(self) -> list[str]:
        """All event ``ocel_id`` values in the sublog, in storage order."""
        where = _event_type_where(self.event_types)
        rows = self.ocel.sql(
            f'SELECT "ocel_id" FROM "event"{where}'
        ).fetchall()
        return [row[0] for row in rows]

    def object_ids(self) -> list[str]:
        """All object ``ocel_id`` values in the sublog, in storage order."""
        where = _object_type_where(self.object_types)
        rows = self.ocel.sql(
            f'SELECT "ocel_id" FROM "object"{where}'
        ).fetchall()
        return [row[0] for row in rows]

    def event_type_names(self) -> list[str]:
        """Distinct event types present in the sublog."""
        where = _event_type_where(self.event_types)
        rows = self.ocel.sql(
            f'SELECT DISTINCT "ocel_type" FROM "event"{where} ORDER BY "ocel_type"'
        ).fetchall()
        return [row[0] for row in rows]

    def object_type_names(self) -> list[str]:
        """Distinct object types present in the sublog."""
        where = _object_type_where(self.object_types)
        rows = self.ocel.sql(
            f'SELECT DISTINCT "ocel_type" FROM "object"{where} ORDER BY "ocel_type"'
        ).fetchall()
        return [row[0] for row in rows]

    # -- materialization -----------------------------------------------------

    def to_ocel(self) -> "OCEL":
        """Materialize the sublog as a new OCEL dataset directory.

        Always returns a fresh ``OCEL`` handle. An identity sublog still
        materializes — it just copies the full dataset; callers who only want
        a handle to the underlying OCEL should use ``self.ocel`` instead.
        """
        from oceldb.api.materialize import materialize_sublog

        return materialize_sublog(
            self.ocel,
            SublogFilter(
                event_types=self.event_types,
                object_types=self.object_types,
                drop_orphan_events=self.drop_orphan_events,
            ),
        )


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _intersect_types(
    current: frozenset[str] | None,
    new: Iterable[str] | None,
    *,
    label: str,
) -> frozenset[str] | None:
    if new is None:
        return current
    new_set = frozenset(new)
    if not new_set:
        raise ValueError(f"{label}=[] is empty; pass None to clear the filter")
    if current is None:
        return new_set
    intersection = current & new_set
    if not intersection:
        raise ValueError(
            f"sublog {label} filter {sorted(new_set)!r} has no overlap with "
            f"the current sublog's {sorted(current)!r}"
        )
    return intersection


def _resolve_types(
    sublog_filter: frozenset[str] | None,
    method_types: tuple[str, ...],
    *,
    label: str,
) -> tuple[str, ...]:
    """Combine a sublog-level filter with a per-method type argument.

    If both are given, the method argument must be a subset of the sublog
    filter. The resulting tuple is ordered: method order wins, else sorted.
    """
    if not method_types:
        return tuple(sorted(sublog_filter)) if sublog_filter else ()
    method_set = frozenset(method_types)
    if sublog_filter is not None and not method_set <= sublog_filter:
        extra = sorted(method_set - sublog_filter)
        raise ValueError(
            f"{label} types {extra!r} are not in the current sublog's "
            f"filter {sorted(sublog_filter)!r}"
        )
    return method_types


def _event_type_where(types: frozenset[str] | None) -> str:
    if not types:
        return ""
    values = ", ".join(_string_literal(t) for t in sorted(types))
    return f' WHERE "ocel_type" IN ({values})'


def _object_type_where(types: frozenset[str] | None) -> str:
    if not types:
        return ""
    values = ", ".join(_string_literal(t) for t in sorted(types))
    return f' WHERE "ocel_type" IN ({values})'


def _string_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


__all__ = ["Sublog"]
