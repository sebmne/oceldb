"""Logical sources — the root of every plan tree.

A ``Source`` knows its scope, the OCEL types it filters to (if any), and how
to render itself as SQL. Column availability is computed against an
``OCELManifest`` by a free function rather than stored on the source, so the
plan IR never holds a live reference to the dataset.

Each concrete source also carries an optional ``SublogFilter`` — the
log-level type filters set at ``Sublog`` time. The native ``selected_types``
of a source handles the "self-axis" filter (e.g. event types on
``EventSource``); ``SublogFilter`` carries the cross-axis constraints
(e.g. object types on ``EventSource``) and the ``drop_orphan_events`` rule.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime

from oceldb.core.manifest import OCELManifest, TableSchema
from oceldb.plan.scope import ScopeKind

# ---------------------------------------------------------------------------
# Sublog filter
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SublogFilter:
    """Log-level type filters that propagate into every source scan.

    ``event_types`` / ``object_types``: the log is restricted to these
    OCEL types. ``None`` means "all types".

    ``drop_orphan_events``: when ``object_types`` is set, events that no
    longer touch any surviving object are filtered out. Has no effect
    when ``object_types`` is ``None``.
    """

    event_types: frozenset[str] | None = None
    object_types: frozenset[str] | None = None
    drop_orphan_events: bool = True

    @property
    def is_identity(self) -> bool:
        return self.event_types is None and self.object_types is None


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------


class Source(ABC):
    """Polymorphic source base.

    ``scope()`` tags the source for validator rules. ``render(alias)`` emits
    the SQL fragment that goes into the FROM clause of the first CTE
    (optionally a subquery with the given alias). ``types`` names the OCEL
    types this source filters to.
    """

    __slots__ = ()

    @abstractmethod
    def scope(self) -> ScopeKind: ...

    @abstractmethod
    def render(self, alias: str) -> str: ...

    @property
    def types(self) -> tuple[str, ...]:
        return ()


# ---------------------------------------------------------------------------
# Canonical-table sources
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EventSource(Source):
    selected_types: tuple[str, ...] = ()
    sublog: SublogFilter | None = None

    def scope(self) -> ScopeKind:
        return "event"

    @property
    def types(self) -> tuple[str, ...]:
        return self.selected_types

    def render(self, alias: str) -> str:
        predicates = _event_type_predicates('"event"."ocel_type"', self.selected_types)
        predicates.extend(_event_orphan_predicates('"event"."ocel_id"', self.sublog))
        if not predicates:
            return f'"event" AS {alias}'
        where = " AND ".join(predicates)
        return f'(SELECT * FROM "event" WHERE {where}) AS {alias}'


@dataclass(frozen=True)
class ObjectSource(Source):
    selected_types: tuple[str, ...] = ()
    sublog: SublogFilter | None = None

    def scope(self) -> ScopeKind:
        return "object"

    @property
    def types(self) -> tuple[str, ...]:
        return self.selected_types

    def render(self, alias: str) -> str:
        predicates = _event_type_predicates('"object"."ocel_type"', self.selected_types)
        predicates.extend(_object_touches_event_predicates('"object"."ocel_id"', self.sublog))
        if not predicates:
            return f'"object" AS {alias}'
        where = " AND ".join(predicates)
        return f'(SELECT * FROM "object" WHERE {where}) AS {alias}'


@dataclass(frozen=True)
class ObjectChangeSource(Source):
    selected_types: tuple[str, ...] = ()
    sublog: SublogFilter | None = None

    def scope(self) -> ScopeKind:
        return "object_change"

    @property
    def types(self) -> tuple[str, ...]:
        return self.selected_types

    def render(self, alias: str) -> str:
        predicates = _event_type_predicates(
            '"object_change"."ocel_type"', self.selected_types
        )
        predicates.extend(
            _object_touches_event_predicates('"object_change"."ocel_id"', self.sublog)
        )
        if not predicates:
            return f'"object_change" AS {alias}'
        where = " AND ".join(predicates)
        return (
            f'(SELECT * FROM "object_change" WHERE {where}) AS {alias}'
        )


@dataclass(frozen=True)
class EventObjectSource(Source):
    sublog: SublogFilter | None = None

    def scope(self) -> ScopeKind:
        return "event_object"

    def render(self, alias: str) -> str:
        if self.sublog is None or self.sublog.is_identity:
            return f'"event_object" AS {alias}'
        predicates = _event_object_predicates(
            event_ref='"event_object"."ocel_event_id"',
            object_ref='"event_object"."ocel_object_id"',
            sublog=self.sublog,
        )
        if not predicates:
            return f'"event_object" AS {alias}'
        where = " AND ".join(predicates)
        return (
            f'(SELECT * FROM "event_object" WHERE {where}) AS {alias}'
        )


@dataclass(frozen=True)
class ObjectObjectSource(Source):
    sublog: SublogFilter | None = None

    def scope(self) -> ScopeKind:
        return "object_object"

    def render(self, alias: str) -> str:
        if self.sublog is None or self.sublog.object_types is None:
            return f'"object_object" AS {alias}'
        types = _type_literal_list(self.sublog.object_types)
        source_in = (
            f'"object_object"."ocel_source_id" IN '
            f'(SELECT "ocel_id" FROM "object" WHERE "ocel_type" IN ({types}))'
        )
        target_in = (
            f'"object_object"."ocel_target_id" IN '
            f'(SELECT "ocel_id" FROM "object" WHERE "ocel_type" IN ({types}))'
        )
        return (
            f'(SELECT * FROM "object_object" WHERE {source_in} AND {target_in}) '
            f"AS {alias}"
        )


# ---------------------------------------------------------------------------
# Derived sources
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EventOccurrenceSource(Source):
    """Event-object incidence rows, one per (event, object) pair."""

    selected_types: tuple[str, ...] = ()  # object types (native axis)
    sublog: SublogFilter | None = None

    def scope(self) -> ScopeKind:
        return "event_occurrence"

    @property
    def types(self) -> tuple[str, ...]:
        return self.selected_types

    def render(self, alias: str) -> str:
        from_sql = (
            '"event_object" eo '
            'JOIN "event" e ON e."ocel_id" = eo."ocel_event_id" '
            'JOIN "object" o ON o."ocel_id" = eo."ocel_object_id"'
        )
        predicates = _event_type_predicates('o."ocel_type"', self.selected_types)
        if self.sublog is not None:
            if self.sublog.event_types is not None:
                values = _type_literal_list(self.sublog.event_types)
                predicates.append(f'e."ocel_type" IN ({values})')
            if self.sublog.object_types is not None:
                values = _type_literal_list(self.sublog.object_types)
                predicates.append(f'o."ocel_type" IN ({values})')
        where_sql = ("WHERE " + " AND ".join(predicates)) if predicates else ""
        return (
            f"(SELECT "
            f'eo."ocel_event_id" AS "ocel_event_id", '
            f'e."ocel_type" AS "ocel_event_type", '
            f'e."ocel_time" AS "ocel_event_time", '
            f'eo."ocel_object_id" AS "ocel_object_id", '
            f'o."ocel_type" AS "ocel_object_type" '
            f"FROM {from_sql} {where_sql}) AS {alias}"
        )


@dataclass(frozen=True)
class ObjectStateSource(Source):
    """Reconstructed object state per identity.

    ``mode`` is one of:
        - ``"latest"``: forward-fill the most recent non-null value per attribute
        - ``("as_of", timestamp)``: state at or before a specific timestamp
        - ``None``: seed state, not yet projected. Must not reach compilation.
    """

    selected_types: tuple[str, ...] = ()
    mode: tuple[str, date | datetime | None] | None = None
    sublog: SublogFilter | None = None

    def scope(self) -> ScopeKind:
        return "object_state"

    @property
    def types(self) -> tuple[str, ...]:
        return self.selected_types

    def with_mode_latest(self) -> "ObjectStateSource":
        return ObjectStateSource(
            selected_types=self.selected_types,
            mode=("latest", None),
            sublog=self.sublog,
        )

    def with_mode_as_of(self, timestamp: date | datetime) -> "ObjectStateSource":
        return ObjectStateSource(
            selected_types=self.selected_types,
            mode=("as_of", timestamp),
            sublog=self.sublog,
        )

    def render(self, alias: str) -> str:
        # Object-state rendering requires manifest context; this is produced by
        # the compiler via render_object_state_source(...). Sources still need
        # a render() for uniformity, but the compiler takes over for this one.
        raise RuntimeError(
            "ObjectStateSource requires manifest-aware rendering via the compiler"
        )


# ---------------------------------------------------------------------------
# Available-column helpers (manifest-aware)
# ---------------------------------------------------------------------------


def source_available_columns(
    source: Source,
    manifest: OCELManifest,
) -> dict[str, str]:
    """Return the column name → SQL type map visible at the source's output."""
    if isinstance(source, EventSource):
        schema = manifest.table("event")
        custom = _custom_for_types(schema, source.selected_types)
        return {**dict(schema.core_columns), **custom}
    if isinstance(source, ObjectSource):
        return dict(manifest.table("object").columns)
    if isinstance(source, ObjectChangeSource):
        schema = manifest.table("object_change")
        custom = _custom_for_types(schema, source.selected_types)
        return {**dict(schema.core_columns), **custom}
    if isinstance(source, ObjectStateSource):
        schema = manifest.table("object_change")
        custom = _custom_for_types(schema, source.selected_types)
        return {
            **dict(manifest.table("object").columns),
            "ocel_time": "TIMESTAMP",
            **custom,
        }
    if isinstance(source, EventOccurrenceSource):
        return {
            "ocel_event_id": "VARCHAR",
            "ocel_event_type": "VARCHAR",
            "ocel_event_time": "TIMESTAMP",
            "ocel_object_id": "VARCHAR",
            "ocel_object_type": "VARCHAR",
        }
    if isinstance(source, EventObjectSource):
        return dict(manifest.table("event_object").columns)
    if isinstance(source, ObjectObjectSource):
        return dict(manifest.table("object_object").columns)
    raise TypeError(f"Unsupported source: {type(source).__name__}")


def _custom_for_types(
    schema: TableSchema,
    selected_types: tuple[str, ...],
) -> dict[str, str]:
    if not selected_types:
        return dict(schema.custom_columns)
    return schema.custom_columns_for_types(selected_types)


# ---------------------------------------------------------------------------
# Predicate helpers for cross-axis filtering
# ---------------------------------------------------------------------------


def _event_type_predicates(
    type_ref: str,
    selected_types: tuple[str, ...],
) -> list[str]:
    """Predicate list for a source's own native type axis (selected_types)."""
    if not selected_types:
        return []
    values = _type_literal_list(selected_types)
    return [f"{type_ref} IN ({values})"]


def _event_orphan_predicates(
    event_id_ref: str,
    sublog: SublogFilter | None,
) -> list[str]:
    """Cross-axis object-types + drop_orphan_events filter on an event-shaped source."""
    if (
        sublog is None
        or sublog.object_types is None
        or not sublog.drop_orphan_events
    ):
        return []
    types = _type_literal_list(sublog.object_types)
    return [
        f"EXISTS (SELECT 1 FROM \"event_object\" eo "
        f'JOIN "object" o ON o."ocel_id" = eo."ocel_object_id" '
        f'WHERE eo."ocel_event_id" = {event_id_ref} '
        f"AND o.\"ocel_type\" IN ({types}))"
    ]


def _object_touches_event_predicates(
    object_id_ref: str,
    sublog: SublogFilter | None,
) -> list[str]:
    """Cross-axis event-types filter on an object-shaped source."""
    if sublog is None or sublog.event_types is None:
        return []
    types = _type_literal_list(sublog.event_types)
    return [
        f"EXISTS (SELECT 1 FROM \"event_object\" eo "
        f'JOIN "event" e ON e."ocel_id" = eo."ocel_event_id" '
        f'WHERE eo."ocel_object_id" = {object_id_ref} '
        f"AND e.\"ocel_type\" IN ({types}))"
    ]


def _event_object_predicates(
    *,
    event_ref: str,
    object_ref: str,
    sublog: SublogFilter,
) -> list[str]:
    predicates: list[str] = []
    if sublog.event_types is not None:
        types = _type_literal_list(sublog.event_types)
        predicates.append(
            f"{event_ref} IN "
            f"(SELECT \"ocel_id\" FROM \"event\" WHERE \"ocel_type\" IN ({types}))"
        )
    if sublog.object_types is not None:
        types = _type_literal_list(sublog.object_types)
        predicates.append(
            f"{object_ref} IN "
            f"(SELECT \"ocel_id\" FROM \"object\" WHERE \"ocel_type\" IN ({types}))"
        )
    return predicates


def _type_literal_list(types: tuple[str, ...] | frozenset[str]) -> str:
    return ", ".join(_string_literal(t) for t in sorted(types))


def _string_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


# Back-compat re-export retained in case external callers imported from the
# old location.
def render_object_change_batches_source(
    object_change_columns: tuple[str, ...],
    *,
    object_types: tuple[str, ...] = (),
    as_of: date | datetime | str | None = None,
) -> str:
    """Render the GROUP-BY-collapsed object_change source used by discovery.

    Preserved verbatim from ``oceldb/sql/object_history.py`` so the lifecycle
    mining code keeps working unchanged.
    """
    custom_columns = tuple(
        name
        for name in object_change_columns
        if name not in ("ocel_id", "ocel_type", "ocel_time", "ocel_changed_field")
    )
    grouped_columns = ",\n            ".join(
        f'MAX(c."{name}") AS "{name}"' for name in custom_columns
    )
    select_grouped = f",\n            {grouped_columns}" if grouped_columns else ""

    predicates: list[str] = []
    if object_types:
        type_values = ", ".join(_render_literal(v) for v in object_types)
        predicates.append(f'c."ocel_type" IN ({type_values})')
    if as_of is not None:
        predicates.append(f'c."ocel_time" <= {_render_literal(as_of)}')

    where_sql = ""
    if predicates:
        where_sql = "WHERE " + " AND ".join(predicates)

    return f"""
        SELECT
            c."ocel_id",
            c."ocel_time"{select_grouped}
        FROM "object_change" c
        {where_sql}
        GROUP BY c."ocel_id", c."ocel_time"
    """


def _render_literal(value: date | datetime | str) -> str:
    if isinstance(value, datetime):
        return f"'{value.isoformat(sep=' ')}'"
    if isinstance(value, date):
        return f"'{value.isoformat()}'"
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


__all__ = [
    "EventObjectSource",
    "EventOccurrenceSource",
    "EventSource",
    "ObjectChangeSource",
    "ObjectObjectSource",
    "ObjectSource",
    "ObjectStateSource",
    "Source",
    "SublogFilter",
    "render_object_change_batches_source",
    "source_available_columns",
]
