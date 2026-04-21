"""Logical sources — the root of every plan tree.

A ``Source`` knows its scope, the OCEL types it filters to (if any), and how
to render itself as SQL. Column availability is computed against an
``OCELManifest`` by a free function rather than stored on the source, so the
plan IR never holds a live reference to the dataset.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime

from oceldb.core.manifest import OCELManifest, TableSchema
from oceldb.plan.scope import ScopeKind

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

    def scope(self) -> ScopeKind:
        return "event"

    @property
    def types(self) -> tuple[str, ...]:
        return self.selected_types

    def render(self, alias: str) -> str:
        if not self.selected_types:
            return f'"event" AS {alias}'
        types_sql = ", ".join(_string_literal(t) for t in self.selected_types)
        return f'(SELECT * FROM "event" WHERE "ocel_type" IN ({types_sql})) AS {alias}'


@dataclass(frozen=True)
class ObjectSource(Source):
    selected_types: tuple[str, ...] = ()

    def scope(self) -> ScopeKind:
        return "object"

    @property
    def types(self) -> tuple[str, ...]:
        return self.selected_types

    def render(self, alias: str) -> str:
        if not self.selected_types:
            return f'"object" AS {alias}'
        types_sql = ", ".join(_string_literal(t) for t in self.selected_types)
        return f'(SELECT * FROM "object" WHERE "ocel_type" IN ({types_sql})) AS {alias}'


@dataclass(frozen=True)
class ObjectChangeSource(Source):
    selected_types: tuple[str, ...] = ()

    def scope(self) -> ScopeKind:
        return "object_change"

    @property
    def types(self) -> tuple[str, ...]:
        return self.selected_types

    def render(self, alias: str) -> str:
        if not self.selected_types:
            return f'"object_change" AS {alias}'
        types_sql = ", ".join(_string_literal(t) for t in self.selected_types)
        return (
            f'(SELECT * FROM "object_change" WHERE "ocel_type" IN ({types_sql})) '
            f"AS {alias}"
        )


@dataclass(frozen=True)
class EventObjectSource(Source):
    def scope(self) -> ScopeKind:
        return "event_object"

    def render(self, alias: str) -> str:
        return f'"event_object" AS {alias}'


@dataclass(frozen=True)
class ObjectObjectSource(Source):
    def scope(self) -> ScopeKind:
        return "object_object"

    def render(self, alias: str) -> str:
        return f'"object_object" AS {alias}'


# ---------------------------------------------------------------------------
# Derived sources
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EventOccurrenceSource(Source):
    """Event-object incidence rows, one per (event, object) pair."""

    selected_types: tuple[str, ...] = ()  # object types

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
        type_filter = ""
        if self.selected_types:
            types_sql = ", ".join(_string_literal(t) for t in self.selected_types)
            type_filter = f'WHERE o."ocel_type" IN ({types_sql})'
        return (
            f"(SELECT "
            f'eo."ocel_event_id" AS "ocel_event_id", '
            f'e."ocel_type" AS "ocel_event_type", '
            f'e."ocel_time" AS "ocel_event_time", '
            f'eo."ocel_object_id" AS "ocel_object_id", '
            f'o."ocel_type" AS "ocel_object_type" '
            f"FROM {from_sql} {type_filter}) AS {alias}"
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

    def scope(self) -> ScopeKind:
        return "object_state"

    @property
    def types(self) -> tuple[str, ...]:
        return self.selected_types

    def with_mode_latest(self) -> "ObjectStateSource":
        return ObjectStateSource(
            selected_types=self.selected_types,
            mode=("latest", None),
        )

    def with_mode_as_of(self, timestamp: date | datetime) -> "ObjectStateSource":
        return ObjectStateSource(
            selected_types=self.selected_types,
            mode=("as_of", timestamp),
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
    "render_object_change_batches_source",
    "source_available_columns",
]
