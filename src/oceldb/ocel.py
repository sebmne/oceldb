"""User-facing OCEL handle built from five lazy Polars tables.

The :class:`OCEL` class represents an OCEL 2.0 log as lazy dataframe views. It
does not own a database connection and it does not materialize data until the
caller executes a Polars action such as :meth:`polars.LazyFrame.collect`.

The constructor trusts that the supplied frames already follow the oceldb
schema:

* ``events``: ``ocel_id``, ``ocel_time``, ``ocel_type``, and event attributes.
* ``objects``: ``ocel_id`` and ``ocel_type``.
* ``object_changes``: ``ocel_id``, ``ocel_time``, ``ocel_type``,
  ``ocel_changed_field``, and object attributes.
* ``e2o``: ``ocel_event_id``, ``ocel_event_type``, ``ocel_object_id``,
  ``ocel_object_type``, and ``ocel_qualifier``.
* ``o2o``: ``ocel_source_id``, ``ocel_source_type``, ``ocel_target_id``,
  ``ocel_target_type``, and ``ocel_qualifier``.
"""

import html
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar, cast

_T = TypeVar("_T")

import polars as pl

from oceldb import schema as s
from oceldb.store import read_frames, write_frames

_TYPE_PREVIEW_LIMIT = 6


@dataclass(frozen=True)
class _Overview:
    events: int
    objects: int
    object_changes: int
    e2o: int
    o2o: int
    event_types: list[str]
    object_types: list[str]


class OCEL:
    """An OCEL 2.0 log exposed as lazy Polars dataframes.

    Create an ``OCEL`` from existing ``pl.LazyFrame`` objects when your data is
    already in memory or opened through another Polars scanner. Use
    :meth:`read` when the log has already been written in oceldb's native
    Parquet layout.

    The object is intentionally lightweight: accessor methods return the stored
    lazy frames or lazy transformations of them. They do not validate referential
    integrity and they do not execute queries until you call ``collect()``,
    ``sink_parquet()``, or another Polars execution method.

    Examples:
        >>> from oceldb import OCEL
        >>> ocel = OCEL.read("converted-log")
        >>> counts = (
        ...     ocel.events()
        ...     .group_by("ocel_type")
        ...     .len()
        ...     .collect()
        ... )
        >>> order_events = ocel.events("Place Order", "Pay Order")
    """

    __slots__ = ("_events", "_objects", "_object_changes", "_o2o", "_e2o")

    def __init__(
        self,
        events: pl.LazyFrame,
        objects: pl.LazyFrame,
        object_changes: pl.LazyFrame,
        o2o: pl.LazyFrame,
        e2o: pl.LazyFrame,
    ) -> None:
        """Build an ``OCEL`` from its five logical tables.

        Args:
            events: Lazy frame containing event rows. Required columns are
                ``ocel_id``, ``ocel_time``, and ``ocel_type``; any additional
                columns are treated as event attributes.
            objects: Lazy frame containing object identities with ``ocel_id``
                and ``ocel_type``.
            object_changes: Lazy frame containing sparse object attribute
                changes. Required columns are ``ocel_id``, ``ocel_time``,
                ``ocel_type``, and ``ocel_changed_field``; any additional
                columns are treated as object attributes.
            o2o: Lazy frame containing object-to-object relations.
            e2o: Lazy frame containing event-to-object relations.

        Notes:
            The constructor is deliberately zero-copy and lazy. It does not
            check schemas, sort rows, drop dangling relations, or collect any
            frame. Validate or normalize data before constructing ``OCEL`` if
            those guarantees matter for your application.
        """
        self._events = events
        self._objects = objects
        self._object_changes = object_changes
        self._o2o = o2o
        self._e2o = e2o

    def events(self, *types: str) -> pl.LazyFrame:
        """Return events, optionally restricted to selected event types.

        Args:
            *types: Event type names to keep. When omitted, all events and all
                event attribute columns are returned.

        Returns:
            A lazy frame with ``ocel_id``, ``ocel_time``, event attributes, and
            ``ocel_type``. When ``types`` are provided, attribute columns that
            are entirely null for the selected event types are omitted so the
            result is easier to inspect and write.

        Examples:
            >>> ocel.events().select("ocel_id", "ocel_type").collect()
            >>> ocel.events("Place Order", "Pay Order").sort("ocel_time")
        """
        if not types:
            return self._events
        return _select_types(self._events, types, (s.OCEL_ID, s.OCEL_TIME))

    def objects(self, *types: str) -> pl.LazyFrame:
        """Return object identities, optionally restricted to object types.

        Args:
            *types: Object type names to keep. When omitted, all objects are
                returned.

        Returns:
            A lazy frame with ``ocel_id`` and ``ocel_type``.

        Examples:
            >>> all_objects = ocel.objects()
            >>> orders = ocel.objects("order")
        """
        if not types:
            return self._objects
        return self._objects.filter(pl.col(s.OCEL_TYPE).is_in(list(types)))

    def object_changes(self, *types: str) -> pl.LazyFrame:
        """Return raw sparse object attribute changes.

        Args:
            *types: Object type names to keep. When omitted, all object change
                rows and all object attribute columns are returned.

        Returns:
            A lazy frame with ``ocel_id``, ``ocel_time``,
            ``ocel_changed_field``, object attribute columns, and
            ``ocel_type``. When ``types`` are provided, attributes that are
            entirely null for those types are omitted.

        Notes:
            This is the source change log. Use :meth:`object_states` when you
            need point-in-time object states with values carried forward.
        """
        if not types:
            return self._object_changes
        return _select_types(
            self._object_changes,
            types,
            (s.OCEL_ID, s.OCEL_TIME, s.OCEL_CHANGED_FIELD),
        )

    def object_states(self, *types: str) -> pl.LazyFrame:
        """Return reconstructed object states from sparse change rows.

        Args:
            *types: Object type names to keep. When omitted, states for all
                object types are returned.

        Returns:
            A lazy frame with one row per object and change timestamp:
            ``ocel_id``, ``ocel_time``, forward-filled object attributes,
            ``ocel_event_id``, ``ocel_event_type``, and ``ocel_type``. Attribute
            values are carried forward per ``(ocel_type, ocel_id)`` from the most
            recent non-null value. The result is sorted by ``ocel_type``,
            ``ocel_id``, and ``ocel_time``.

        Notes:
            OCEL 2.0 objects change only through events, so each change row is
            stamped with the event that caused it (the event linked via E2O at
            the same timestamp): ``ocel_event_id`` / ``ocel_event_type``. The
            synthetic initial-state row at the epoch has no causing event and
            leaves both null; when an object is in several events at one instant
            the smallest ``ocel_event_id`` is chosen. This event-stamped state
            history is the basis for temporal filters and for flattening.

        Examples:
            >>> states = ocel.object_states("order")
            >>> # the order's attributes at the moment it was paid
            >>> paid = states.filter(pl.col("ocel_event_type") == "Pay Order")
            >>> latest = (
            ...     states
            ...     .sort("ocel_type", "ocel_id", "ocel_time")
            ...     .unique(subset=["ocel_type", "ocel_id"], keep="last")
            ...     .collect()
            ... )
        """
        states, attrs = self._forward_filled_states(types)
        enriched = states.join(
            self._causing_events(types),
            left_on=[s.OCEL_ID, s.OCEL_TIME],
            right_on=[s.OCEL_OBJECT_ID, s.OCEL_TIME],
            how="left",
        )
        return enriched.select(
            s.OCEL_ID,
            s.OCEL_TIME,
            *attrs,
            s.OCEL_EVENT_ID,
            s.OCEL_EVENT_TYPE,
            s.OCEL_TYPE,
        ).sort(s.OCEL_TYPE, s.OCEL_ID, s.OCEL_TIME)

    def _forward_filled_states(
        self, types: tuple[str, ...]
    ) -> tuple[pl.LazyFrame, list[str]]:
        """Per-object state history with attributes carried forward.

        Returns the forward-filled state frame (``ocel_type``, ``ocel_id``,
        ``ocel_time``, attributes — one row per object and change timestamp) and
        the list of attribute columns kept for *types*.
        """
        frame = self._object_changes
        core = (s.OCEL_ID, s.OCEL_TIME, s.OCEL_CHANGED_FIELD)
        candidates = _attribute_columns(frame, core)
        if types:
            frame = frame.filter(pl.col(s.OCEL_TYPE).is_in(list(types)))
            attrs = _present(frame, candidates)
        else:
            attrs = candidates
        keys = [s.OCEL_TYPE, s.OCEL_ID, s.OCEL_TIME]
        if not attrs:
            return frame.select(keys).unique(), attrs
        collapsed = frame.group_by(keys).agg(
            pl.col(attr).drop_nulls().last().alias(attr) for attr in attrs
        )
        states = collapsed.with_columns(
            pl.col(attr)
            .forward_fill()
            .over([s.OCEL_TYPE, s.OCEL_ID], order_by=s.OCEL_TIME)
            for attr in attrs
        )
        return states, attrs

    def _causing_events(self, types: tuple[str, ...]) -> pl.LazyFrame:
        """Map each ``(object, timestamp)`` to the event that changed it there.

        OCEL 2.0 objects change only through events, so a change at time ``T``
        for object ``O`` corresponds to the event linked to ``O`` via E2O whose
        timestamp is ``T``. If several events share that instant, the smallest
        ``ocel_event_id`` is kept so the result is deterministic.
        """
        e2o = self._e2o
        if types:
            e2o = e2o.filter(pl.col(s.OCEL_OBJECT_TYPE).is_in(list(types)))
        return (
            e2o.select(s.OCEL_OBJECT_ID, s.OCEL_EVENT_ID, s.OCEL_EVENT_TYPE)
            .join(
                self._events.select(s.OCEL_ID, s.OCEL_TIME),
                left_on=s.OCEL_EVENT_ID,
                right_on=s.OCEL_ID,
                how="inner",
            )
            .sort(s.OCEL_EVENT_ID)
            .unique(
                subset=[s.OCEL_OBJECT_ID, s.OCEL_TIME],
                keep="first",
                maintain_order=True,
            )
        )

    def event_object(self) -> pl.LazyFrame:
        """Return event-to-object relations.

        Returns:
            A lazy frame with ``ocel_event_id``, ``ocel_event_type``,
            ``ocel_object_id``, ``ocel_object_type``, and ``ocel_qualifier``.

        Examples:
            >>> order_links = ocel.event_object().filter(
            ...     pl.col("ocel_object_type") == "order"
            ... )
        """
        return self._e2o

    def object_object(self) -> pl.LazyFrame:
        """Return object-to-object relations.

        Returns:
            A lazy frame with ``ocel_source_id``, ``ocel_source_type``,
            ``ocel_target_id``, ``ocel_target_type``, and ``ocel_qualifier``.
            Logs without O2O data return an empty lazy frame with these columns.
        """
        return self._o2o

    def __rshift__(self, step: Callable[["OCEL"], _T]) -> _T:
        """Apply a step, enabling ``ocel >> step(...)`` pipeline syntax."""
        return step(self)

    def __repr__(self) -> str:
        overview = self._overview()
        return (
            "OCEL\n"
            f"  events:         {_rows(overview.events)} | "
            f"{_types(overview.event_types)}\n"
            f"  objects:        {_rows(overview.objects)} | "
            f"{_types(overview.object_types)}\n"
            f"  object changes: {_rows(overview.object_changes)}\n"
            f"  relations:      E2O: {_rows(overview.e2o)} | "
            f"O2O: {_rows(overview.o2o)}"
        )

    def _repr_html_(self) -> str:
        overview = self._overview()
        counts = "".join(
            f"<tr><th style='text-align:left'>{label}</th>"
            f"<td style='text-align:right'>{value:,}</td></tr>"
            for label, value in (
                ("Events", overview.events),
                ("Objects", overview.objects),
                ("Object changes", overview.object_changes),
                ("E2O relations", overview.e2o),
                ("O2O relations", overview.o2o),
            )
        )
        return (
            "<div style='font-family:sans-serif'>"
            "<strong>OCEL</strong>"
            f"<table>{counts}</table>"
            f"<div><em>Event types:</em> {_chips(overview.event_types)}</div>"
            f"<div><em>Object types:</em> {_chips(overview.object_types)}</div>"
            "</div>"
        )

    def _overview(self) -> _Overview:
        return _Overview(
            events=_count(self._events),
            objects=_count(self._objects),
            object_changes=_count(self._object_changes),
            e2o=_count(self._e2o),
            o2o=_count(self._o2o),
            event_types=_distinct_types(self._events),
            object_types=_distinct_types(self._objects),
        )

    @classmethod
    def read(cls, path: str | Path) -> "OCEL":
        """Open an oceldb Parquet log directory.

        Args:
            path: Directory written by :meth:`write`,
                :func:`oceldb.store.write_frames`, or
                :func:`oceldb.io.sqlite.convert_sqlite`.

        Returns:
            An ``OCEL`` backed by lazy Polars scans of the Parquet files under
            ``path``.

        Notes:
            This method reads oceldb's native directory layout. To open an OCEL
            2.0 SQLite export directly, use
            ``oceldb.io.read_sqlite("log.sqlite")`` or convert it first with
            ``oceldb.io.convert_sqlite``.

        Examples:
            >>> ocel = OCEL.read("converted-log")
            >>> ocel.events("Pay Order").collect()
        """
        frames = read_frames(path)
        return cls(
            events=frames["events"],
            objects=frames["objects"],
            object_changes=frames["object_changes"],
            o2o=frames["o2o"],
            e2o=frames["e2o"],
        )

    def write(self, target: str | Path, *, overwrite: bool = False) -> None:
        """Write this log to oceldb's native Parquet directory layout.

        Args:
            target: Destination directory. The directory is created atomically
                through a temporary sibling directory.
            overwrite: Replace an existing file or directory at ``target`` when
                ``True``. The default raises :class:`FileExistsError` instead.

        Raises:
            FileExistsError: If ``target`` already exists and ``overwrite`` is
                ``False``.

        Notes:
            All lazy frames are collected during the write. The operation first
            writes a complete temporary directory and then renames it into
            place, so writing back to a directory currently being scanned is
            safe once all input scans can still read their original files.
        """
        write_frames(
            {
                "events": self._events,
                "objects": self._objects,
                "object_changes": self._object_changes,
                "e2o": self._e2o,
                "o2o": self._o2o,
            },
            target,
            overwrite=overwrite,
        )

    def sql(self, query: str) -> pl.DataFrame:
        """Run a DuckDB SQL query over the log's tables.

        Opens a temporary in-memory DuckDB connection, registers the five
        logical tables as views, runs *query*, and returns the result as a
        Polars ``DataFrame``. The connection is closed before returning, so it is
        a self-contained query rather than a long-lived session.

        The registered views are the raw tables: ``events``, ``objects``,
        ``object_changes``, ``event_object`` (E2O) and ``object_object`` (O2O).
        DuckDB scans the underlying (parquet-backed) Polars frames directly;
        only the tables the query references are materialized.

        Args:
            query: A DuckDB SQL statement referencing the registered views.

        Returns:
            The query result as an eager ``pl.DataFrame``.

        Examples:
            >>> ocel.sql(
            ...     "SELECT ocel_type, count(*) AS n "
            ...     "FROM events GROUP BY 1 ORDER BY n DESC"
            ... )
            >>> ocel.sql('''
            ...     SELECT e.ocel_type, eo.ocel_object_type, count(*) AS n
            ...     FROM events e
            ...     JOIN event_object eo ON eo.ocel_event_id = e.ocel_id
            ...     GROUP BY 1, 2
            ... ''')
        """
        import duckdb

        connection = duckdb.connect()
        try:
            connection.register("events", self._events)
            connection.register("objects", self._objects)
            connection.register("object_changes", self._object_changes)
            connection.register("event_object", self._e2o)
            connection.register("object_object", self._o2o)
            return connection.sql(query).pl()
        finally:
            connection.close()


def _select_types(
    frame: pl.LazyFrame, types: Sequence[str], core: tuple[str, ...]
) -> pl.LazyFrame:
    sub = frame.filter(pl.col(s.OCEL_TYPE).is_in(list(types)))
    kept = _present(sub, _attribute_columns(frame, core))
    return sub.select(*core, *kept, s.OCEL_TYPE)


def _attribute_columns(frame: pl.LazyFrame, core: tuple[str, ...]) -> list[str]:
    return [
        name
        for name in frame.collect_schema().names()
        if name not in core and name != s.OCEL_TYPE
    ]


def _present(frame: pl.LazyFrame, candidates: list[str]) -> list[str]:
    """Of *candidates*, the columns that hold at least one non-null value."""
    if not candidates:
        return []
    row = frame.select(
        pl.col(name).is_not_null().any().alias(name) for name in candidates
    ).collect()
    return [name for name in candidates if cast(bool, row.get_column(name).item())]


def _count(frame: pl.LazyFrame) -> int:
    return int(cast(int, frame.select(pl.len()).collect().item()))


def _distinct_types(frame: pl.LazyFrame) -> list[str]:
    values: Iterable[object] = (
        frame.select(s.OCEL_TYPE)
        .unique()
        .collect()
        .get_column(s.OCEL_TYPE)
        .drop_nulls()
        .sort()
        .to_list()
    )
    return [str(value) for value in values]


def _rows(count: int) -> str:
    return f"{count:,} row" if count == 1 else f"{count:,} rows"


def _types(values: list[str]) -> str:
    count = len(values)
    label = "type" if count == 1 else "types"
    return f"{count} {label} {_preview(values)}"


def _preview(values: Sequence[str]) -> str:
    if not values:
        return "[]"
    visible = [repr(value) for value in values[:_TYPE_PREVIEW_LIMIT]]
    remaining = len(values) - len(visible)
    if remaining:
        visible.append(f"... +{remaining} more")
    return "[" + ", ".join(visible) + "]"


def _chips(values: list[str]) -> str:
    if not values:
        return "<em>none</em>"
    return ", ".join(html.escape(value) for value in values)
