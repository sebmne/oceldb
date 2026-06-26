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
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import cast

import polars as pl

from oceldb import schema as s
from oceldb.store import read_frames, write_frames


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
            A lazy frame with one row per object and change timestamp. Attribute
            values are forward-filled per ``(ocel_type, ocel_id)`` from the most
            recent non-null value. The result is sorted by ``ocel_type``,
            ``ocel_id``, and ``ocel_time``.

        Notes:
            OCEL stores object attributes as sparse changes. This method turns
            those changes into a state history that is convenient for temporal
            analysis, joins, and "latest state" queries.

        Examples:
            >>> states = ocel.object_states("order")
            >>> latest = (
            ...     states
            ...     .sort("ocel_type", "ocel_id", "ocel_time")
            ...     .unique(subset=["ocel_type", "ocel_id"], keep="last")
            ...     .collect()
            ... )
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
            return (
                frame.select(keys)
                .unique()
                .select(s.OCEL_ID, s.OCEL_TIME, s.OCEL_TYPE)
                .sort(s.OCEL_TYPE, s.OCEL_ID, s.OCEL_TIME)
            )

        collapsed = frame.group_by(keys).agg(
            pl.col(attr).drop_nulls().last().alias(attr) for attr in attrs
        )
        filled = collapsed.with_columns(
            pl.col(attr)
            .forward_fill()
            .over([s.OCEL_TYPE, s.OCEL_ID], order_by=s.OCEL_TIME)
            for attr in attrs
        )
        return filled.select(s.OCEL_ID, s.OCEL_TIME, *attrs, s.OCEL_TYPE).sort(
            s.OCEL_TYPE, s.OCEL_ID, s.OCEL_TIME
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

    def __repr__(self) -> str:
        events, objects, e2o, o2o, event_types, object_types = self._overview()
        return (
            f"OCEL(events={events:,}, objects={objects:,}, "
            f"e2o={e2o:,}, o2o={o2o:,}, "
            f"event_types={len(event_types)}, object_types={len(object_types)})"
        )

    def _repr_html_(self) -> str:
        events, objects, e2o, o2o, event_types, object_types = self._overview()
        counts = "".join(
            f"<tr><th style='text-align:left'>{label}</th>"
            f"<td style='text-align:right'>{value:,}</td></tr>"
            for label, value in (
                ("Events", events),
                ("Objects", objects),
                ("E2O relations", e2o),
                ("O2O relations", o2o),
            )
        )
        return (
            "<div style='font-family:sans-serif'>"
            "<strong>OCEL</strong>"
            f"<table>{counts}</table>"
            f"<div><em>Event types:</em> {_chips(event_types)}</div>"
            f"<div><em>Object types:</em> {_chips(object_types)}</div>"
            "</div>"
        )

    def _overview(self) -> tuple[int, int, int, int, list[str], list[str]]:
        return (
            _count(self._events),
            _count(self._objects),
            _count(self._e2o),
            _count(self._o2o),
            _distinct_types(self._events),
            _distinct_types(self._objects),
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
            2.0 SQLite export directly, use ``oceldb.read_sqlite("log.sqlite")``
            or convert it first with ``oceldb.io.convert_sqlite``.

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


def _chips(values: list[str]) -> str:
    if not values:
        return "<em>none</em>"
    return ", ".join(html.escape(value) for value in values)
