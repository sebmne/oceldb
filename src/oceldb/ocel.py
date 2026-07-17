from collections.abc import Callable
from pathlib import Path
from typing import TypeVar
import polars as pl

from oceldb.core import schema as s
from oceldb.core.sql import execute_sql

_T = TypeVar("_T")

_EVENT_CORE = (s.OCEL_ID, s.OCEL_TIME)
_CHANGE_CORE = (s.OCEL_ID, s.OCEL_TIME, s.OCEL_CHANGED_FIELD, s.OCEL_IS_INITIAL)


class OCEL:
    """An OCEL 2.0 log as five lazy Polars tables.

    The constructor trusts its input frames and performs no integrity checks.
    On a log opened from native storage, type-filtered accessors return only
    the selected types' attribute columns and the query optimizer prunes the
    scan to the matching partition files; on a manually constructed log they
    are plain type filters over the given frames. Row-filtering operations in
    ``oceldb.operations`` preserve this narrowing across a ``>>`` pipeline;
    only an operation that changes a type's actual column set needs to update
    it explicitly.
    """

    __slots__ = (
        "_events",
        "_objects",
        "_object_changes",
        "_e2o",
        "_o2o",
        "_event_attributes",
        "_change_attributes",
    )

    def __init__(
        self,
        *,
        events: pl.LazyFrame,
        objects: pl.LazyFrame,
        object_changes: pl.LazyFrame,
        e2o: pl.LazyFrame,
        o2o: pl.LazyFrame,
        event_attributes: dict[str, list[str]] | None = None,
        change_attributes: dict[str, list[str]] | None = None,
    ) -> None:
        """Wrap five lazy frames as an OCEL log, trusting them as given.

        Args:
            events: One row per event; must carry ``ocel_id``, ``ocel_time``,
                and ``ocel_type``, plus any event attribute columns.
            objects: One row per object identity; must carry ``ocel_id`` and
                ``ocel_type``.
            object_changes: One row per recorded object state; must carry
                ``ocel_id``, ``ocel_time``, ``ocel_changed_field``,
                ``ocel_is_initial``, and ``ocel_type``, plus any object
                attribute columns. See ``docs/storage-format.md`` for the
                ``ocel_is_initial`` contract.
            e2o: The event-to-object relation table.
            o2o: The object-to-object relation table.
            event_attributes: Which attribute columns each event type
                actually has, keyed by type name. When set, typed accessors
                narrow to just those columns instead of the full union
                schema. ``None`` disables narrowing. Normally supplied by
                :meth:`open`, not passed by hand.
            change_attributes: The same narrowing map for
                ``object_changes``, keyed by object type.
        """
        self._events = events
        self._objects = objects
        self._object_changes = object_changes
        self._e2o = e2o
        self._o2o = o2o
        # Which attribute columns each type actually has. ``open()`` derives
        # this from the partition Parquet footers; operations that only
        # filter rows (not columns) carry their input's dicts forward
        # unchanged so narrowing survives a pipeline instead of resetting to
        # "unnarrowed" on every step. ``None`` means "narrowing unknown" —
        # typed accessors then fall back to plain type filters.
        self._event_attributes = event_attributes
        self._change_attributes = change_attributes

    def events(self, *types: str) -> pl.LazyFrame:
        """Return events, optionally narrowed to the given event types.

        Args:
            *types: Event type names to keep. Omit to get every type.

        Returns:
            A lazy frame of ``ocel_id``, ``ocel_time``, ``ocel_type``, and
            attribute columns. When narrowing is available (see
            :meth:`open`) and *types* is given, only the selected types'
            attribute columns are included — not the full union schema.
        """
        return self._typed(self._events, types, _EVENT_CORE, self._event_attributes)

    def objects(self, *types: str) -> pl.LazyFrame:
        """Return object identities, optionally narrowed to given types.

        Args:
            *types: Object type names to keep. Omit to get every type.

        Returns:
            A lazy frame of ``ocel_id`` and ``ocel_type``. Objects carry no
            attributes of their own — see :meth:`object_changes` for their
            state history.
        """
        return self._typed(self._objects, types, (s.OCEL_ID,), None)

    def object_changes(self, *types: str) -> pl.LazyFrame:
        """Return recorded object states, optionally narrowed to given types.

        Args:
            *types: Object type names to keep. Omit to get every type.

        Returns:
            A lazy frame of ``ocel_id``, ``ocel_time``, ``ocel_changed_field``,
            ``ocel_is_initial``, ``ocel_type``, and attribute columns — one
            row per recorded state, not a snapshot. See
            ``docs/storage-format.md`` for how to reconstruct a point-in-time
            state from this table. When narrowing is available (see
            :meth:`open`) and *types* is given, only the selected types'
            attribute columns are included.
        """
        return self._typed(
            self._object_changes, types, _CHANGE_CORE, self._change_attributes
        )

    def e2o(self) -> pl.LazyFrame:
        """Return the full event-to-object relation table."""
        return self._e2o

    def o2o(self) -> pl.LazyFrame:
        """Return the full object-to-object relation table."""
        return self._o2o

    def sql(self, query: str) -> pl.DataFrame:
        """Run a DuckDB SQL query over the log's five tables.

        Registers ``events``, ``objects``, ``object_changes``, ``e2o``, and
        ``o2o`` as DuckDB views of the *unnarrowed* union-schema frames (the
        same shape :meth:`events` and :meth:`object_changes` return when
        called with no type arguments), then executes eagerly.

        Args:
            query: A SQL query referencing the five table names above.

        Returns:
            The query result as an eager ``polars.DataFrame``.

        Examples:
            >>> ocel.sql("SELECT count(*) FROM events WHERE ocel_type = 'Pay Order'")
        """
        return execute_sql(
            query,
            events=self._events,
            objects=self._objects,
            object_changes=self._object_changes,
            e2o=self._e2o,
            o2o=self._o2o,
        )

    @classmethod
    def open(cls, path: str | Path) -> "OCEL":
        """Open a native oceldb dataset as lazy scans.

        Reads the manifest and each partition's Parquet footer to validate
        the layout and derive per-type attribute narrowing; row data stays
        lazy until a returned frame is collected. See
        ``docs/storage-format.md`` for the on-disk layout.

        Args:
            path: Directory containing a native oceldb dataset (a
                ``manifest.json`` plus the ``events``/``objects``/
                ``object_changes`` partitions and relation files).

        Returns:
            The opened ``OCEL``, with typed accessors narrowed to each
            type's real attribute columns.

        Raises:
            FileNotFoundError: If *path*, its manifest, or a required table
                is missing.
            ValueError: If the manifest or a table's schema is malformed.
        """
        from oceldb.core.open import open_native

        return open_native(path)

    def write(self, target: str | Path, *, overwrite: bool = False) -> None:
        """Write this log as a native oceldb dataset.

        Stages the complete directory next to *target* and renames it into
        place, so a failed write never leaves a half-written dataset. See
        ``docs/storage-format.md`` for the on-disk layout this produces.

        Args:
            target: Directory to write the dataset to.
            overwrite: If ``True``, replace an existing dataset at *target*.
                If ``False`` (default), raise instead.

        Raises:
            FileExistsError: If *target* already exists and *overwrite* is
                ``False``.
            ValueError: If a table is missing a required column.
        """
        from oceldb.core.write import write_native

        write_native(
            target,
            events=self._events,
            objects=self._objects,
            object_changes=self._object_changes,
            e2o=self._e2o,
            o2o=self._o2o,
            overwrite=overwrite,
        )

    def __rshift__(self, step: Callable[["OCEL"], _T]) -> _T:
        """Apply *step* to this log: ``ocel >> step`` is ``step(ocel)``.

        Enables pipeline syntax with any ``Callable[[OCEL], T]``, including
        every function in ``oceldb.operations`` called without its first
        argument (see ``oceldb.operations.step``).

        Args:
            step: A callable taking this ``OCEL`` and returning anything —
                typically another ``OCEL`` or a ``polars.LazyFrame``.

        Returns:
            Whatever *step* returns.

        Examples:
            >>> ocel >> filter_events_by_type("Place Order") >> flatten("order")
        """
        return step(self)

    @staticmethod
    def _typed(
        frame: pl.LazyFrame,
        types: tuple[str, ...],
        core: tuple[str, ...],
        attributes: dict[str, list[str]] | None,
    ) -> pl.LazyFrame:
        """Filter *frame* to *types*, narrowing columns when possible.

        With no *types*, returns *frame* unchanged. With *types* but no
        *attributes* map, applies a plain type filter (the union schema is
        unaffected). With both, also drops attribute columns none of the
        selected types has values for.
        """
        if not types:
            return frame
        selected = frame.filter(pl.col(s.OCEL_TYPE).is_in(types))
        if attributes is None:
            return selected
        kept = list(
            dict.fromkeys(
                name for type_name in types for name in attributes.get(type_name, ())
            )
        )
        return selected.select(*core, *kept, s.OCEL_TYPE)
