"""The user-facing OCEL façade over five lazy Polars tables."""

from collections.abc import Callable, Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Any, TypeVar
import polars as pl

from oceldb import schema as s
from oceldb.core.frames import concat_unique, reconstruct_object_states
from oceldb.core.inspection import (
    OCELSummary,
    describe_frames,
    html_repr,
    text_repr,
)
from oceldb.core.presence import TypeDirectory
from oceldb.core.sql import execute_sql
from oceldb.schema._layout import CHANGE_CORE, EVENT_CORE

_T = TypeVar("_T")

_TABLES = (
    "events",
    "objects",
    "object_changes",
    "event_object",
    "object_object",
)


class OCEL:
    """An OCEL 2.0 log exposed as five lazy Polars dataframes.

    The constructor trusts its input frames and performs no integrity checks.
    Accessors remain lazy. Type-specific accessors return only the attribute
    columns belonging to the selected types: declared columns for logs opened
    or read at an IO boundary, observed non-null columns otherwise. For
    native datasets the query optimizer prunes type predicates down to the
    hive partition files.
    """

    __slots__ = (
        "_events",
        "_objects",
        "_object_changes",
        "_event_object",
        "_object_object",
        "_metadata",
        "_source",
        "_summary",
        "_presence",
    )

    def __init__(
        self,
        *,
        events: pl.LazyFrame,
        objects: pl.LazyFrame,
        object_changes: pl.LazyFrame,
        event_object: pl.LazyFrame,
        object_object: pl.LazyFrame,
        metadata: Mapping[str, Any] | None = None,
        presence: TypeDirectory | None = None,
        source: Path | None = None,
    ) -> None:
        """Wrap five lazy frames.

        ``presence`` seeds the per-type attribute directory with declared IO
        types; without it the directory is probed from the data on demand.
        ``source`` marks a pristine native dataset whose files can be copied
        verbatim on write; it is set only by :meth:`open`.
        """
        self._events = events
        self._objects = objects
        self._object_changes = object_changes
        self._event_object = event_object
        self._object_object = object_object
        self._metadata: Mapping[str, Any] = MappingProxyType(dict(metadata or {}))
        self._source = source
        self._summary: OCELSummary | None = None
        self._presence = presence

    @classmethod
    def from_frames(
        cls,
        *,
        events: pl.LazyFrame,
        objects: pl.LazyFrame,
        object_changes: pl.LazyFrame,
        event_object: pl.LazyFrame,
        object_object: pl.LazyFrame,
        metadata: Mapping[str, Any] | None = None,
    ) -> "OCEL":
        """Build an OCEL from five explicitly named lazy frames."""
        return cls(
            events=events,
            objects=objects,
            object_changes=object_changes,
            event_object=event_object,
            object_object=object_object,
            metadata=metadata,
        )

    @property
    def metadata(self) -> Mapping[str, Any]:
        """Read-only native dataset metadata."""
        return self._metadata

    def events(self, *types: str) -> pl.LazyFrame:
        """Return events, optionally filtered by type.

        With ``types``, the result carries only the attribute columns those
        event types actually have.
        """
        if not types:
            return self._events
        return self._select_typed(
            self._events,
            types,
            EVENT_CORE,
            self._directory().event_attributes(types),
        )

    def objects(self, *types: str) -> pl.LazyFrame:
        """Return object identities, optionally filtered by type."""
        if not types:
            return self._objects
        return self._select_typed(self._objects, types, (s.OCEL_ID,), [])

    def object_changes(self, *types: str) -> pl.LazyFrame:
        """Return sparse attribute changes, optionally filtered by object type.

        With ``types``, the result carries only the attribute columns those
        object types actually have. Use :meth:`object_states` for
        forward-filled point-in-time states.
        """
        if not types:
            return self._object_changes
        return self._select_typed(
            self._object_changes,
            types,
            CHANGE_CORE,
            self._directory().object_attributes(types),
        )

    def object_states(self, *types: str) -> pl.LazyFrame:
        """Return reconstructed object states from sparse change rows.

        Values are carried forward per object. Each timestamp is enriched with
        the matching E2O event; deterministic ID order resolves simultaneous
        events, while synthetic epoch states have no causing event.
        """
        return reconstruct_object_states(
            self.object_changes(*types),
            self._events,
            self._event_object,
            types,
        )

    def event_object(self) -> pl.LazyFrame:
        """Return event-to-object relations."""
        return self._event_object

    def object_object(self) -> pl.LazyFrame:
        """Return object-to-object relations."""
        return self._object_object

    def describe(self) -> OCELSummary:
        """Materialize row/type counts and event time bounds.

        This is the programmatic counterpart to the text representation.
        The immutable result is cached because an OCEL's lazy frames do not
        change after construction.
        """
        if self._summary is None:
            self._summary = describe_frames(
                self._events,
                self._objects,
                self._object_changes,
                self._event_object,
                self._object_object,
            )
        return self._summary

    def validate(self) -> None:
        """Validate logical columns, identities, types, and relationships."""
        from oceldb.core.validation import validate_tables

        validate_tables(
            events=self._events,
            objects=self._objects,
            object_changes=self._object_changes,
            event_object=self._event_object,
            object_object=self._object_object,
        )

    def materialize(self) -> "OCEL":
        """Execute all five lazy tables once and return an in-memory OCEL.

        This is useful after constructing a sub-OCEL that will feed several
        independent operations. Merely assigning a filtered OCEL to a variable
        retains its lazy plans, so every later collection evaluates those plans
        again. Materialization pays that cost once while preserving the same
        lazy Polars accessor API on the returned OCEL.
        """
        frames = pl.collect_all(
            [
                self._events,
                self._objects,
                self._object_changes,
                self._event_object,
                self._object_object,
            ]
        )
        result = self._replace(
            events=frames[0].lazy(),
            objects=frames[1].lazy(),
            object_changes=frames[2].lazy(),
            event_object=frames[3].lazy(),
            object_object=frames[4].lazy(),
        )
        result._summary = self._summary
        result._presence = self._presence
        return result

    def __rshift__(self, step: Callable[["OCEL"], _T]) -> _T:
        """Apply a step, enabling ``ocel >> step(...)`` pipeline syntax."""
        return step(self)

    def __repr__(self) -> str:
        return text_repr(self.describe())

    def _repr_html_(self) -> str:
        return html_repr(self.describe())

    @classmethod
    def open(cls, path: str | Path) -> "OCEL":
        """Open a native oceldb Parquet directory as lazy hive scans.

        The manifest, type partitions, and Parquet schemas are validated
        eagerly; row data remains lazy. The manifest's declared types seed the
        attribute directory, so type-specific accessors stay fully lazy, and
        type predicates prune down to the matching partition files.

        Use :func:`oceldb.io.import_ocel` to persist an exchange file; the
        format-specific readers are available for one-off in-memory reads.
        """
        from oceldb.io.native.storage import open_native

        return open_native(path)

    @classmethod
    def merge(cls, *ocels: "OCEL") -> "OCEL":
        """Lazily union logs and de-duplicate shared identifiers and rows.

        IDs are assumed to be globally meaningful; conflicting entities with
        the same ID are not detected. At least one log is required.
        """
        if not ocels:
            raise ValueError("merge requires at least one OCEL.")
        return cls.from_frames(
            events=concat_unique([o._events for o in ocels], subset=[s.OCEL_ID]),
            objects=concat_unique([o._objects for o in ocels], subset=[s.OCEL_ID]),
            object_changes=concat_unique(
                [o._object_changes for o in ocels], subset=None
            ),
            object_object=concat_unique([o._object_object for o in ocels], subset=None),
            event_object=concat_unique([o._event_object for o in ocels], subset=None),
        )

    def write(self, target: str | Path, *, overwrite: bool = False) -> None:
        """Materialize and transactionally write the native Parquet layout.

        A pristine opened dataset is copied file-by-file without executing
        any query plans.
        """
        from oceldb.io.native.storage import write_native

        write_native(self, target, overwrite=overwrite)

    def sql(self, query: str) -> pl.DataFrame:
        """Execute DuckDB SQL over the five logical tables.

        Views are named ``events``, ``objects``, ``object_changes``,
        ``event_object``, and ``object_object``. The result is eager.
        """
        return execute_sql(
            query,
            events=self._events,
            objects=self._objects,
            object_changes=self._object_changes,
            e2o=self._event_object,
            o2o=self._object_object,
        )

    def _replace(self, **frames: pl.LazyFrame) -> "OCEL":
        """Return a new OCEL with the supplied tables replaced.

        The result keeps the metadata but is no longer backed by a pristine
        native source, and its attribute directory is recomputed on demand.
        """
        current: dict[str, pl.LazyFrame] = {
            name: getattr(self, f"_{name}") for name in _TABLES
        }
        unknown = set(frames) - set(current)
        if unknown:
            raise TypeError(f"Unknown OCEL tables: {sorted(unknown)}")
        current.update(frames)
        return OCEL(
            events=current["events"],
            objects=current["objects"],
            object_changes=current["object_changes"],
            event_object=current["event_object"],
            object_object=current["object_object"],
            metadata=self._metadata,
        )

    def _select_typed(
        self,
        frame: pl.LazyFrame,
        types: tuple[str, ...],
        core: tuple[str, ...],
        attributes: list[str],
    ) -> pl.LazyFrame:
        """Filter by type and keep only the selected types' columns."""
        selected = frame.filter(pl.col(s.OCEL_TYPE).is_in(list(types)).fill_null(False))
        available = set(frame.collect_schema().names())
        kept = [name for name in attributes if name in available]
        return selected.select(*core, *kept, s.OCEL_TYPE)

    def _directory(self) -> TypeDirectory:
        """Return the attribute directory, probing and caching if unseeded."""
        if self._presence is None:
            self._presence = TypeDirectory.probe(
                events=self._events,
                objects=self._objects,
                object_changes=self._object_changes,
            )
        return self._presence
