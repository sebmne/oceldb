"""The user-facing OCEL façade over five lazy Polars tables."""

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, TypeVar
import polars as pl

from oceldb import schema as s
from oceldb.core.dataset import OCELDataset, OCELTables
from oceldb.core.frames import concat_unique, reconstruct_object_states
from oceldb.core.inspection import (
    OCELSummary,
    describe_frames,
    html_repr,
    text_repr,
)
from oceldb.core.presence import TypeDirectory
from oceldb.core.sql import execute_sql
from oceldb.schema._layout import EVENT_CORE, CHANGE_CORE

_T = TypeVar("_T")


class OCEL:
    """An OCEL 2.0 log exposed as lazy Polars dataframes.

    The constructor trusts its input frames and performs no integrity checks.
    Accessors remain lazy. Type-specific accessors return only the attribute
    columns belonging to the selected types: declared columns for logs opened
    or read at an IO boundary, observed non-null columns otherwise.
    """

    __slots__ = ("_dataset", "_summary", "_presence")

    def __init__(
        self,
        dataset: OCELDataset,
        *,
        presence: TypeDirectory | None = None,
    ) -> None:
        """Build the user-facing façade over a canonical dataset.

        ``presence`` seeds the per-type attribute directory with declared IO
        types; without it the directory is probed from the data on demand.
        """
        self._dataset = dataset
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
        """Build a canonical dataset from five explicitly named lazy frames."""
        return cls(
            OCELDataset(
                tables=OCELTables.from_frames(
                    events=events,
                    objects=objects,
                    object_changes=object_changes,
                    event_object=event_object,
                    object_object=object_object,
                ),
                metadata=metadata or {},
            ),
        )

    @property
    def metadata(self) -> Mapping[str, Any]:
        """Read-only native dataset metadata."""
        return self._dataset.metadata

    def events(self, *types: str) -> pl.LazyFrame:
        """Return events, optionally filtered by type.

        With ``types``, the result carries only the attribute columns those
        event types actually have. Native datasets read only the requested
        type partitions.
        """
        if not types:
            return self._dataset.tables.events.all()
        return self._dataset.tables.events.select(
            types,
            EVENT_CORE,
            attributes=self._directory().event_attributes(types),
        )

    def objects(self, *types: str) -> pl.LazyFrame:
        """Return object identities, optionally filtered by type."""
        if not types:
            return self._dataset.tables.objects.all()
        return self._dataset.tables.objects.select(
            types,
            (s.OCEL_ID,),
            attributes=(),
        )

    def object_changes(self, *types: str) -> pl.LazyFrame:
        """Return sparse attribute changes, optionally filtered by object type.

        With ``types``, the result carries only the attribute columns those
        object types actually have. Use :meth:`object_states` for
        forward-filled point-in-time states.
        """
        if not types:
            return self._dataset.tables.object_changes.all()
        return self._dataset.tables.object_changes.select(
            types,
            CHANGE_CORE,
            attributes=self._directory().object_attributes(types),
        )

    def object_states(self, *types: str) -> pl.LazyFrame:
        """Return reconstructed object states from sparse change rows.

        Values are carried forward per object. Each timestamp is enriched with
        the matching E2O event; deterministic ID order resolves simultaneous
        events, while synthetic epoch states have no causing event.
        """
        return reconstruct_object_states(
            self.object_changes(*types),
            self._dataset.tables.events.all(),
            self._dataset.tables.event_object.all(),
            types,
        )

    def event_object(self) -> pl.LazyFrame:
        """Return event-to-object relations."""
        return self._dataset.tables.event_object.all()

    def object_object(self) -> pl.LazyFrame:
        """Return object-to-object relations."""
        return self._dataset.tables.object_object.all()

    def describe(self) -> OCELSummary:
        """Materialize row/type counts and event time bounds.

        This is the programmatic counterpart to the text representation.
        The immutable result is cached because an OCEL's lazy dataset does not
        change after construction.
        """
        if self._summary is None:
            self._summary = describe_frames(
                self._dataset.tables.events.all(),
                self._dataset.tables.objects.all(),
                self._dataset.tables.object_changes.all(),
                self._dataset.tables.event_object.all(),
                self._dataset.tables.object_object.all(),
            )
        return self._summary

    def validate(self) -> None:
        """Validate logical columns, identities, types, and relationships."""
        from oceldb.core.validation import validate_dataset

        validate_dataset(self._dataset)

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
                self.events(),
                self.objects(),
                self.object_changes(),
                self.event_object(),
                self.object_object(),
            ]
        )
        result = OCEL.from_frames(
            events=frames[0].lazy(),
            objects=frames[1].lazy(),
            object_changes=frames[2].lazy(),
            event_object=frames[3].lazy(),
            object_object=frames[4].lazy(),
            metadata=self.metadata,
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
        """Open a native oceldb Parquet directory as lazy scans.

        The manifest, type partitions, and Parquet schemas are validated
        eagerly; row data remains lazy. The manifest's declared types seed the
        attribute directory, so type-specific accessors stay fully lazy.

        Use :func:`oceldb.io.import_ocel` to persist an exchange file; the
        format-specific readers are available for one-off in-memory reads.
        """
        from oceldb.io.native.storage import open_native

        dataset, declared = open_native(path)
        return cls(dataset, presence=TypeDirectory.from_schema(declared))

    @classmethod
    def merge(cls, *ocels: "OCEL") -> "OCEL":
        """Lazily union logs and de-duplicate shared identifiers and rows.

        IDs are assumed to be globally meaningful; conflicting entities with
        the same ID are not detected. At least one log is required.
        """
        if not ocels:
            raise ValueError("merge requires at least one OCEL.")
        return cls.from_frames(
            events=concat_unique(
                [o._dataset.tables.events.all() for o in ocels], subset=[s.OCEL_ID]
            ),
            objects=concat_unique(
                [o._dataset.tables.objects.all() for o in ocels], subset=[s.OCEL_ID]
            ),
            object_changes=concat_unique(
                [o._dataset.tables.object_changes.all() for o in ocels], subset=None
            ),
            object_object=concat_unique(
                [o._dataset.tables.object_object.all() for o in ocels], subset=None
            ),
            event_object=concat_unique(
                [o._dataset.tables.event_object.all() for o in ocels], subset=None
            ),
        )

    def write(self, target: str | Path, *, overwrite: bool = False) -> None:
        """Materialize and transactionally write the native Parquet layout."""
        from oceldb.io.native.storage import write_native

        write_native(
            self._dataset,
            target,
            declared=self._directory().to_schema(),
            overwrite=overwrite,
        )

    def sql(self, query: str) -> pl.DataFrame:
        """Execute DuckDB SQL over the five logical tables.

        Views are named ``events``, ``objects``, ``object_changes``,
        ``event_object``, and ``object_object``. The result is eager.
        """
        return execute_sql(
            query,
            events=self._dataset.tables.events.all(),
            objects=self._dataset.tables.objects.all(),
            object_changes=self._dataset.tables.object_changes.all(),
            e2o=self._dataset.tables.event_object.all(),
            o2o=self._dataset.tables.object_object.all(),
        )

    def _directory(self) -> TypeDirectory:
        """Return the attribute directory, probing and caching if unseeded."""
        if self._presence is None:
            tables = self._dataset.tables
            self._presence = TypeDirectory.probe(
                events=tables.events.all(),
                objects=tables.objects.all(),
                object_changes=tables.object_changes.all(),
            )
        return self._presence
