"""The user-facing OCEL façade over five lazy Polars tables."""

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, TypeVar
import polars as pl

from oceldb import schema as s
from oceldb.core.dataset import OCELDataset, OCELFrames
from oceldb.core.frames import concat_unique, reconstruct_object_states, select_types
from oceldb.core.inspection import (
    OCELSummary,
    describe_frames,
    html_repr,
    text_repr,
)
from oceldb.core.sql import execute_sql
from oceldb.schema import OCELSchema

_T = TypeVar("_T")


class OCEL:
    """An OCEL 2.0 log exposed as lazy Polars dataframes.

    The constructor trusts its input schemas and performs no integrity checks.
    Accessors remain lazy except for the presence query used to omit all-null
    attributes from type-specific event and change views.
    """

    __slots__ = ("_dataset",)

    def __init__(self, dataset: OCELDataset) -> None:
        """Build the user-facing façade over a canonical dataset."""
        self._dataset = dataset

    @classmethod
    def from_frames(
        cls,
        *,
        events: pl.LazyFrame,
        objects: pl.LazyFrame,
        object_changes: pl.LazyFrame,
        event_object: pl.LazyFrame,
        object_object: pl.LazyFrame,
        schema: OCELSchema | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> "OCEL":
        """Build a canonical dataset from five explicitly named lazy frames.

        ``schema`` preserves declared types and all-null attributes during
        exchange; writers infer it from observed values when omitted.
        """
        return cls(
            OCELDataset(
                frames=OCELFrames(
                    events=events,
                    objects=objects,
                    object_changes=object_changes,
                    event_object=event_object,
                    object_object=object_object,
                ),
                schema=schema,
                metadata=metadata or {},
            ),
        )

    @property
    def dataset(self) -> OCELDataset:
        """The canonical typed dataset backing this façade."""
        return self._dataset

    @property
    def schema(self) -> OCELSchema | None:
        """Declared type metadata, or ``None`` when it must be inferred."""
        return self._dataset.schema

    @property
    def metadata(self) -> Mapping[str, Any]:
        """Read-only native dataset metadata."""
        return self._dataset.metadata

    def events(self, *types: str) -> pl.LazyFrame:
        """Return events, optionally filtered by type.

        Type-specific views omit attributes that are entirely null.
        """
        if not types:
            return self._dataset.frames.events
        return select_types(
            self._dataset.frames.events, types, (s.OCEL_ID, s.OCEL_TIME)
        )

    def objects(self, *types: str) -> pl.LazyFrame:
        """Return object identities, optionally filtered by type."""
        if not types:
            return self._dataset.frames.objects
        return self._dataset.frames.objects.filter(
            pl.col(s.OCEL_TYPE).is_in(list(types))
        )

    def object_changes(self, *types: str) -> pl.LazyFrame:
        """Return sparse attribute changes, optionally filtered by object type.

        Use :meth:`object_states` for forward-filled point-in-time states.
        """
        if not types:
            return self._dataset.frames.object_changes
        return select_types(
            self._dataset.frames.object_changes,
            types,
            (s.OCEL_ID, s.OCEL_TIME, s.OCEL_CHANGED_FIELD),
        )

    def object_states(self, *types: str) -> pl.LazyFrame:
        """Return reconstructed object states from sparse change rows.

        Values are carried forward per object. Each timestamp is enriched with
        the matching E2O event; deterministic ID order resolves simultaneous
        events, while synthetic epoch states have no causing event.
        """
        return reconstruct_object_states(
            self._dataset.frames.object_changes,
            self._dataset.frames.events,
            self._dataset.frames.event_object,
            types,
        )

    def event_object(self) -> pl.LazyFrame:
        """Return event-to-object relations."""
        return self._dataset.frames.event_object

    def object_object(self) -> pl.LazyFrame:
        """Return object-to-object relations."""
        return self._dataset.frames.object_object

    def describe(self) -> OCELSummary:
        """Materialize row/type counts and event time bounds.

        This is the programmatic counterpart to the text representation.
        """
        return describe_frames(
            self._dataset.frames.events,
            self._dataset.frames.objects,
            self._dataset.frames.object_changes,
            self._dataset.frames.event_object,
            self._dataset.frames.object_object,
        )

    def __rshift__(self, step: Callable[["OCEL"], _T]) -> _T:
        """Apply a step, enabling ``ocel >> step(...)`` pipeline syntax."""
        return step(self)

    def __repr__(self) -> str:
        return text_repr(
            self._dataset.frames.events,
            self._dataset.frames.objects,
            self._dataset.frames.object_changes,
            self._dataset.frames.event_object,
            self._dataset.frames.object_object,
        )

    def _repr_html_(self) -> str:
        return html_repr(
            self._dataset.frames.events,
            self._dataset.frames.objects,
            self._dataset.frames.object_changes,
            self._dataset.frames.event_object,
            self._dataset.frames.object_object,
        )

    @classmethod
    def open(cls, path: str | Path) -> "OCEL":
        """Open a native oceldb Parquet directory as lazy scans.

        Use :func:`oceldb.io.import_ocel` to persist an exchange file; the
        format-specific readers are available for one-off in-memory reads.
        """
        from oceldb.io.native import open_native

        return cls(open_native(path))

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
                [o.dataset.frames.events for o in ocels], subset=[s.OCEL_ID]
            ),
            objects=concat_unique(
                [o.dataset.frames.objects for o in ocels], subset=[s.OCEL_ID]
            ),
            object_changes=concat_unique(
                [o.dataset.frames.object_changes for o in ocels], subset=None
            ),
            object_object=concat_unique(
                [o.dataset.frames.object_object for o in ocels], subset=None
            ),
            event_object=concat_unique(
                [o.dataset.frames.event_object for o in ocels], subset=None
            ),
            schema=OCELSchema.merge(*(o.schema for o in ocels if o.schema is not None))
            if any(o.schema is not None for o in ocels)
            else None,
        )

    def write(self, target: str | Path, *, overwrite: bool = False) -> None:
        """Materialize and atomically write the native Parquet layout."""
        from oceldb.io.native import write_native

        write_native(self._dataset, target, overwrite=overwrite)

    def sql(self, query: str) -> pl.DataFrame:
        """Execute DuckDB SQL over the five logical tables.

        Views are named ``events``, ``objects``, ``object_changes``,
        ``event_object``, and ``object_object``. The result is eager.
        """
        return execute_sql(
            query,
            events=self._dataset.frames.events,
            objects=self._dataset.frames.objects,
            object_changes=self._dataset.frames.object_changes,
            e2o=self._dataset.frames.event_object,
            o2o=self._dataset.frames.object_object,
        )
