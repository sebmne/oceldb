"""Typed containers for the canonical lazy OCEL dataset."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import MappingProxyType
from typing import Any

import polars as pl

from oceldb import schema as s


@dataclass(frozen=True)
class OCELTable:
    """One logical table together with its useful physical representation.

    ``frame`` is the complete fallback plan. Native typed tables additionally
    retain their per-type plans, and unchanged native tables retain the exact
    path that can be copied when writing. Keeping this state together prevents
    transformations from having to synchronize three parallel data models.
    """

    frame: pl.LazyFrame
    partitions: Mapping[str, pl.LazyFrame] | None = None
    source: Path | None = None

    def __post_init__(self) -> None:
        if self.partitions is not None:
            object.__setattr__(
                self,
                "partitions",
                MappingProxyType(dict(self.partitions)),
            )

    def all(self) -> pl.LazyFrame:
        """Return the complete logical table."""
        return self.frame

    def select(
        self,
        types: Sequence[str],
        core: tuple[str, ...],
        *,
        attributes: Sequence[str] | None = None,
    ) -> pl.LazyFrame:
        """Select types, using physical partitions when they are available."""
        if self.partitions is not None:
            selected = [
                self.partitions[name]
                for name in dict.fromkeys(types)
                if name in self.partitions
            ]
            if selected:
                return pl.concat(selected, how="diagonal_relaxed")
            return self.frame.filter(pl.lit(False))

        selected = self.frame.filter(pl.col(s.OCEL_TYPE).is_in(list(types)))
        if attributes is None:
            return selected
        available = set(self.frame.collect_schema().names())
        kept = [name for name in attributes if name in available]
        return selected.select(*core, *kept, s.OCEL_TYPE)

    def map_partitions(
        self,
        operation: Callable[[pl.LazyFrame], pl.LazyFrame],
        *,
        names: Mapping[str, str] | None = None,
    ) -> "OCELTable":
        """Apply an operation without collapsing known type partitions.

        The operation must distribute over row-wise concatenation, as filters,
        semi-joins, and column expressions do. Use :meth:`replace` for a global
        operation whose result depends on seeing every partition together.

        ``names`` optionally remaps partition keys. Multiple old keys mapping
        to the same new key are unioned into one physical partition plan.
        """
        if self.partitions is None:
            return OCELTable(operation(self.frame))
        grouped: dict[str, list[pl.LazyFrame]] = {}
        for name, frame in self.partitions.items():
            target = names.get(name, name) if names is not None else name
            grouped.setdefault(target, []).append(operation(frame))
        mapped = {
            name: frames[0]
            if len(frames) == 1
            else pl.concat(frames, how="diagonal_relaxed")
            for name, frames in grouped.items()
        }
        # Keep the complete plan independent from the per-type plans. Building
        # it by concatenating ``mapped`` copies cross-table joins into every
        # partition branch; chained sub-log filters then multiply that work.
        # Applying the distributive operation once keeps ``all()`` compact,
        # while ``select()`` can still prune to the mapped physical partitions.
        combined = operation(self.frame)
        return OCELTable(combined, partitions=mapped)

    def filter_types(
        self,
        types: Sequence[str],
        *,
        include: bool,
    ) -> "OCELTable":
        """Keep or remove complete type partitions when their lineage is known."""
        requested = set(types)
        if self.partitions is None:
            matches = pl.col(s.OCEL_TYPE).is_in(list(requested)).fill_null(False)
            return OCELTable(self.frame.filter(matches if include else ~matches))

        selected = {
            name: frame
            for name, frame in self.partitions.items()
            if (name in requested) is include
        }
        if len(selected) == len(self.partitions):
            return self
        combined = (
            pl.concat(list(selected.values()), how="diagonal_relaxed")
            if selected
            else self.frame.filter(pl.lit(False))
        )
        return OCELTable(combined, partitions=selected)

    def replace(self, frame: pl.LazyFrame) -> "OCELTable":
        """Replace the logical plan and discard now-invalid physical state."""
        return OCELTable(frame)


@dataclass(frozen=True)
class OCELTables:
    """The five canonical OCEL tables."""

    events: OCELTable
    objects: OCELTable
    object_changes: OCELTable
    event_object: OCELTable
    object_object: OCELTable

    @classmethod
    def from_frames(
        cls,
        *,
        events: pl.LazyFrame,
        objects: pl.LazyFrame,
        object_changes: pl.LazyFrame,
        event_object: pl.LazyFrame,
        object_object: pl.LazyFrame,
    ) -> "OCELTables":
        """Wrap five standalone lazy frames as canonical tables."""
        return cls(
            events=OCELTable(events),
            objects=OCELTable(objects),
            object_changes=OCELTable(object_changes),
            event_object=OCELTable(event_object),
            object_object=OCELTable(object_object),
        )


@dataclass(frozen=True)
class OCELDataset:
    """Canonical tables plus native dataset metadata."""

    tables: OCELTables
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    def with_tables(
        self,
        *,
        events: OCELTable | None = None,
        objects: OCELTable | None = None,
        object_changes: OCELTable | None = None,
        event_object: OCELTable | None = None,
        object_object: OCELTable | None = None,
    ) -> "OCELDataset":
        """Return a dataset with supplied tables and their lineage replaced."""
        current = self.tables
        return replace(
            self,
            tables=OCELTables(
                events=current.events if events is None else events,
                objects=current.objects if objects is None else objects,
                object_changes=current.object_changes
                if object_changes is None
                else object_changes,
                event_object=current.event_object
                if event_object is None
                else event_object,
                object_object=current.object_object
                if object_object is None
                else object_object,
            ),
        )
