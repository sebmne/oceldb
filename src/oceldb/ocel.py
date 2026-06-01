"""OCEL class - the primary user-facing entry point for an oceldb log."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import datetime
from pathlib import Path
from typing import Self

import ibis
import ibis.backends.duckdb

from oceldb.expr import Table, col, desc, row_number
from oceldb.storage.manifest import Manifest
from oceldb.storage.views import build_views


class ObjectStates:
    """Fill-forward state view for an object type."""

    def __init__(self, table: Table) -> None:
        self._table = table

    def history(self) -> Table:
        """Return all reconstructed state rows."""
        return self._table

    def latest(self) -> Table:
        """Return the most recent state row for each ``ocel_id``."""
        rn = row_number().over(group_by="ocel_id", order_by=desc("ocel_time"))
        return self._table.mutate(_rn=rn).filter(col("_rn") == 0).drop("_rn")

    def as_of(self, t: datetime) -> Table:
        """Return one state row per ``ocel_id`` at or before *t*."""
        rn = row_number().over(group_by="ocel_id", order_by=desc("ocel_time"))
        return (
            self._table.filter(col("ocel_time") <= t)
            .mutate(_rn=rn)
            .filter(col("_rn") == 0)
            .drop("_rn")
        )


class OCEL(AbstractContextManager["OCEL"]):
    """An open, queryable OCEL log."""

    manifest: Manifest
    con: ibis.backends.duckdb.Backend
    path: Path | None

    def __init__(
        self, path: Path | None, con: ibis.backends.duckdb.Backend, manifest: Manifest
    ) -> None:
        self.path = path
        self.con = con
        self.manifest = manifest

    @classmethod
    def read(cls, path: str | Path) -> Self:
        """Open a persisted oceldb Parquet log."""
        path = Path(path)
        manifest = Manifest.load(path / "manifest.json")
        con = ibis.duckdb.connect()
        build_views(con, path, manifest)
        return cls(path, con, manifest)

    @classmethod
    def load(
        cls,
        source: object,
        *,
        format: str | None = None,
        progress: Callable[[str], None] | None = None,
    ) -> Self:
        """Load an OCEL source into ephemeral DuckDB storage."""
        from oceldb.io.resolve import resolve_source
        from oceldb.storage.memory import materialize_memory

        resolved = resolve_source(source, format=format, progress=progress)
        con, manifest = materialize_memory(
            resolved.source,
            source_kind=resolved.kind,
            source_path=resolved.path,
            progress=progress,
        )
        return cls(None, con, manifest)

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self.con.disconnect()

    def __exit__(
        self,
        exc_type: object,
        exc_val: object,
        exc_tb: object,
    ) -> None:
        self.close()

    def _table(self, name: str) -> Table:
        return Table(self.con.table(name))

    def events(self, *types: str) -> Table:
        """Return events, optionally filtered by type."""
        t = self._table("events")
        if types:
            t = t.filter(t["ocel_type"].isin(list(types)))
        return t

    def objects(self, ocel_type: str | None = None) -> Table:
        """Return object identities, optionally filtered by type."""
        t = self._table("objects")
        if ocel_type is not None:
            t = t.filter(t["ocel_type"] == ocel_type)
        return t

    def object_changes(self, ocel_type: str) -> Table:
        """Return raw attribute-change rows for *ocel_type*."""
        return self._table(f"object_changes_{ocel_type}")

    def object_states(self, ocel_type: str) -> ObjectStates:
        """Return reconstructed states for *ocel_type*."""
        return ObjectStates(self._table(f"{ocel_type}_state_history"))

    def flatten(self, ocel_type: str) -> Table:
        """Flatten the OCEL to a case-centric event log for one object type.

        Objects of *ocel_type* become cases. Object attributes are resolved to
        the latest known state at each event timestamp and exposed as
        ``case:<attribute>`` columns. The returned table follows the XES naming
        convention for classical event logs:
        ``case:concept:name`` for the case id, ``concept:name`` for activity,
        and ``time:timestamp`` for the event timestamp.
        """
        event_attrs = [
            c
            for c in self.events().columns
            if c not in {"ocel_id", "ocel_type", "ocel_time"}
        ]
        events = self.events().select(
            col("ocel_id").name("ocel_event_id"),
            col("ocel_type").name("concept:name"),
            col("ocel_time").name("time:timestamp"),
            *event_attrs,
        )
        objects = self.objects(ocel_type).select(
            col("ocel_id").name("case:concept:name"),
            col("ocel_type").name("case:ocel_type"),
        )
        relations = (
            self.event_object.filter(col("ocel_object_type") == ocel_type)
            .select(
                "ocel_event_id",
                col("ocel_object_id").name("case:concept:name"),
            )
            .distinct()
        )
        cases = objects.join(relations, "case:concept:name")
        base = cases.join(events, "ocel_event_id")

        state_attrs = [
            c
            for c in self.object_states(ocel_type).history().columns
            if c not in {"ocel_id", "ocel_time"}
        ]
        states = self.object_states(ocel_type).history().select(
            col("ocel_id").name("_oceldb_case_id"),
            col("ocel_time").name("_oceldb_state_time"),
            *(col(c).name(f"case:{c}") for c in state_attrs),
        )
        joined = base.join(
            states,
            [
                base["case:concept:name"] == states["_oceldb_case_id"],
                states["_oceldb_state_time"] <= base["time:timestamp"],
            ],
            how="left",
        )
        rn = row_number().over(
            group_by=["ocel_event_id", "case:concept:name"],
            order_by=desc("_oceldb_state_time"),
        )

        return (
            joined.mutate(_oceldb_state_rn=rn)
            .filter(col("_oceldb_state_rn") == 0)
            .select(
                "ocel_event_id",
                "case:concept:name",
                "case:ocel_type",
                *(f"case:{c}" for c in state_attrs),
                "concept:name",
                "time:timestamp",
                *event_attrs,
            )
            .order_by("case:concept:name", "time:timestamp", "ocel_event_id")
        )

    @property
    def event_object(self) -> Table:
        """Return event-to-object relations."""
        return self._table("event_object")

    @property
    def object_object(self) -> Table:
        """Return object-to-object relations."""
        return self._table("object_object")
