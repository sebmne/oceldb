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

    def event_occurrences(self, ocel_type: str) -> Table:
        """Return event occurrences for objects of *ocel_type*."""
        eo = self.event_object.filter(col("ocel_object_type") == ocel_type)
        ev = self._table("events").select(
            col("ocel_id").name("ocel_event_id"),
            col("ocel_time").name("ocel_event_time"),
        )
        return eo.join(ev, "ocel_event_id").select(
            "ocel_event_id",
            "ocel_event_type",
            "ocel_event_time",
            "ocel_object_id",
            "ocel_qualifier",
        )

    @property
    def event_object(self) -> Table:
        """Return event-to-object relations."""
        return self._table("event_object")

    @property
    def object_object(self) -> Table:
        """Return object-to-object relations."""
        return self._table("object_object")
