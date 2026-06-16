"""OCEL class - the primary user-facing entry point for an oceldb log."""

from collections.abc import Mapping
from contextlib import AbstractContextManager
from datetime import datetime
from pathlib import Path
from typing import Any, Self

import ibis
import ibis.backends.duckdb

from oceldb.case_centric.types import CaseCentricEventLog
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
        rn = row_number().over(
            group_by=["ocel_type", "ocel_id"], order_by=desc("ocel_time")
        )
        return self._table.mutate(_rn=rn).filter(col("_rn") == 0).drop("_rn")

    def as_of(self, t: datetime) -> Table:
        """Return one state row per ``ocel_id`` at or before *t*."""
        rn = row_number().over(
            group_by=["ocel_type", "ocel_id"], order_by=desc("ocel_time")
        )
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

    @staticmethod
    def _union_attributes(
        type_infos: Mapping[str, Any], types: tuple[str, ...]
    ) -> list[str]:
        seen: set[str] = set()
        attrs: list[str] = []
        for name in types:
            info = type_infos.get(name)
            if info is None:
                continue
            for attr in info.attributes:
                if attr not in seen:
                    seen.add(attr)
                    attrs.append(attr)
        return attrs

    def events(self, *types: str) -> Table:
        """Return events, optionally filtered by type."""
        t = self._table("events")
        if types:
            attrs = self._union_attributes(self.manifest.event_types, types)
            t = t.filter(t["ocel_type"].isin(list(types))).select(
                "ocel_id", "ocel_time", *attrs, "ocel_type"
            )
        return t

    def objects(self, *types: str) -> Table:
        """Return object identities, optionally filtered by type."""
        t = self._table("objects")
        if types:
            t = t.filter(t["ocel_type"].isin(list(types)))
        return t

    def object_changes(self, *types: str) -> Table:
        """Return raw attribute-change rows, optionally filtered by type."""
        t = self._table("object_changes")
        if types:
            attrs = self._union_attributes(self.manifest.object_types, types)
            t = t.filter(t["ocel_type"].isin(list(types))).select(
                "ocel_id", "ocel_time", *attrs, "ocel_type"
            )
        return t

    def object_states(self, *types: str) -> ObjectStates:
        """Return reconstructed object states, optionally filtered by type."""
        t = self._table("object_states")
        if types:
            attrs = self._union_attributes(self.manifest.object_types, types)
            t = t.filter(t["ocel_type"].isin(list(types))).select(
                "ocel_id", "ocel_time", *attrs, "ocel_type"
            )
        return ObjectStates(t)

    def flatten(self, ocel_type: str) -> CaseCentricEventLog:
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
            if c not in {"ocel_id", "ocel_type", "ocel_time"}
        ]
        states = (
            self.object_states(ocel_type)
            .history()
            .select(
                col("ocel_id").name("_oceldb_case_id"),
                col("ocel_time").name("_oceldb_state_time"),
                *(col(c).name(f"case:{c}") for c in state_attrs),
            )
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


def ocel(path: str | Path) -> OCEL:
    """Open a persisted oceldb log.

    This is a short alias for ``OCEL.read(path)``, intended for interactive
    use in notebooks and scripts:

    ``log = oceldb.ocel("my-log")``

    The returned ``OCEL`` owns a DuckDB connection. Close it explicitly with
    ``log.close()`` or use it as a context manager when possible.
    """
    return OCEL.read(path)
