"""Disk-backed endpoint-type resolution for nested JSON/XML relations."""

from __future__ import annotations

from pathlib import Path
import sqlite3

import polars as pl

from oceldb.core import schema as s
from oceldb.io._errors import OCELConversionError
from oceldb.io._sink import TableSink


class RelationIndex:
    """Resolve relation endpoint types without an in-memory identity map."""

    def __init__(self, path: Path, *, batch_size: int) -> None:
        self.batch_size = batch_size
        self.connection = sqlite3.connect(path)
        self.connection.executescript(
            """
            PRAGMA journal_mode = OFF;
            PRAGMA synchronous = OFF;
            PRAGMA temp_store = FILE;
            CREATE TABLE object_type (
                ocel_id TEXT PRIMARY KEY,
                ocel_type TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE raw_e2o (
                event_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                object_id TEXT NOT NULL,
                qualifier TEXT NOT NULL
            );
            CREATE TABLE raw_o2o (
                source_id TEXT NOT NULL,
                source_type TEXT NOT NULL,
                target_id TEXT NOT NULL,
                qualifier TEXT NOT NULL
            );
            """
        )
        self._objects: list[tuple[str, str]] = []
        self._e2o: list[tuple[str, str, str, str]] = []
        self._o2o: list[tuple[str, str, str, str]] = []

    def add_object(self, identifier: str, type_name: str) -> None:
        """Register an object identity."""
        self._objects.append((identifier, type_name))
        if len(self._objects) >= self.batch_size:
            self._flush_objects()

    def add_e2o(
        self,
        event_id: str,
        event_type: str,
        object_id: str,
        qualifier: str,
    ) -> None:
        """Stage one event-to-object relation."""
        self._e2o.append((event_id, event_type, object_id, qualifier))
        if len(self._e2o) >= self.batch_size:
            self._flush_e2o()

    def add_o2o(
        self,
        source_id: str,
        source_type: str,
        target_id: str,
        qualifier: str,
    ) -> None:
        """Stage one object-to-object relation."""
        self._o2o.append((source_id, source_type, target_id, qualifier))
        if len(self._o2o) >= self.batch_size:
            self._flush_o2o()

    def resolve_into(self, sink: TableSink) -> None:
        """Resolve target types in SQLite and stream canonical relations."""
        self._flush_objects()
        self._flush_e2o()
        self._flush_o2o()
        self.connection.commit()
        self._resolve_e2o(
            sink,
            table="e2o",
            order_by="r.event_id, r.object_id, r.qualifier",
        )
        self._resolve_e2o(
            sink,
            table="e2o_by_object",
            order_by="r.object_id, r.event_id, r.qualifier",
        )
        self._resolve_o2o(sink)

    def close(self) -> None:
        """Close the temporary relation index."""
        self.connection.close()

    def _resolve_e2o(
        self,
        sink: TableSink,
        *,
        table: str,
        order_by: str,
    ) -> None:
        cursor = self.connection.execute(
            f"""
            SELECT r.event_id, r.event_type, r.object_id, o.ocel_type, r.qualifier
            FROM raw_e2o AS r
            LEFT JOIN object_type AS o ON r.object_id = o.ocel_id
            ORDER BY {order_by}
            """
        )
        while rows := cursor.fetchmany(self.batch_size):
            sink.add_sorted_frame(
                table,
                pl.from_dicts(
                    [
                        {
                            s.OCEL_EVENT_ID: row[0],
                            s.OCEL_EVENT_TYPE: row[1],
                            s.OCEL_OBJECT_ID: row[2],
                            s.OCEL_OBJECT_TYPE: row[3],
                            s.OCEL_QUALIFIER: row[4],
                        }
                        for row in rows
                    ],
                    schema=s.E2O_SCHEMA,
                    strict=True,
                ),
            )

    def _resolve_o2o(self, sink: TableSink) -> None:
        cursor = self.connection.execute(
            """
            SELECT r.source_id, r.source_type, r.target_id, o.ocel_type, r.qualifier
            FROM raw_o2o AS r
            LEFT JOIN object_type AS o ON r.target_id = o.ocel_id
            ORDER BY r.source_id, r.target_id, r.qualifier
            """
        )
        while rows := cursor.fetchmany(self.batch_size):
            sink.add_sorted_frame(
                "o2o",
                pl.from_dicts(
                    [
                        {
                            s.OCEL_SOURCE_ID: row[0],
                            s.OCEL_SOURCE_TYPE: row[1],
                            s.OCEL_TARGET_ID: row[2],
                            s.OCEL_TARGET_TYPE: row[3],
                            s.OCEL_QUALIFIER: row[4],
                        }
                        for row in rows
                    ],
                    schema=s.O2O_SCHEMA,
                    strict=True,
                ),
            )

    def _flush_objects(self) -> None:
        if not self._objects:
            return
        try:
            self.connection.executemany(
                "INSERT INTO object_type VALUES (?, ?)",
                self._objects,
            )
        except sqlite3.IntegrityError as exc:
            raise OCELConversionError(
                "OCEL exchange document contains a duplicate object id."
            ) from exc
        self._objects.clear()

    def _flush_e2o(self) -> None:
        if not self._e2o:
            return
        self.connection.executemany(
            "INSERT INTO raw_e2o VALUES (?, ?, ?, ?)",
            self._e2o,
        )
        self._e2o.clear()

    def _flush_o2o(self) -> None:
        if not self._o2o:
            return
        self.connection.executemany(
            "INSERT INTO raw_o2o VALUES (?, ?, ?, ?)",
            self._o2o,
        )
        self._o2o.clear()
