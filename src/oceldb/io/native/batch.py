"""Bounded-memory writer for staged native OCEL imports."""

from dataclasses import dataclass, field
from pathlib import Path
from collections.abc import Iterable, Mapping
from typing import Any

import duckdb
import polars as pl
import pyarrow.parquet as pq

from oceldb import schema as s
from oceldb.io._paths import DirectoryTransaction
from oceldb.io._schema import validate_frames_for_io
from oceldb.io.errors import ValidationMode
from oceldb.io.native.layout import (
    COMPRESSION,
    EVENTS,
    EVENT_OBJECT,
    OBJECTS,
    OBJECT_CHANGES,
    OBJECT_OBJECT,
    NativeTable,
)
from oceldb.io.native.manifest import write_manifest
from oceldb.io.native.storage import scan_native_tables
from oceldb.schema import OCELSchema, TypeAttributes, widen_shared_attributes


@dataclass
class _BufferedWriter:
    path: Path
    output: Path
    schema: Mapping[str, pl.DataType]
    sort_by: tuple[str, ...]
    batch_size: int
    rows: list[dict[str, Any]] = field(default_factory=list)
    writer: Any = None

    def __post_init__(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        arrow_schema = pl.DataFrame(schema=self.schema).to_arrow().schema
        self.writer = pq.ParquetWriter(self.path, arrow_schema, compression=COMPRESSION)

    def add(self, rows: Iterable[dict[str, Any]]) -> None:
        for row in rows:
            self.rows.append(row)
            if len(self.rows) >= self.batch_size:
                self.flush()

    def flush(self) -> None:
        if not self.rows:
            return
        frame = pl.from_dicts(
            self.rows,
            schema=self.schema,
            strict=False,
            infer_schema_length=None,
        )
        self.writer.write_table(frame.to_arrow())
        self.rows.clear()

    def close(self) -> None:
        self.flush()
        self.writer.close()


class NativeBatchSink:
    """Write normalized row batches and transactionally commit a native dataset."""

    def __init__(
        self,
        target: str | Path,
        schema: OCELSchema,
        *,
        overwrite: bool,
        batch_size: int = 10_000,
        validation: ValidationMode = "strict",
    ) -> None:
        self._destination = DirectoryTransaction(Path(target), overwrite=overwrite)
        self.target = self._destination.target
        # The union scan over the partitioned layout requires one dtype per
        # shared attribute column, so conflicting declarations are widened.
        self.schema = widen_shared_attributes(schema)
        self.batch_size = batch_size
        self.validation: ValidationMode = validation
        self.staging = self._destination.staging
        self._writers: dict[str, _BufferedWriter] = {}

    def __enter__(self) -> "NativeBatchSink":
        self._destination.__enter__()
        try:
            for table in (EVENTS, OBJECTS, OBJECT_CHANGES):
                table.root(self.staging).mkdir()
            for type_name, attributes in self.schema.event_types.items():
                self._writer(
                    f"event:{type_name}",
                    EVENTS,
                    type_name=type_name,
                    attributes=attributes,
                )
            for type_name, attributes in self.schema.object_types.items():
                self._writer(
                    f"object:{type_name}",
                    OBJECTS,
                    type_name=type_name,
                )
                self._writer(
                    f"change:{type_name}",
                    OBJECT_CHANGES,
                    type_name=type_name,
                    attributes=attributes,
                )
            self._writer("e2o", EVENT_OBJECT)
        except BaseException:
            self.abort()
            raise
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if exc_type is not None:
            self.abort()
            return
        try:
            self.commit()
        except BaseException:
            self.abort()
            raise

    def add_events(self, rows: Iterable[dict[str, Any]]) -> None:
        self._add_typed("event", rows)

    def add_objects(self, rows: Iterable[dict[str, Any]]) -> None:
        self._add_typed("object", rows)

    def add_object_changes(self, rows: Iterable[dict[str, Any]]) -> None:
        self._add_typed("change", rows)

    def add_e2o(self, rows: Iterable[dict[str, Any]]) -> None:
        self._writers["e2o"].add(rows)

    def add_o2o(self, rows: Iterable[dict[str, Any]]) -> None:
        materialized = list(rows)
        if not materialized:
            return
        writer = self._writers.get("o2o")
        if writer is None:
            writer = self._writer("o2o", OBJECT_OBJECT)
        writer.add(materialized)

    def commit(self) -> None:
        for writer in self._writers.values():
            writer.close()
        con = duckdb.connect()
        try:
            for writer in self._writers.values():
                order = ", ".join(_quote(name) for name in writer.sort_by)
                con.execute(
                    f"COPY (SELECT * FROM read_parquet("
                    f"{_sql_string(str(writer.path))}, hive_partitioning = false) "
                    f"ORDER BY {order}) TO {_sql_string(str(writer.output))} "
                    f"(FORMAT PARQUET, COMPRESSION {COMPRESSION.upper()})"
                )
                writer.path.unlink()
        finally:
            con.close()
        validate_frames_for_io(
            scan_native_tables(self.staging, self.schema),
            self.validation,
        )
        write_manifest(self.staging, schema=self.schema)
        self._destination.commit()

    def abort(self) -> None:
        for writer in self._writers.values():
            try:
                writer.writer.close()
            except BaseException:
                pass
        self._destination.abort()

    def _add_typed(self, prefix: str, rows: Iterable[dict[str, Any]]) -> None:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            type_name = str(row[s.OCEL_TYPE])
            physical = {
                name: value for name, value in row.items() if name != s.OCEL_TYPE
            }
            grouped.setdefault(type_name, []).append(physical)
        for type_name, typed_rows in grouped.items():
            self._writers[f"{prefix}:{type_name}"].add(typed_rows)

    def _writer(
        self,
        key: str,
        table: NativeTable,
        *,
        type_name: str | None = None,
        attributes: TypeAttributes | None = None,
    ) -> _BufferedWriter:
        writer = _BufferedWriter(
            path=table.file(self.staging, type_name=type_name, unsorted=True),
            output=table.file(self.staging, type_name=type_name),
            schema=table.schema(attributes),
            sort_by=table.sort_by,
            batch_size=self.batch_size,
        )
        self._writers[key] = writer
        return writer


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
