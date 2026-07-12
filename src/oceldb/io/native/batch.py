"""Bounded-memory writer for staged native OCEL imports."""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4
from collections.abc import Iterable, Mapping
from typing import Any

import duckdb
import polars as pl
import pyarrow.parquet as pq

from oceldb import schema as s
from oceldb.io.native.manifest import write_manifest
from oceldb.io.native.storage import encode_type_name
from oceldb.schema import OCELSchema, TypeAttributes
from oceldb.schema._layout import E2O_SCHEMA, O2O_SCHEMA

_COMPRESSION = "zstd"


@dataclass
class _BufferedWriter:
    path: Path
    schema: Mapping[str, pl.DataType]
    sort_by: tuple[str, ...]
    batch_size: int
    rows: list[dict[str, Any]] = field(default_factory=list)
    writer: Any = None

    def __post_init__(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        arrow_schema = pl.DataFrame(schema=self.schema).to_arrow().schema
        self.writer = pq.ParquetWriter(
            self.path, arrow_schema, compression=_COMPRESSION
        )

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
    """Write normalized row batches and atomically commit a native dataset."""

    def __init__(
        self,
        target: str | Path,
        schema: OCELSchema,
        *,
        overwrite: bool,
        batch_size: int = 10_000,
    ) -> None:
        self.target = Path(target)
        self.schema = schema
        self.overwrite = overwrite
        self.batch_size = batch_size
        self.staging = self.target.with_name(f"{self.target.name}.tmp-{uuid4().hex}")
        self._writers: dict[str, _BufferedWriter] = {}

    def __enter__(self) -> "NativeBatchSink":
        if self.target.exists() and not self.overwrite:
            raise FileExistsError(
                f"Target already exists: {self.target}. Pass overwrite=True to replace it."
            )
        self.staging.mkdir(parents=True)
        for type_name, attributes in self.schema.event_types.items():
            self._writer(
                f"event:{type_name}",
                self.staging
                / "events"
                / f"ocel_type={encode_type_name(type_name)}"
                / "data.unsorted.parquet",
                _event_schema(attributes),
                (s.OCEL_TIME,),
            )
        for type_name, attributes in self.schema.object_types.items():
            partition = f"ocel_type={encode_type_name(type_name)}"
            self._writer(
                f"object:{type_name}",
                self.staging / "objects" / partition / "data.unsorted.parquet",
                {s.OCEL_ID: pl.String()},
                (s.OCEL_ID,),
            )
            self._writer(
                f"change:{type_name}",
                self.staging / "object_changes" / partition / "data.unsorted.parquet",
                _change_schema(attributes),
                (s.OCEL_ID, s.OCEL_TIME),
            )
        self._writer(
            "e2o",
            self.staging / "event_object.unsorted.parquet",
            E2O_SCHEMA,
            (s.OCEL_OBJECT_ID, s.OCEL_EVENT_ID),
        )
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
            writer = self._writer(
                "o2o",
                self.staging / "object_object.unsorted.parquet",
                O2O_SCHEMA,
                (s.OCEL_SOURCE_ID, s.OCEL_TARGET_ID),
            )
        writer.add(materialized)

    def commit(self) -> None:
        for writer in self._writers.values():
            writer.close()
        con = duckdb.connect()
        try:
            for writer in self._writers.values():
                output = writer.path.with_name(
                    writer.path.name.replace(".unsorted", "")
                )
                order = ", ".join(_quote(name) for name in writer.sort_by)
                con.execute(
                    f"COPY (SELECT * FROM read_parquet({_sql_string(str(writer.path))}) "
                    f"ORDER BY {order}) TO {_sql_string(str(output))} "
                    "(FORMAT PARQUET, COMPRESSION ZSTD)"
                )
                writer.path.unlink()
        finally:
            con.close()
        write_manifest(self.staging, schema=self.schema)
        if self.target.is_dir():
            shutil.rmtree(self.target)
        elif self.target.exists():
            self.target.unlink()
        self.staging.rename(self.target)

    def abort(self) -> None:
        for writer in self._writers.values():
            try:
                writer.writer.close()
            except BaseException:
                pass
        shutil.rmtree(self.staging, ignore_errors=True)

    def _add_typed(self, prefix: str, rows: Iterable[dict[str, Any]]) -> None:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            type_name = str(row.pop(s.OCEL_TYPE))
            grouped.setdefault(type_name, []).append(row)
        for type_name, typed_rows in grouped.items():
            self._writers[f"{prefix}:{type_name}"].add(typed_rows)

    def _writer(
        self,
        key: str,
        path: Path,
        schema: Mapping[str, pl.DataType],
        sort_by: tuple[str, ...],
    ) -> _BufferedWriter:
        writer = _BufferedWriter(path, schema, sort_by, self.batch_size)
        self._writers[key] = writer
        return writer


def _event_schema(attributes: TypeAttributes) -> dict[str, pl.DataType]:
    return {
        s.OCEL_ID: pl.String(),
        s.OCEL_TIME: pl.Datetime("us", "UTC"),
        **{name: attr_type.polars_dtype() for name, attr_type in attributes.items()},
    }


def _change_schema(attributes: TypeAttributes) -> dict[str, pl.DataType]:
    return {
        s.OCEL_ID: pl.String(),
        s.OCEL_TIME: pl.Datetime("us", "UTC"),
        s.OCEL_CHANGED_FIELD: pl.String(),
        **{name: attr_type.polars_dtype() for name, attr_type in attributes.items()},
    }


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
