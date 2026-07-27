"""Bounded Parquet staging shared by exchange-format converters."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
import shutil
from typing import Any
from urllib.parse import quote

import polars as pl

from oceldb import OCEL
from oceldb.core import schema as s
from oceldb.core.write import (
    _ROWS_PER_GROUP,
    _write_manifest,
    _write_relation_index,
    install_staged_native,
)
from oceldb.io._schema import ExchangeSchema

_TYPED_ROWS_PER_FILE = 50_000


class TableSink:
    """Buffer normalized rows up to a fixed limit and stage Parquet parts."""

    def __init__(
        self,
        root: Path,
        schema: ExchangeSchema,
        *,
        batch_size: int,
    ) -> None:
        if isinstance(batch_size, bool) or batch_size < 1:
            raise ValueError("batch_size must be a positive integer.")
        self.root = root
        self.batch_size = batch_size
        self._schemas: dict[str, Mapping[str, pl.DataType]] = {
            "events": schema.event_schema,
            "objects": s.OBJECT_SCHEMA,
            "object_changes": schema.change_schema,
            "e2o": s.E2O_SCHEMA,
            "e2o_by_object": s.E2O_SCHEMA,
            "o2o": s.O2O_SCHEMA,
        }
        self._buffers: dict[str, list[dict[str, Any]]] = {
            name: [] for name in self._schemas
        }
        self._parts = {name: 0 for name in self._schemas}
        self._sorted_tables: set[str] = set()
        self.root.mkdir(parents=True)

    def add(self, table: str, row: dict[str, Any]) -> None:
        """Add one normalized row."""
        buffer = self._buffers[table]
        buffer.append(row)
        if len(buffer) >= self.batch_size:
            self._flush(table)

    def extend(self, table: str, rows: Iterable[dict[str, Any]]) -> None:
        """Add normalized rows from an iterable."""
        for row in rows:
            self.add(table, row)

    def add_frame(self, table: str, frame: pl.DataFrame) -> None:
        """Stage an already-normalized frame without creating Python rows."""
        self._flush(table)
        schema = self._schemas[table]
        missing = [name for name in schema if name not in frame.columns]
        if missing:
            frame = frame.with_columns(
                pl.lit(None, dtype=schema[name]).alias(name) for name in missing
            )
        normalized = frame.select(
            pl.col(name).cast(dtype, strict=True) for name, dtype in schema.items()
        )
        for offset in range(0, normalized.height, self.batch_size):
            self._write_frame(table, normalized.slice(offset, self.batch_size))

    def add_sorted_frame(self, table: str, frame: pl.DataFrame) -> None:
        """Stage a frame whose batches follow the table's physical sort order."""
        self._sorted_tables.add(table)
        self.add_frame(table, frame)

    def finish(
        self,
        target: Path,
        *,
        overwrite: bool,
        validate: bool,
    ) -> OCEL:
        """Flush staged rows, write a native snapshot, and open it."""
        for table in ("events", "objects", "object_changes", "e2o", "o2o"):
            self._flush(table, write_empty=True)
        if self._parts["e2o_by_object"] or self._buffers["e2o_by_object"]:
            self._flush("e2o_by_object")
        native = self.root.parent / "native"
        native.mkdir()
        self._partition_table(
            "events",
            native,
            s.EVENT_SCHEMA,
            sort_by=(s.OCEL_TIME,),
        )
        self._partition_table(
            "objects",
            native,
            s.OBJECT_SCHEMA,
            sort_by=(s.OCEL_ID,),
        )
        self._partition_table(
            "object_changes",
            native,
            s.CHANGE_SCHEMA,
            sort_by=(s.OCEL_ID, s.OCEL_IS_INITIAL, s.OCEL_TIME),
            descending=(False, True, False),
        )
        self._finalize_relations(native)
        _write_manifest(native)
        staged = OCEL.open(native)
        if validate:
            staged.validate()
        install_staged_native(native, target, overwrite=overwrite)
        return OCEL.open(target)

    def _flush(self, table: str, *, write_empty: bool = False) -> None:
        rows = self._buffers[table]
        if not rows and (not write_empty or self._parts[table]):
            return
        directory = self.root / table
        directory.mkdir(exist_ok=True)
        frame = pl.from_dicts(
            rows,
            schema=self._schemas[table],
            strict=True,
            infer_schema_length=None,
        )
        self._write_frame(table, frame)
        rows.clear()

    def _write_frame(self, table: str, frame: pl.DataFrame) -> None:
        directory = self.root / table
        directory.mkdir(exist_ok=True)
        frame.write_parquet(
            directory / f"part-{self._parts[table]:05d}.parquet",
            compression="zstd" if table in {"e2o", "o2o"} else "lz4",
            statistics=True,
        )
        self._parts[table] += 1

    def _partition_table(
        self,
        table: str,
        native: Path,
        core_schema: Mapping[str, pl.DataType],
        *,
        sort_by: tuple[str, ...],
        descending: tuple[bool, ...] | None = None,
    ) -> None:
        output = native / table
        output.mkdir()
        counters: dict[str, int] = {}
        attributes = [name for name in self._schemas[table] if name not in core_schema]
        stored_core = [name for name in core_schema if name != s.OCEL_TYPE]
        for part in sorted((self.root / table).glob("*.parquet")):
            frame = pl.read_parquet(part)
            if frame.is_empty():
                continue
            for key, typed in frame.partition_by(
                s.OCEL_TYPE,
                as_dict=True,
                maintain_order=False,
            ).items():
                type_name = key[0]
                assert isinstance(type_name, str)
                present = [
                    name
                    for name in attributes
                    if (
                        typed.get_column(name).null_count() < typed.height
                        or (
                            s.OCEL_CHANGED_FIELD in core_schema
                            and typed.get_column(s.OCEL_CHANGED_FIELD)
                            .eq(name)
                            .fill_null(False)
                            .any()
                        )
                    )
                ]
                partition = output / f"ocel_type={quote(type_name, safe='')}"
                partition.mkdir(exist_ok=True)
                number = counters.get(type_name, 0)
                typed.select(*stored_core, *present).sort(
                    *sort_by,
                    descending=descending or False,
                ).write_parquet(
                    partition / f"part-{number:05d}.parquet",
                    compression="lz4",
                    statistics=True,
                )
                counters[type_name] = number + 1
        for partition in sorted(output.glob("ocel_type=*")):
            files = sorted(partition.glob("*.parquet"))
            if not files:
                continue
            file_columns = {
                name for file in files for name in pl.read_parquet_schema(file)
            }
            final_schema = {
                name: dtype
                for name, dtype in self._schemas[table].items()
                if name != s.OCEL_TYPE and name in file_columns
            }
            compact = output / f".compact-{partition.name}"
            compact.mkdir()
            group_size = max(1, _TYPED_ROWS_PER_FILE // self.batch_size)
            for number, offset in enumerate(range(0, len(files), group_size)):
                group = files[offset : offset + group_size]
                source = pl.scan_parquet(
                    group,
                    schema=final_schema,
                    missing_columns="insert",
                )
                source.sort(
                    *sort_by,
                    descending=descending or False,
                ).sink_parquet(
                    compact / f"part-{number:05d}.parquet",
                    compression="zstd",
                    row_group_size=_ROWS_PER_GROUP,
                    maintain_order=True,
                )
            shutil.rmtree(partition)
            compact.rename(partition)

    def _finalize_relations(self, native: Path) -> None:
        e2o = pl.scan_parquet(sorted((self.root / "e2o").glob("*.parquet")))
        _write_relation_index(
            e2o,
            native / "e2o",
            s.E2O_SCHEMA,
            sort_by=(s.OCEL_EVENT_ID, s.OCEL_OBJECT_ID, s.OCEL_QUALIFIER),
            input_sorted="e2o" in self._sorted_tables,
        )
        if self._parts["e2o_by_object"]:
            e2o_by_object = pl.scan_parquet(
                sorted((self.root / "e2o_by_object").glob("*.parquet"))
            )
            _write_relation_index(
                e2o_by_object,
                native / "indexes" / "e2o_by_object",
                s.E2O_SCHEMA,
                sort_by=(s.OCEL_OBJECT_ID, s.OCEL_EVENT_ID, s.OCEL_QUALIFIER),
                input_sorted="e2o_by_object" in self._sorted_tables,
            )
        else:
            _write_relation_index(
                e2o,
                native / "indexes" / "e2o_by_object",
                s.E2O_SCHEMA,
                sort_by=(s.OCEL_OBJECT_ID, s.OCEL_EVENT_ID, s.OCEL_QUALIFIER),
            )
        o2o = pl.scan_parquet(sorted((self.root / "o2o").glob("*.parquet")))
        _write_relation_index(
            o2o,
            native / "o2o",
            s.O2O_SCHEMA,
            sort_by=(s.OCEL_SOURCE_ID, s.OCEL_TARGET_ID, s.OCEL_QUALIFIER),
            input_sorted="o2o" in self._sorted_tables,
        )
