"""Open the native layout as lazy scans.

Each partitioned table becomes ONE hive scan: ``ocel_type`` comes from the
partition directory names, and the union column schema is assembled from the
Parquet footers up front. The explicit union schema matters — without it the
first file would act as schema authority and other partitions' attribute
columns would be silently dropped. With it, the query optimizer prunes type
predicates down to the matching partition files, and per-type files keep
storing only their own columns.

Opening reads footers and the manifest only; row data stays lazy.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import unquote

import polars as pl

from oceldb.core import schema as s
from oceldb.core.schema import (
    E2O_SCHEMA,
    O2O_SCHEMA,
    EVENT_SCHEMA,
    CHANGE_SCHEMA,
    OBJECT_SCHEMA,
)
from oceldb.ocel import OCEL

TypeAttributes = dict[str, list[str]]


def open_native(path: str | Path) -> OCEL:
    """Open a native oceldb dataset at *path* as an ``OCEL`` of lazy scans.

    Validates the manifest and each table's schema, and derives per-type
    attribute narrowing from the partition Parquet footers, without reading
    row data. See :meth:`oceldb.ocel.OCEL.open`, which calls this.

    Args:
        path: Directory containing a native oceldb dataset.

    Returns:
        The opened ``OCEL``, narrowed per type.

    Raises:
        FileNotFoundError: If *path*, its manifest, or a required table is
            missing.
        ValueError: If the manifest or a table's schema is malformed.
    """
    base = Path(path).expanduser().resolve()
    if not base.is_dir():
        raise FileNotFoundError(f"Native OCEL directory not found: {base}")
    _validate_manifest(base)

    events, event_attributes = _scan_partitioned(base / "events", EVENT_SCHEMA)
    objects, _ = _scan_partitioned(base / "objects", OBJECT_SCHEMA)
    object_changes, change_attributes = _scan_partitioned(
        base / "object_changes", CHANGE_SCHEMA
    )
    return OCEL(
        events=events,
        objects=objects,
        object_changes=object_changes,
        e2o=_scan_relation(base / "e2o.parquet", E2O_SCHEMA),
        o2o=_scan_relation(base / "o2o.parquet", O2O_SCHEMA, optional=True),
        event_attributes=event_attributes,
        change_attributes=change_attributes,
    )


def _validate_manifest(base: Path) -> None:
    """Raise if ``base/manifest.json`` is missing or not a valid v2 manifest."""
    path = base / "manifest.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise FileNotFoundError(f"Native OCEL manifest not found: {path}") from None
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid native OCEL manifest {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError("Native OCEL manifest must be an object")
    if set(value) != {"format", "formatVersion", "createdAt"}:
        raise ValueError("Native OCEL manifest has unexpected fields")
    if value.get("format") != "oceldb" or value.get("formatVersion") != 2:
        raise ValueError("Unsupported native OCEL format or version")
    created_at = value.get("createdAt")
    if not isinstance(created_at, str) or not created_at.endswith("Z"):
        raise ValueError("Native OCEL createdAt must be a UTC timestamp")
    try:
        timestamp = datetime.fromisoformat(created_at)
    except ValueError as exc:
        raise ValueError("Native OCEL createdAt must be a UTC timestamp") from exc
    if timestamp.utcoffset() != timedelta(0):
        raise ValueError("Native OCEL createdAt must be a UTC timestamp")


def _scan_partitioned(
    directory: Path, table_schema: dict[str, pl.DataType]
) -> tuple[pl.LazyFrame, TypeAttributes]:
    """Return one pruning-capable scan plus each type's attribute columns."""
    if not directory.is_dir():
        raise FileNotFoundError(f"Native OCEL table not found: {directory}")
    core = {name: dtype for name, dtype in table_schema.items() if name != s.OCEL_TYPE}

    union: dict[str, pl.DataType] = dict(core)
    attributes: TypeAttributes = {}
    for file in sorted(directory.glob("ocel_type=*/*.parquet")):
        type_name = unquote(file.parent.name.removeprefix("ocel_type="))
        context = f"Native OCEL table {directory.name!r} partition {type_name!r}"
        file_schema = pl.read_parquet_schema(file)
        _validate_partition(file_schema, core, context)
        columns = attributes.setdefault(type_name, [])
        for name, dtype in file_schema.items():
            if name in core:
                continue
            previous = union.get(name)
            if previous is not None and previous != dtype:
                raise ValueError(
                    f"{context} stores attribute {name!r} as {dtype}, but "
                    f"another partition stores it as {previous}. Shared "
                    "attribute columns must use one dtype across types."
                )
            union[name] = dtype
            if name not in columns:
                columns.append(name)

    if not attributes:
        return pl.LazyFrame(schema=table_schema), {}
    frame = pl.scan_parquet(
        directory / "ocel_type=*" / "*.parquet",
        hive_partitioning=True,
        hive_schema={s.OCEL_TYPE: pl.String()},
        schema=union,
        missing_columns="insert",
    )
    return frame, attributes


def _validate_columns(
    file_schema: dict[str, pl.DataType],
    required: dict[str, pl.DataType],
    context: str,
) -> None:
    """Raise if *file_schema* is missing any *required* column or dtype."""
    missing = [name for name in required if name not in file_schema]
    if missing:
        raise ValueError(f"{context} is missing required columns: {missing}")
    for name, dtype in required.items():
        if file_schema[name] != dtype:
            raise ValueError(
                f"{context} stores {name!r} as {file_schema[name]}; expected {dtype}."
            )


def _validate_partition(
    file_schema: dict[str, pl.DataType],
    core: dict[str, pl.DataType],
    context: str,
) -> None:
    """Validate a partition file's columns and reject a stored ``ocel_type``."""
    _validate_columns(file_schema, core, context)
    if s.OCEL_TYPE in file_schema:
        raise ValueError(
            f"{context} must not store {s.OCEL_TYPE!r}; it is the partition key."
        )


def _scan_relation(
    path: Path, table_schema: dict[str, pl.DataType], *, optional: bool = False
) -> pl.LazyFrame:
    """Scan a flat relation file, or return an empty frame if it's optional."""
    if path.is_file():
        file_schema = pl.read_parquet_schema(path)
        _validate_columns(file_schema, table_schema, f"Native OCEL table {path.name!r}")
        return pl.scan_parquet(path)
    if optional:
        return pl.LazyFrame(schema=table_schema)
    raise FileNotFoundError(f"Native OCEL table not found: {path}")
