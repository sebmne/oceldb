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
from urllib.parse import quote, unquote

import polars as pl
from polars.exceptions import PolarsError

from oceldb.core import schema as s
from oceldb.core.schema import (
    E2O_SCHEMA,
    O2O_SCHEMA,
    EVENT_SCHEMA,
    CHANGE_SCHEMA,
    OBJECT_SCHEMA,
)
from oceldb.errors import OCELFormatError
from oceldb.ocel import OCEL
from oceldb.types import PathLikeStr

TypeAttributes = dict[str, list[str]]
TypeNames = tuple[str, ...]


def open_native(path: PathLikeStr) -> OCEL:
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
        OCELFormatError: If the manifest or a table's schema is malformed.
    """
    base = Path(path).expanduser().resolve()
    if not base.is_dir():
        raise FileNotFoundError(f"Native OCEL directory not found: {base}")
    format_version, indexes, relations = _validate_manifest(base)

    events, event_attributes, event_types = _scan_partitioned(
        base / "events", EVENT_SCHEMA
    )
    objects, _, object_types = _scan_partitioned(base / "objects", OBJECT_SCHEMA)
    object_changes, change_attributes, _ = _scan_partitioned(
        base / "object_changes", CHANGE_SCHEMA
    )
    e2o = _scan_relation_dataset(
        base / "e2o",
        E2O_SCHEMA,
        sorted_by=(s.OCEL_EVENT_ID if "e2o" in relations else None),
    )
    o2o = _scan_relation_dataset(
        base / "o2o",
        O2O_SCHEMA,
        sorted_by=(s.OCEL_SOURCE_ID if "o2o" in relations else None),
    )
    e2o_by_object = None
    if "e2oByObject" in indexes:
        e2o_by_object = _scan_relation_dataset(
            base / "indexes" / "e2o_by_object",
            E2O_SCHEMA,
            sorted_by=s.OCEL_OBJECT_ID,
        )
    return OCEL(
        events=events,
        objects=objects,
        object_changes=object_changes,
        e2o=e2o,
        _e2o_by_object=e2o_by_object,
        _e2o_by_event_sorted="e2o" in relations,
        o2o=o2o,
        event_attributes=event_attributes,
        change_attributes=change_attributes,
        _event_types=event_types,
        _object_types=object_types,
        _source=base,
        _format_version=format_version,
    )


def _validate_manifest(
    base: Path,
) -> tuple[int, dict[str, object], dict[str, object]]:
    """Validate the manifest and return its version and known indexes."""
    path = base / "manifest.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise FileNotFoundError(f"Native OCEL manifest not found: {path}") from None
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise OCELFormatError(f"Invalid native OCEL manifest {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise OCELFormatError("Native OCEL manifest must be an object")
    required = {"format", "formatVersion", "createdAt"}
    if missing := sorted(required - set(value)):
        raise OCELFormatError(f"Native OCEL manifest is missing fields: {missing}")
    version = value.get("formatVersion")
    if value.get("format") != "oceldb" or version != 2:
        raise OCELFormatError("Unsupported native OCEL format or version")
    created_at = value.get("createdAt")
    if not isinstance(created_at, str) or not created_at.endswith("Z"):
        raise OCELFormatError("Native OCEL createdAt must be a UTC timestamp")
    try:
        timestamp = datetime.fromisoformat(created_at)
    except ValueError as exc:
        raise OCELFormatError("Native OCEL createdAt must be a UTC timestamp") from exc
    if timestamp.utcoffset() != timedelta(0):
        raise OCELFormatError("Native OCEL createdAt must be a UTC timestamp")
    indexes = value.get("indexes", {})
    if not isinstance(indexes, dict):
        raise OCELFormatError("Native OCEL indexes must be an object")
    e2o_by_object = indexes.get("e2oByObject")
    if e2o_by_object is not None and e2o_by_object != {
        "path": "indexes/e2o_by_object",
        "sort": [
            s.OCEL_OBJECT_ID,
            s.OCEL_EVENT_ID,
            s.OCEL_QUALIFIER,
        ],
    }:
        raise OCELFormatError("Native OCEL e2oByObject index declaration is invalid")
    relations = value.get("relations", {})
    expected_relations = {
        "e2o": {
            "sort": [
                s.OCEL_EVENT_ID,
                s.OCEL_OBJECT_ID,
                s.OCEL_QUALIFIER,
            ]
        },
        "o2o": {
            "sort": [
                s.OCEL_SOURCE_ID,
                s.OCEL_TARGET_ID,
                s.OCEL_QUALIFIER,
            ]
        },
    }
    if not isinstance(relations, dict) or any(
        declaration != expected_relations[name]
        for name, declaration in relations.items()
        if name in expected_relations
    ):
        raise OCELFormatError("Native OCEL relation ordering declaration is invalid")
    unknown_relations = sorted(set(relations) - set(expected_relations))
    if unknown_relations:
        raise OCELFormatError(
            f"Native OCEL declares unknown relation metadata: {unknown_relations}"
        )
    assert isinstance(version, int)
    return version, indexes, relations


def _scan_partitioned(
    directory: Path, table_schema: dict[str, pl.DataType]
) -> tuple[pl.LazyFrame, TypeAttributes, TypeNames]:
    """Return a pruning-capable scan, attribute map, and partition type names."""
    if not directory.is_dir():
        raise FileNotFoundError(f"Native OCEL table not found: {directory}")
    core = {name: dtype for name, dtype in table_schema.items() if name != s.OCEL_TYPE}

    union: dict[str, pl.DataType] = dict(core)
    attributes: TypeAttributes = {}
    type_names: list[str] = []
    partitions = sorted(
        entry
        for entry in directory.iterdir()
        if entry.is_dir() and entry.name.startswith("ocel_type=")
    )
    unexpected_entries = sorted(
        entry.name
        for entry in directory.iterdir()
        if not (entry.is_dir() and entry.name.startswith("ocel_type="))
    )
    if unexpected_entries:
        raise OCELFormatError(
            f"Native OCEL table {directory.name!r} has unexpected entries: "
            f"{unexpected_entries}"
        )
    for partition in partitions:
        encoded_type = partition.name.removeprefix("ocel_type=")
        type_name = unquote(encoded_type)
        if not type_name or quote(type_name, safe="") != encoded_type:
            raise OCELFormatError(
                f"Native OCEL table {directory.name!r} has a non-canonical "
                f"type partition: {partition.name!r}"
            )
        type_names.append(type_name)
        files = sorted(partition.glob("*.parquet"))
        if not files:
            raise OCELFormatError(
                f"Native OCEL type partition contains no Parquet files: {partition}"
            )
        for file in files:
            context = f"Native OCEL table {directory.name!r} partition {type_name!r}"
            file_schema = _read_parquet_schema(file, context)
            _validate_partition(file_schema, core, context)
            columns = attributes.setdefault(type_name, [])
            for name, dtype in file_schema.items():
                if name in core:
                    continue
                previous = union.get(name)
                if previous is not None and previous != dtype:
                    raise OCELFormatError(
                        f"{context} stores attribute {name!r} as {dtype}, but "
                        f"another partition stores it as {previous}. Shared "
                        "attribute columns must use one dtype across types."
                    )
                union[name] = dtype
                if name not in columns:
                    columns.append(name)

    if not type_names:
        return pl.LazyFrame(schema=table_schema), {}, ()
    frame = pl.scan_parquet(
        directory / "ocel_type=*" / "*.parquet",
        hive_partitioning=True,
        hive_schema={s.OCEL_TYPE: pl.String()},
        schema=union,
        missing_columns="insert",
    )
    return frame, attributes, tuple(sorted(type_names))


def _validate_columns(
    file_schema: dict[str, pl.DataType],
    required: dict[str, pl.DataType],
    context: str,
) -> None:
    """Raise if *file_schema* is missing any *required* column or dtype."""
    missing = [name for name in required if name not in file_schema]
    if missing:
        raise OCELFormatError(f"{context} is missing required columns: {missing}")
    for name, dtype in required.items():
        if file_schema[name] != dtype:
            raise OCELFormatError(
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
        raise OCELFormatError(
            f"{context} must not store {s.OCEL_TYPE!r}; it is the partition key."
        )
    reserved_attributes = sorted(
        (set(file_schema) - set(core)) & (s.RESERVED_COLUMNS - {s.OCEL_TYPE})
    )
    if reserved_attributes:
        raise OCELFormatError(
            f"{context} uses reserved attribute columns: {reserved_attributes}"
        )


def _scan_relation_dataset(
    directory: Path,
    table_schema: dict[str, pl.DataType],
    *,
    sorted_by: str | None,
) -> pl.LazyFrame:
    """Scan a relation dataset containing one or more Parquet parts."""
    if not directory.is_dir():
        raise FileNotFoundError(f"Native OCEL table not found: {directory}")
    files = sorted(directory.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(
            f"Native OCEL relation dataset contains no Parquet files: {directory}"
        )
    for file in files:
        context = f"Native OCEL table {directory.name!r} part {file.name!r}"
        file_schema = _read_parquet_schema(file, context)
        _validate_columns(file_schema, table_schema, context)
        unexpected = sorted(set(file_schema) - set(table_schema))
        if unexpected:
            raise OCELFormatError(f"{context} has unexpected columns: {unexpected}")
    frame = pl.scan_parquet(files)
    return frame if sorted_by is None else frame.set_sorted(sorted_by)


def _read_parquet_schema(path: Path, context: str) -> dict[str, pl.DataType]:
    """Read one footer and convert corrupt-Parquet errors to the public type."""
    try:
        return dict(pl.read_parquet_schema(path))
    except FileNotFoundError:
        raise
    except PolarsError as exc:
        raise OCELFormatError(
            f"{context} is not a readable Parquet file: {exc}"
        ) from exc
