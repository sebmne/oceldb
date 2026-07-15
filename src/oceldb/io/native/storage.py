"""Read and write oceldb's native Parquet storage layout.

An oceldb log is a directory of Parquet files split by logical table and OCEL
type:

```
log/
  events/ocel_type=<urlencoded>/data.parquet
  objects/ocel_type=<urlencoded>/data.parquet
  object_changes/ocel_type=<urlencoded>/data.parquet
  event_object.parquet
  object_object.parquet
```

The partitioned tables are opened as one hive scan each: ``ocel_type`` comes
from the partition directory names, the union column schema comes from the
manifest, and per-type files only store the columns their type declares.
Type predicates are pruned to the matching partition files by the query
optimizer. An attribute column shared by several types must therefore carry
one dtype across partitions; writers widen conflicting declarations.
"""

from pathlib import Path
import shutil
from typing import cast

import polars as pl

from oceldb import schema as s
from oceldb.core.presence import TypeDirectory
from oceldb.core.validation import validate_storage_input, validate_tables
from oceldb.errors import OCELDBError
from oceldb.io._paths import DirectoryTransaction
from oceldb.io.native.layout import (
    COMPRESSION,
    EVENTS,
    EVENT_OBJECT,
    OBJECTS,
    OBJECT_CHANGES,
    OBJECT_OBJECT,
    PARTITION_PREFIX,
    NativeTable,
    NativeStorageError,
    decode_type_name,
    validate_native_layout,
)
from oceldb.io.native.manifest import read_manifest, write_manifest
from oceldb.ocel import OCEL
from oceldb.schema import (
    AttributeType,
    OCELSchema,
    TypeAttributes,
    widen_shared_attributes,
)
from collections.abc import Mapping


def open_native(path: str | Path) -> OCEL:
    """Open a native dataset as lazy hive scans seeded with its schema."""
    base = Path(path).resolve()
    if not base.is_dir():
        raise FileNotFoundError(f"Native oceldb directory not found: {base}")
    manifest = read_manifest(base)
    validate_native_layout(base, manifest.schema)
    return OCEL(
        **scan_native_tables(base, manifest.schema),
        metadata=manifest.metadata,
        presence=TypeDirectory.from_schema(manifest.schema),
        source=base,
    )


def scan_native_tables(
    path: str | Path, declared: OCELSchema
) -> dict[str, pl.LazyFrame]:
    """Scan the five native tables using the declared union schemas."""
    base = Path(path).resolve()
    return {
        "events": _scan_typed(base, EVENTS, declared.event_types),
        "objects": _scan_typed(
            base, OBJECTS, {name: {} for name in declared.object_types}
        ),
        "object_changes": _scan_typed(base, OBJECT_CHANGES, declared.object_types),
        "event_object": _scan_relation(base, EVENT_OBJECT),
        "object_object": _scan_relation(base, OBJECT_OBJECT),
    }


def write_native(
    ocel: OCEL,
    path: str | Path,
    *,
    overwrite: bool = False,
) -> None:
    """Transactionally write an OCEL in the native Parquet layout.

    A pristine opened dataset is copied verbatim. Otherwise the manifest
    declares the log's attribute directory, with attributes shared across
    types widened to one common dtype.
    """
    try:
        _write_dataset(ocel, Path(path), overwrite=overwrite)
    except (OCELDBError, OSError):
        raise
    except Exception as exc:
        raise NativeStorageError(f"Cannot write native dataset {path}: {exc}") from exc


def _write_dataset(ocel: OCEL, base: Path, *, overwrite: bool) -> None:
    source = ocel._source
    if source is not None:
        with DirectoryTransaction(base, overwrite=overwrite) as staging:
            shutil.copytree(source, staging, dirs_exist_ok=True)
        return

    validate_storage_input(
        events=ocel.events(),
        objects=ocel.objects(),
        object_changes=ocel.object_changes(),
        event_object=ocel.event_object(),
        object_object=ocel.object_object(),
    )
    declared = widen_shared_attributes(ocel._directory().to_schema())
    with DirectoryTransaction(base, overwrite=overwrite) as staging:
        _write_partitioned(ocel.events(), staging, EVENTS, declared.event_types)
        _write_partitioned(
            ocel.objects(),
            staging,
            OBJECTS,
            {name: {} for name in declared.object_types},
        )
        _write_partitioned(
            ocel.object_changes(), staging, OBJECT_CHANGES, declared.object_types
        )
        _write_relation(ocel.event_object(), staging, EVENT_OBJECT)
        _write_relation(ocel.object_object(), staging, OBJECT_OBJECT)
        validate_native_layout(staging, declared)
        validate_tables(**scan_native_tables(staging, declared))
        write_manifest(staging, schema=declared, metadata=ocel.metadata)


def _scan_typed(
    base: Path,
    table: NativeTable,
    declared_types: Mapping[str, TypeAttributes],
) -> pl.LazyFrame:
    union = _union_schema(table, declared_types)
    if not _partition_files(base, table):
        return pl.LazyFrame(schema={**union, s.OCEL_TYPE: pl.String()})
    return pl.scan_parquet(
        table.root(base) / "**" / "*.parquet",
        hive_partitioning=True,
        hive_schema={s.OCEL_TYPE: pl.String()},
        schema=union,
        missing_columns="insert",
    )


def _union_schema(
    table: NativeTable,
    declared_types: Mapping[str, TypeAttributes],
) -> dict[str, pl.DataType]:
    """Union the declared per-type columns, rejecting physical conflicts."""
    result: dict[str, pl.DataType] = dict(table.base_schema)
    for type_name, attributes in declared_types.items():
        for name, attr_type in attributes.items():
            dtype = attr_type.polars_dtype()
            previous = result.get(name)
            if previous is not None and previous != dtype:
                raise NativeStorageError(
                    f"Native table {table.key!r} declares attribute {name!r} "
                    f"with conflicting types ({previous!r} vs {dtype!r} in "
                    f"{type_name!r}). Rewrite the dataset with a current "
                    "oceldb version to widen shared attributes."
                )
            result[name] = dtype
    return result


def _partition_files(base: Path, table: NativeTable) -> list[Path]:
    base_dir = table.root(base)
    if not base_dir.is_dir():
        return []
    return [
        file
        for child in sorted(base_dir.iterdir())
        if child.is_dir() and child.name.startswith(PARTITION_PREFIX)
        for file in sorted(child.glob("*.parquet"))
    ]


def _scan_relation(base: Path, table: NativeTable) -> pl.LazyFrame:
    file = table.file(base)
    if file.exists():
        return pl.scan_parquet(file)
    return pl.LazyFrame(schema=table.base_schema)


def _write_partitioned(
    frame: pl.LazyFrame,
    base: Path,
    table: NativeTable,
    declared_types: Mapping[str, TypeAttributes],
) -> None:
    base_dir = table.root(base)
    base_dir.mkdir(parents=True)
    type_names = set(_distinct_types(frame)) | set(declared_types)
    for type_name in sorted(type_names):
        typed = frame.filter(pl.col(s.OCEL_TYPE) == type_name)
        attributes = (
            declared_types[type_name]
            if type_name in declared_types
            else _present_attributes(typed, table)
        )
        output = _prepare_frame(typed.drop(s.OCEL_TYPE), table, attributes)
        file = table.file(base, type_name=type_name)
        file.parent.mkdir()
        _sink(output.sort(*table.sort_by), file)


def _write_relation(
    frame: pl.LazyFrame,
    base: Path,
    table: NativeTable,
) -> None:
    if table.optional and frame.limit(1).collect().height == 0:
        return
    output = _prepare_frame(frame, table, {})
    _sink(output.sort(*table.sort_by), table.file(base))


def _prepare_frame(
    frame: pl.LazyFrame,
    table: NativeTable,
    attributes: TypeAttributes,
) -> pl.LazyFrame:
    """Select and cast one physical table without materializing its rows."""
    actual = frame.collect_schema()
    desired = {
        **table.base_schema,
        **{name: attr_type.polars_dtype() for name, attr_type in attributes.items()},
    }
    normalized = frame.with_columns(
        *(
            pl.lit(None, dtype=dtype).alias(name)
            if name not in actual
            else pl.col(name).cast(dtype, strict=True)
            for name, dtype in desired.items()
            if name not in actual or actual[name] != dtype
        )
    )
    return normalized.select(*desired)


def _present_attributes(
    frame: pl.LazyFrame, table: NativeTable
) -> dict[str, AttributeType]:
    """Infer populated attributes for an undeclared typed frame."""
    schema = frame.collect_schema()
    candidates = [
        name
        for name in schema.names()
        if name not in table.columns and name != s.OCEL_TYPE
    ]
    if not candidates:
        return {}
    presence = frame.select(
        pl.col(name).is_not_null().any().alias(name) for name in candidates
    ).collect()
    return {
        name: AttributeType.from_polars(schema[name])
        for name in candidates
        if presence.get_column(name).item()
    }


def _sink(frame: pl.LazyFrame, file: Path) -> None:
    """Stream a normalized lazy frame into one native Parquet file."""
    frame.sink_parquet(file, compression=COMPRESSION, mkdir=True)


def _distinct_types(frame: pl.LazyFrame) -> list[str]:
    values = (
        frame.select(s.OCEL_TYPE)
        .unique()
        .collect()
        .get_column(s.OCEL_TYPE)
        .drop_nulls()
        .sort()
        .to_list()
    )
    return [str(value) for value in cast(list[object], values)]


def _partition_schemas(
    base: Path, table: NativeTable
) -> dict[str, dict[str, AttributeType]]:
    base_dir = table.root(base)
    result: dict[str, dict[str, AttributeType]] = {}
    if not base_dir.is_dir():
        return result
    for child in sorted(base_dir.iterdir()):
        if not child.is_dir() or not child.name.startswith(PARTITION_PREFIX):
            continue
        encoded = child.name.removeprefix(PARTITION_PREFIX)
        type_name = decode_type_name(encoded)
        file = table.file(base, type_name=type_name)
        if not file.exists():
            continue
        parquet_schema = pl.read_parquet_schema(file)
        result[type_name] = {
            name: AttributeType.from_polars(dtype)
            for name, dtype in parquet_schema.items()
            if name not in table.columns
        }
    return result


def _object_type_schemas(base: Path) -> dict[str, dict[str, AttributeType]]:
    identity_types = _partition_schemas(base, OBJECTS)
    change_types = _partition_schemas(base, OBJECT_CHANGES)
    return {
        name: dict(change_types.get(name, {}))
        for name in sorted(set(identity_types) | set(change_types))
    }


def infer_storage_schema(path: str | Path) -> OCELSchema:
    """Infer the schema from staged native Parquet partitions."""
    base = Path(path)
    return OCELSchema(
        event_types=_partition_schemas(base, EVENTS),
        object_types=_object_type_schemas(base),
    )
