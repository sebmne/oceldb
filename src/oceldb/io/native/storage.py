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

``ocel_type`` is encoded in partition directory names and is re-attached as a
regular column when reading. The per-type files therefore store only the core
columns and type-specific attributes.
"""

from collections.abc import Mapping
from pathlib import Path
import shutil
from typing import cast

import polars as pl

from oceldb import schema as s
from oceldb.core.dataset import (
    OCELDataset,
    OCELTable,
    OCELTables,
)
from oceldb.core.validation import validate_dataset, validate_storage_input
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
from oceldb.schema import AttributeType, OCELSchema, TypeAttributes


def open_native(path: str | Path) -> tuple[OCELDataset, OCELSchema]:
    """Open a native dataset as typed lazy frames plus its declared schema."""
    base = Path(path).resolve()
    if not base.is_dir():
        raise FileNotFoundError(f"Native oceldb directory not found: {base}")
    manifest = read_manifest(base)
    validate_native_layout(base, manifest.schema)
    return staged_dataset(base, metadata=manifest.metadata), manifest.schema


def staged_dataset(
    path: str | Path,
    *,
    metadata: Mapping[str, object] | None = None,
) -> OCELDataset:
    """Scan complete native tables before their manifest is committed."""
    base = Path(path).resolve()
    event_partitions = _scan_partitions(base, EVENTS)
    object_partitions = _scan_partitions(base, OBJECTS)
    change_partitions = _scan_partitions(base, OBJECT_CHANGES)
    return OCELDataset(
        tables=OCELTables(
            events=OCELTable(
                _union_partitions(event_partitions, EVENTS),
                partitions=event_partitions,
                source=EVENTS.root(base),
            ),
            objects=OCELTable(
                _union_partitions(object_partitions, OBJECTS),
                partitions=object_partitions,
                source=OBJECTS.root(base),
            ),
            object_changes=OCELTable(
                _union_partitions(change_partitions, OBJECT_CHANGES),
                partitions=change_partitions,
                source=OBJECT_CHANGES.root(base),
            ),
            event_object=OCELTable(
                _scan_relation(base, EVENT_OBJECT),
                source=EVENT_OBJECT.root(base),
            ),
            object_object=OCELTable(
                _scan_relation(base, OBJECT_OBJECT),
                source=OBJECT_OBJECT.root(base),
            ),
        ),
        metadata=metadata or {},
    )


def write_native(
    dataset: OCELDataset,
    path: str | Path,
    *,
    declared: OCELSchema | None = None,
    overwrite: bool = False,
) -> None:
    """Transactionally write a typed dataset in the native Parquet layout.

    ``declared`` fixes the manifest's types and per-type attribute columns;
    without it both are inferred from the staged partitions.
    """
    try:
        _write_dataset(dataset, Path(path), declared=declared, overwrite=overwrite)
    except (OCELDBError, OSError):
        raise
    except Exception as exc:
        raise NativeStorageError(f"Cannot write native dataset {path}: {exc}") from exc


def _write_dataset(
    dataset: OCELDataset,
    base: Path,
    *,
    declared: OCELSchema | None,
    overwrite: bool,
) -> None:
    validate_storage_input(dataset)
    tables = dataset.tables

    with DirectoryTransaction(base, overwrite=overwrite) as staging:
        _persist_partitioned(
            tables.events,
            staging,
            EVENTS,
            declared.event_types if declared is not None else None,
        )
        _persist_partitioned(
            tables.objects,
            staging,
            OBJECTS,
            {name: {} for name in declared.object_types}
            if declared is not None
            else None,
        )
        _persist_partitioned(
            tables.object_changes,
            staging,
            OBJECT_CHANGES,
            declared.object_types if declared is not None else None,
        )
        _persist_relation(tables.event_object, staging, EVENT_OBJECT)
        _persist_relation(tables.object_object, staging, OBJECT_OBJECT)
        storage_schema = (
            declared if declared is not None else infer_storage_schema(staging)
        )
        validate_native_layout(staging, storage_schema)
        validate_dataset(staged_dataset(staging, metadata=dataset.metadata))
        write_manifest(
            staging,
            schema=storage_schema,
            metadata=dataset.metadata,
        )


def _persist_partitioned(
    data: OCELTable,
    target: Path,
    table: NativeTable,
    declared_types: Mapping[str, TypeAttributes] | None,
) -> None:
    if data.source is not None:
        _copy_source(data.source, table.root(target), partitioned=True)
        return
    _write_partitioned(
        data.all(),
        target,
        table,
        declared_types,
        data.partitions,
    )


def _persist_relation(data: OCELTable, target: Path, table: NativeTable) -> None:
    if data.source is not None:
        _copy_source(data.source, table.root(target), partitioned=False)
        return
    _write_relation(data.all(), target, table)


def _copy_source(source_path: Path, target_path: Path, *, partitioned: bool) -> None:
    """Copy an unchanged validated native table into a transaction."""
    if partitioned:
        shutil.copytree(source_path, target_path)
    elif source_path.exists():
        shutil.copyfile(source_path, target_path)


def _scan_partitions(base: Path, table: NativeTable) -> dict[str, pl.LazyFrame]:
    base_dir = table.root(base)
    partitions: dict[str, pl.LazyFrame] = {}
    if base_dir.is_dir():
        for child in sorted(base_dir.iterdir()):
            if not child.is_dir() or not child.name.startswith(PARTITION_PREFIX):
                continue
            encoded = child.name.removeprefix(PARTITION_PREFIX)
            type_name = decode_type_name(encoded)
            file = table.file(base, type_name=type_name)
            if not file.exists():
                continue
            partitions[type_name] = pl.scan_parquet(file).with_columns(
                pl.lit(type_name, dtype=pl.String()).alias(s.OCEL_TYPE)
            )
    return partitions


def _union_partitions(
    partitions: Mapping[str, pl.LazyFrame], table: NativeTable
) -> pl.LazyFrame:
    if not partitions:
        return pl.LazyFrame(schema={**table.base_schema, s.OCEL_TYPE: pl.String()})
    return pl.concat(list(partitions.values()), how="diagonal_relaxed")


def _scan_relation(base: Path, table: NativeTable) -> pl.LazyFrame:
    file = table.file(base)
    if file.exists():
        return pl.scan_parquet(file)
    return pl.LazyFrame(schema=table.base_schema)


def _write_partitioned(
    frame: pl.LazyFrame,
    base: Path,
    table: NativeTable,
    declared_types: Mapping[str, TypeAttributes] | None,
    partitions: Mapping[str, pl.LazyFrame] | None,
) -> None:
    base_dir = table.root(base)
    base_dir.mkdir(parents=True)
    observed = (
        set(partitions) if partitions is not None else set(_distinct_types(frame))
    )
    type_names = observed | (
        set(declared_types) if declared_types is not None else set()
    )
    for type_name in sorted(type_names):
        typed = (
            partitions[type_name]
            if partitions is not None and type_name in partitions
            else frame.filter(pl.col(s.OCEL_TYPE) == type_name)
        )
        if declared_types is None:
            attributes = _present_attributes(typed, table)
        elif type_name in declared_types:
            attributes = declared_types[type_name]
        else:
            # Filters retain per-type plans for types they emptied out; an
            # undeclared partition only carries data when the declaration was
            # inconsistent, so keep it and let layout validation report that.
            if typed.limit(1).collect().height == 0:
                continue
            attributes = _present_attributes(typed, table)
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
    """Infer populated attributes for a schema-less typed frame."""
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
