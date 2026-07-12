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

import shutil
import urllib.parse
from collections.abc import Mapping
from pathlib import Path
from typing import cast
from uuid import uuid4

import polars as pl

from oceldb import schema as s
from oceldb.core.dataset import OCELDataset, OCELFrames
from oceldb.io.native.manifest import read_manifest, write_manifest
from oceldb.schema import AttributeType, OCELSchema, TypeAttributes
from oceldb.schema._layout import (
    CHANGE_CORE,
    E2O_COLUMNS,
    EVENT_CORE,
    OBJECT_CORE,
    O2O_COLUMNS,
)

_COMPRESSION = "zstd"

_PREFIX = "ocel_type="


def encode_type_name(type_name: str) -> str:
    """Encode an OCEL type name for a partition directory.

    Args:
        type_name: Event or object type name as it appears in the OCEL data.

    Returns:
        A URL-encoded string that is safe to place after ``ocel_type=`` in a
        directory name.

    Examples:
        >>> encode_type_name("Place Order")
        'Place%20Order'
        >>> encode_type_name("invoice/item")
        'invoice%2Fitem'
    """
    return urllib.parse.quote(type_name, safe="")


def decode_type_name(encoded: str) -> str:
    """Decode a partition directory type name back to its OCEL type name.

    Args:
        encoded: URL-encoded type name without the ``ocel_type=`` prefix.

    Returns:
        The original event or object type name.
    """
    return urllib.parse.unquote(encoded)


def open_native(path: str | Path) -> OCELDataset:
    """Open a native dataset as typed lazy frames, schema, and metadata."""
    base = Path(path)
    if not base.is_dir():
        raise FileNotFoundError(f"Native oceldb directory not found: {base}")
    manifest = read_manifest(base)
    return OCELDataset(
        frames=OCELFrames(
            events=_scan_partitioned(
                base / "events",
                {s.OCEL_ID: pl.String(), s.OCEL_TIME: pl.Datetime("us")},
            ),
            objects=_scan_partitioned(base / "objects", {s.OCEL_ID: pl.String()}),
            object_changes=_scan_partitioned(
                base / "object_changes",
                {
                    s.OCEL_ID: pl.String(),
                    s.OCEL_TIME: pl.Datetime("us"),
                    s.OCEL_CHANGED_FIELD: pl.String(),
                },
            ),
            event_object=_scan_relation(base / "event_object.parquet", E2O_COLUMNS),
            object_object=_scan_relation(base / "object_object.parquet", O2O_COLUMNS),
        ),
        schema=manifest.schema,
        metadata=manifest.metadata,
    )


def write_native(
    dataset: OCELDataset,
    path: str | Path,
    *,
    overwrite: bool = False,
) -> None:
    """Atomically write a typed dataset in the native Parquet layout."""
    _write_dataset(dataset, Path(path), overwrite=overwrite)


def _write_dataset(
    dataset: OCELDataset,
    base: Path,
    *,
    overwrite: bool,
) -> None:
    frames = dataset.frames
    schema = dataset.schema

    if base.exists() and not overwrite:
        raise FileExistsError(
            f"Target already exists: {base}. Pass overwrite=True to replace it."
        )

    staging = base.with_name(f"{base.name}.tmp-{uuid4().hex}")
    staging.mkdir(parents=True)
    try:
        _write_partitioned(
            frames.events,
            staging / "events",
            EVENT_CORE,
            (s.OCEL_TIME,),
            schema.event_types if schema is not None else None,
        )
        _write_partitioned(
            frames.objects,
            staging / "objects",
            OBJECT_CORE,
            (s.OCEL_ID,),
            {name: {} for name in schema.object_types} if schema is not None else None,
        )
        _write_partitioned(
            frames.object_changes,
            staging / "object_changes",
            CHANGE_CORE,
            (s.OCEL_ID, s.OCEL_TIME),
            schema.object_types if schema is not None else None,
        )
        _write_relation(
            frames.event_object,
            staging / "event_object.parquet",
            E2O_COLUMNS,
            (s.OCEL_OBJECT_ID, s.OCEL_EVENT_ID),
        )
        _write_relation(
            frames.object_object,
            staging / "object_object.parquet",
            O2O_COLUMNS,
            (s.OCEL_SOURCE_ID, s.OCEL_TARGET_ID),
            skip_if_empty=True,
        )
        write_manifest(
            staging,
            schema=infer_storage_schema(staging),
            metadata=dataset.metadata,
        )
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise

    if base.is_dir():
        shutil.rmtree(base)
    elif base.exists():
        base.unlink()
    staging.rename(base)


def _scan_partitioned(
    base_dir: Path, empty_schema: dict[str, pl.DataType]
) -> pl.LazyFrame:
    frames: list[pl.LazyFrame] = []
    if base_dir.is_dir():
        for child in sorted(base_dir.iterdir()):
            file = child / "data.parquet"
            if not (
                child.is_dir() and child.name.startswith(_PREFIX) and file.exists()
            ):
                continue
            type_name = decode_type_name(child.name[len(_PREFIX) :])
            frames.append(
                pl.scan_parquet(file).with_columns(
                    pl.lit(type_name, dtype=pl.String()).alias(s.OCEL_TYPE)
                )
            )
    if not frames:
        return pl.LazyFrame(schema={**empty_schema, s.OCEL_TYPE: pl.String()})
    return pl.concat(frames, how="diagonal_relaxed")


def _scan_relation(file: Path, columns: tuple[str, ...]) -> pl.LazyFrame:
    if file.exists():
        return pl.scan_parquet(file)
    return pl.LazyFrame(schema={c: pl.String() for c in columns})


def _write_partitioned(
    frame: pl.LazyFrame,
    base_dir: Path,
    core: tuple[str, ...],
    sort_by: tuple[str, ...],
    declared_types: Mapping[str, TypeAttributes] | None,
) -> None:
    base_dir.mkdir(parents=True)
    observed = set(_distinct_types(frame))
    type_names = observed | (
        set(declared_types) if declared_types is not None else set()
    )
    for type_name in sorted(type_names):
        df = (
            frame.filter(pl.col(s.OCEL_TYPE) == type_name)
            .drop(s.OCEL_TYPE)
            .sort(*sort_by)
            .collect()
        )
        if declared_types is None:
            df = _drop_all_null_attributes(df, core)
        else:
            df = _apply_declared_schema(df, core, declared_types.get(type_name, {}))
        out_dir = base_dir / f"{_PREFIX}{encode_type_name(type_name)}"
        out_dir.mkdir()
        df.write_parquet(out_dir / "data.parquet", compression=_COMPRESSION)


def _write_relation(
    frame: pl.LazyFrame,
    file: Path,
    columns: tuple[str, ...],
    sort_by: tuple[str, ...],
    *,
    skip_if_empty: bool = False,
) -> None:
    df = frame.select(*columns).sort(*sort_by).collect()
    if skip_if_empty and df.height == 0:
        return
    df.write_parquet(file, compression=_COMPRESSION)


def _drop_all_null_attributes(df: pl.DataFrame, core: tuple[str, ...]) -> pl.DataFrame:
    kept = [
        c
        for c in df.columns
        if c not in core and df.get_column(c).null_count() < df.height
    ]
    return df.select(*core, *kept)


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


def _apply_declared_schema(
    df: pl.DataFrame, core: tuple[str, ...], attributes: TypeAttributes
) -> pl.DataFrame:
    for name, attr_type in attributes.items():
        dtype = attr_type.polars_dtype()
        if name not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=dtype).alias(name))
        elif df.schema[name] != dtype:
            df = df.with_columns(pl.col(name).cast(dtype, strict=True))
    extras = [
        name
        for name in df.columns
        if name not in core
        and name not in attributes
        and df.get_column(name).null_count() < df.height
    ]
    return df.select(*core, *attributes, *extras)


def _partition_schemas(
    base_dir: Path, core: tuple[str, ...]
) -> dict[str, dict[str, AttributeType]]:
    result: dict[str, dict[str, AttributeType]] = {}
    if not base_dir.is_dir():
        return result
    for child in sorted(base_dir.iterdir()):
        file = child / "data.parquet"
        if not (child.is_dir() and child.name.startswith(_PREFIX) and file.exists()):
            continue
        type_name = decode_type_name(child.name[len(_PREFIX) :])
        parquet_schema = pl.read_parquet_schema(file)
        result[type_name] = {
            name: AttributeType.from_polars(dtype)
            for name, dtype in parquet_schema.items()
            if name not in core
        }
    return result


def _object_type_schemas(base: Path) -> dict[str, dict[str, AttributeType]]:
    identity_types = _partition_schemas(base / "objects", OBJECT_CORE)
    change_types = _partition_schemas(base / "object_changes", CHANGE_CORE)
    return {
        name: dict(change_types.get(name, {}))
        for name in sorted(set(identity_types) | set(change_types))
    }


def infer_storage_schema(path: str | Path) -> OCELSchema:
    """Infer the schema from staged native Parquet partitions."""
    base = Path(path)
    return OCELSchema(
        event_types=_partition_schemas(base / "events", EVENT_CORE),
        object_types=_object_type_schemas(base),
    )
