"""Write the native layout transactionally.

The complete directory is staged next to the target and renamed into place,
so a failed write never leaves a half-written dataset.

Each partitioned table's source plan is executed exactly ONCE: it is streamed
into a temporary spill file, and the type partitioning, per-type attribute
presence, and sorted partition files are then derived from cheap re-scans of
that local file. Without the spill, a lazy pipeline would re-execute for the
type listing, once per type for the presence probe, and once per type for the
partition sink — roughly 2N+1 times for N types.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
import shutil
from urllib.parse import quote
from uuid import uuid4

import polars as pl

from oceldb.core import schema as s
from oceldb.core.schema import (
    E2O_SCHEMA,
    O2O_SCHEMA,
    EVENT_SCHEMA,
    CHANGE_SCHEMA,
    OBJECT_SCHEMA,
)


def write_native(
    path: str | Path,
    *,
    events: pl.LazyFrame,
    objects: pl.LazyFrame,
    object_changes: pl.LazyFrame,
    e2o: pl.LazyFrame,
    o2o: pl.LazyFrame,
    overwrite: bool = False,
) -> None:
    """Write five lazy frames as a native oceldb dataset at *path*.

    See :meth:`oceldb.ocel.OCEL.write`, which calls this, and
    ``docs/storage-format.md`` for the on-disk layout produced.

    Args:
        path: Directory to write the dataset to.
        events: The events table.
        objects: The objects table.
        object_changes: The object_changes table.
        e2o: The E2O relation table.
        o2o: The O2O relation table.
        overwrite: If ``True``, replace an existing dataset at *path*. If
            ``False`` (default), raise instead.

    Raises:
        FileExistsError: If *path* already exists and *overwrite* is
            ``False``.
        ValueError: If a table is missing a required column.
    """
    target = Path(path).expanduser().resolve()
    if target.exists() and not overwrite:
        raise FileExistsError(
            f"Target already exists: {target}. Pass overwrite=True to replace it."
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = target.with_name(f".{target.name}.tmp-{uuid4().hex}")
    backup = target.with_name(f".{target.name}.backup-{uuid4().hex}")
    staging.mkdir()

    try:
        _write_partitioned(
            events, staging / "events", EVENT_SCHEMA, sort_by=(s.OCEL_TIME,)
        )
        _write_partitioned(
            objects, staging / "objects", OBJECT_SCHEMA, sort_by=(s.OCEL_ID,)
        )
        _write_partitioned(
            object_changes,
            staging / "object_changes",
            CHANGE_SCHEMA,
            sort_by=(s.OCEL_ID, s.OCEL_IS_INITIAL, s.OCEL_TIME),
            descending=(False, True, False),
        )
        _write_relation(
            e2o,
            staging / "e2o.parquet",
            E2O_SCHEMA,
            sort_by=(s.OCEL_OBJECT_ID, s.OCEL_EVENT_ID),
        )
        _write_relation(
            o2o,
            staging / "o2o.parquet",
            O2O_SCHEMA,
            sort_by=(s.OCEL_SOURCE_ID, s.OCEL_TARGET_ID),
            optional=True,
        )
        _write_manifest(staging)

        if target.exists():
            if not overwrite:
                raise FileExistsError(
                    f"Target already exists: {target}. "
                    "Pass overwrite=True to replace it."
                )
            target.rename(backup)
        try:
            staging.rename(target)
        except BaseException:
            if backup.exists():
                backup.rename(target)
            raise
        _remove(backup)
    except BaseException:
        _remove(staging)
        raise


def _write_partitioned(
    frame: pl.LazyFrame,
    directory: Path,
    core_schema: dict[str, pl.DataType],
    *,
    sort_by: tuple[str, ...],
    descending: tuple[bool, ...] | None = None,
) -> None:
    """Write *frame* as one sorted Parquet file per ``ocel_type`` partition.

    Each partition keeps only the attribute columns that type actually has
    at least one non-null value for.
    """
    _require_columns(frame, core_schema)
    directory.mkdir()
    attributes = [
        name for name in frame.collect_schema().names() if name not in core_schema
    ]

    # Execute the source plan once, streaming into a fast local spill file.
    spill = directory / ".spill.parquet"
    frame.select(
        *(pl.col(name).cast(dtype, strict=True) for name, dtype in core_schema.items()),
        *attributes,
    ).sink_parquet(spill, compression="lz4")

    try:
        source = pl.scan_parquet(spill)
        # One pass answers both "which types exist" and "which attribute
        # columns does each type actually carry values for".
        presence = (
            source.group_by(s.OCEL_TYPE)
            .agg(pl.col(name).is_not_null().any() for name in attributes)
            .sort(s.OCEL_TYPE)
            .collect()
        )
        if presence.get_column(s.OCEL_TYPE).null_count():
            raise ValueError("Native OCEL types must not be null")
        for row in presence.iter_rows(named=True):
            type_name = row[s.OCEL_TYPE]
            present = [name for name in attributes if row[name]]
            partition = directory / f"ocel_type={quote(type_name, safe='')}"
            partition.mkdir()
            source.filter(pl.col(s.OCEL_TYPE) == type_name).select(
                *(name for name in core_schema if name != s.OCEL_TYPE),
                *present,
            ).sort(*sort_by, descending=descending or False).sink_parquet(
                partition / "data.parquet", compression="zstd"
            )
    finally:
        spill.unlink(missing_ok=True)


def _write_relation(
    frame: pl.LazyFrame,
    path: Path,
    schema: dict[str, pl.DataType],
    *,
    sort_by: tuple[str, ...],
    optional: bool = False,
) -> None:
    """Write *frame* as a single sorted Parquet file at *path*.

    If *optional* and *frame* is empty, no file is written — a missing file
    means an empty relation on read.
    """
    _require_columns(frame, schema)
    if optional and frame.limit(1).collect().is_empty():
        return
    frame.select(
        *(pl.col(name).cast(dtype, strict=True) for name, dtype in schema.items())
    ).sort(*sort_by).sink_parquet(path, compression="zstd")


def _require_columns(frame: pl.LazyFrame, required: dict[str, pl.DataType]) -> None:
    """Raise if *frame* is missing any *required* column."""
    missing = set(required) - set(frame.collect_schema().names())
    if missing:
        raise ValueError(f"Native OCEL table is missing columns: {sorted(missing)}")


def _write_manifest(directory: Path) -> None:
    """Write the version-2 commit-marker manifest into *directory*."""
    document = {
        "format": "oceldb",
        "formatVersion": 2,
        "createdAt": datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
    }
    (directory / "manifest.json").write_text(
        json.dumps(document, indent=2) + "\n", encoding="utf-8"
    )


def _remove(path: Path) -> None:
    """Remove *path*, whether it's a directory, file, or symlink."""
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        path.unlink(missing_ok=True)
