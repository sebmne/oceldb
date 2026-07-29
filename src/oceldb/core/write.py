"""Write complete native snapshots through a staged installation.

The complete directory is staged next to the target and physically reopened
before it enters the committed target path.

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
import warnings

import polars as pl

from oceldb.core import schema as s
from oceldb.core.schema import (
    E2O_SCHEMA,
    O2O_SCHEMA,
    EVENT_SCHEMA,
    CHANGE_SCHEMA,
    OBJECT_SCHEMA,
)
from oceldb.errors import OCELFormatError, OCELStorageError
from oceldb.types import PathLikeStr

_ROWS_PER_FILE = 250_000
_ROWS_PER_GROUP = 50_000


def write_native(
    path: PathLikeStr,
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
        overwrite: If ``True``, replace an existing valid native dataset at
            *path*. Unrelated files and directories are never replaced.
            If ``False`` (default), raise when *path* exists.

    Raises:
        FileExistsError: If *path* already exists and *overwrite* is
            ``False``.
        OCELStorageError: If *path* is a symlink or ``overwrite=True`` targets
            something other than a valid native dataset.
        ValueError: If a table is missing a required column.
    """
    target = _target_path(path)
    _check_target(target, overwrite=overwrite)
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = target.with_name(f".{target.name}.tmp-{uuid4().hex}")
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
            staging / "e2o",
            E2O_SCHEMA,
            sort_by=(s.OCEL_EVENT_ID, s.OCEL_OBJECT_ID, s.OCEL_QUALIFIER),
        )
        _write_relation(
            o2o,
            staging / "o2o",
            O2O_SCHEMA,
            sort_by=(s.OCEL_SOURCE_ID, s.OCEL_TARGET_ID, s.OCEL_QUALIFIER),
        )
        _write_relation_index(
            pl.scan_parquet(sorted((staging / "e2o").glob("*.parquet"))),
            staging / "indexes" / "e2o_by_object",
            E2O_SCHEMA,
            sort_by=(s.OCEL_OBJECT_ID, s.OCEL_EVENT_ID, s.OCEL_QUALIFIER),
        )
        _write_manifest(staging)
        install_staged_native(staging, target, overwrite=overwrite)
    except BaseException:
        try:
            _remove(staging)
        except OSError:
            # Never hide the exception that caused the write to fail.
            pass
        raise


def copy_native(
    source: Path,
    path: PathLikeStr,
    *,
    overwrite: bool = False,
) -> None:
    """Transactionally copy an unchanged native snapshot."""
    target = _target_path(path)
    source = source.resolve()
    if target == source:
        if not overwrite:
            raise FileExistsError(
                f"Target already exists: {target}. Pass overwrite=True to replace it."
            )
        _require_native_snapshot(source)
        return
    if target.is_relative_to(source) or source.is_relative_to(target):
        raise OCELStorageError(
            "A native snapshot cannot be copied into itself or one of its "
            "ancestor directories."
        )
    _check_target(target, overwrite=overwrite)
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = target.with_name(f".{target.name}.tmp-{uuid4().hex}")
    try:
        shutil.copytree(source, staging, copy_function=shutil.copy2)
        install_staged_native(staging, target, overwrite=overwrite)
    except BaseException:
        try:
            _remove(staging)
        except OSError:
            pass
        raise


def install_staged_native(
    staging: Path,
    path: PathLikeStr,
    *,
    overwrite: bool = False,
) -> None:
    """Validate and atomically install a complete staged native snapshot.

    This internal boundary is shared by the general lazy-frame writer and
    bounded exchange converters that already produced the final physical
    layout.
    """
    target = _target_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    from oceldb.core.open import open_native

    open_native(staging)
    # Check after the potentially long staging operation. Another process may
    # have created or replaced the target in the meantime.
    _check_target(target, overwrite=overwrite)
    backup = target.with_name(f".{target.name}.backup-{uuid4().hex}")
    if target.exists():
        if not overwrite:
            raise FileExistsError(
                f"Target already exists: {target}. Pass overwrite=True to replace it."
            )
        target.rename(backup)
        try:
            _require_native_snapshot(backup)
        except BaseException:
            if not target.exists():
                backup.rename(target)
            raise
    if target.exists():
        raise OCELStorageError(
            f"Target changed during installation: {target}. The previous "
            f"snapshot remains at {backup}."
        )
    try:
        staging.rename(target)
    except BaseException:
        if backup.exists() and not target.exists():
            backup.rename(target)
        raise
    try:
        _remove(backup)
    except OSError as exc:
        try:
            warnings.warn(
                f"Native snapshot was installed at {target}, but its backup "
                f"could not be removed: {backup} ({exc})",
                RuntimeWarning,
                stacklevel=2,
            )
        except Warning:
            # Warning filters must not turn a committed write into a reported
            # failure.
            pass


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
    at least one non-null value or explicit null tombstone for.
    """
    _validate_input_schema(frame, core_schema)
    directory.mkdir()
    attributes = [
        name for name in frame.collect_schema().names() if name not in core_schema
    ]

    # Execute the source plan once, streaming into a fast local spill file.
    spill = directory / ".spill.parquet"
    frame.select(
        *core_schema,
        *attributes,
    ).sink_parquet(spill, compression="lz4", maintain_order=False)

    try:
        source = pl.scan_parquet(spill)
        # One pass answers both "which types exist" and "which attribute
        # columns does each type actually carry values for".
        is_object_changes = s.OCEL_CHANGED_FIELD in core_schema
        presence = (
            source.group_by(s.OCEL_TYPE)
            .agg(
                (
                    pl.col(name).is_not_null()
                    | (
                        (pl.col(s.OCEL_CHANGED_FIELD) == name)
                        if is_object_changes
                        else pl.lit(False)
                    )
                )
                .any()
                .alias(name)
                for name in attributes
            )
            .sort(s.OCEL_TYPE)
            .collect(engine="streaming")
        )
        if presence.get_column(s.OCEL_TYPE).null_count():
            raise ValueError("Native OCEL types must not be null")
        if presence.get_column(s.OCEL_TYPE).str.len_chars().eq(0).any():
            raise ValueError("Native OCEL types must not be empty")
        for row in presence.iter_rows(named=True):
            type_name = row[s.OCEL_TYPE]
            present = [name for name in attributes if row[name]]
            partition = directory / f"ocel_type={quote(type_name, safe='')}"
            partition.mkdir()
            source.filter(pl.col(s.OCEL_TYPE) == type_name).select(
                *(name for name in core_schema if name != s.OCEL_TYPE),
                *present,
            ).sort(*sort_by, descending=descending or False).sink_parquet(
                partition / "part-00000.parquet", compression="zstd"
            )
    finally:
        spill.unlink(missing_ok=True)


def _write_relation(
    frame: pl.LazyFrame,
    directory: Path,
    schema: dict[str, pl.DataType],
    *,
    sort_by: tuple[str, ...],
) -> None:
    """Write a sorted, bounded-shard relation dataset."""
    _validate_input_schema(frame, schema, allow_attributes=False)
    _write_relation_index(
        frame.select(*schema),
        directory,
        schema,
        sort_by=sort_by,
    )


def _write_relation_index(
    frame: pl.LazyFrame,
    directory: Path,
    schema: dict[str, pl.DataType],
    *,
    sort_by: tuple[str, ...],
    descending: tuple[bool, ...] | None = None,
    input_sorted: bool = False,
) -> None:
    """Sort and shard one canonical or secondary relation representation."""
    directory.mkdir(parents=True)
    destination = pl.PartitionBy(
        directory,
        max_rows_per_file=_ROWS_PER_FILE,
    )
    source = frame.select(*schema)
    if not input_sorted:
        source = source.sort(
            *sort_by,
            descending=descending or False,
        )
    source.sink_parquet(
        destination,
        compression="zstd",
        row_group_size=_ROWS_PER_GROUP,
        maintain_order=True,
    )
    generated = sorted(directory.glob("*.parquet"))
    if not generated:
        pl.DataFrame(schema=schema).write_parquet(
            directory / "part-00000.parquet",
            compression="zstd",
            statistics=True,
        )
        return
    temporary = [
        file.rename(directory / f".part-{index:05d}.parquet")
        for index, file in enumerate(generated)
    ]
    for index, file in enumerate(temporary):
        file.rename(directory / f"part-{index:05d}.parquet")


def _validate_input_schema(
    frame: pl.LazyFrame,
    required: dict[str, pl.DataType],
    *,
    allow_attributes: bool = True,
) -> None:
    """Validate a logical input schema without executing its query plan."""
    actual = frame.collect_schema()
    missing = set(required) - set(actual.names())
    if missing:
        raise ValueError(f"Native OCEL table is missing columns: {sorted(missing)}")
    incompatible = [
        f"{name} ({actual[name]!r}, expected {dtype!r})"
        for name, dtype in required.items()
        if actual[name] != dtype
    ]
    if incompatible:
        raise ValueError(
            "Native OCEL table has incompatible columns: " + ", ".join(incompatible)
        )
    attributes = set(actual.names()) - set(required)
    if not allow_attributes and attributes:
        raise ValueError(
            f"Native OCEL relation table has unexpected columns: {sorted(attributes)}"
        )
    reserved_attributes = sorted(attributes & s.RESERVED_COLUMNS)
    if reserved_attributes:
        raise ValueError(
            f"Native OCEL table uses reserved attribute columns: {reserved_attributes}"
        )


def _target_path(path: PathLikeStr) -> Path:
    """Resolve a target's parent without following the final path component."""
    candidate = Path(path).expanduser()
    if candidate.is_symlink():
        raise OCELStorageError(
            f"Refusing to write a native snapshot through symlink: {candidate}"
        )
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    target = candidate.parent.resolve() / candidate.name
    if target.is_symlink():
        raise OCELStorageError(
            f"Refusing to write a native snapshot through symlink: {target}"
        )
    return target


def _check_target(target: Path, *, overwrite: bool) -> None:
    """Ensure an existing target is safe to replace."""
    if target.is_symlink():
        raise OCELStorageError(
            f"Refusing to write a native snapshot through symlink: {target}"
        )
    if not target.exists():
        return
    if not overwrite:
        raise FileExistsError(
            f"Target already exists: {target}. Pass overwrite=True to replace it."
        )
    _require_native_snapshot(target)


def _require_native_snapshot(target: Path) -> None:
    """Raise unless *target* is a physically valid native snapshot."""
    try:
        from oceldb.core.open import open_native

        open_native(target)
    except (FileNotFoundError, OCELFormatError, OSError) as exc:
        raise OCELStorageError(
            f"Refusing to overwrite {target}: it is not a valid native oceldb snapshot."
        ) from exc


def _write_manifest(directory: Path) -> None:
    """Write the version-2 commit-marker manifest into *directory*."""
    document: dict[str, object] = {
        "format": "oceldb",
        "formatVersion": 2,
        "createdAt": datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "relations": {
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
        },
    }
    if (directory / "indexes" / "e2o_by_object").is_dir():
        document["indexes"] = {
            "e2oByObject": {
                "path": "indexes/e2o_by_object",
                "sort": [
                    s.OCEL_OBJECT_ID,
                    s.OCEL_EVENT_ID,
                    s.OCEL_QUALIFIER,
                ],
            }
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
