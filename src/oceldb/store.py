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

_COMPRESSION = "zstd"

_EVENT_CORE: tuple[str, ...] = (s.OCEL_ID, s.OCEL_TIME)
_OBJECT_CORE: tuple[str, ...] = (s.OCEL_ID,)
_CHANGE_CORE: tuple[str, ...] = (s.OCEL_ID, s.OCEL_TIME, s.OCEL_CHANGED_FIELD)

_E2O_COLS: tuple[str, ...] = (
    s.OCEL_EVENT_ID,
    s.OCEL_EVENT_TYPE,
    s.OCEL_OBJECT_ID,
    s.OCEL_OBJECT_TYPE,
    s.OCEL_QUALIFIER,
)
_O2O_COLS: tuple[str, ...] = (
    s.OCEL_SOURCE_ID,
    s.OCEL_SOURCE_TYPE,
    s.OCEL_TARGET_ID,
    s.OCEL_TARGET_TYPE,
    s.OCEL_QUALIFIER,
)
_FRAME_NAMES: tuple[str, ...] = ("events", "objects", "object_changes", "e2o", "o2o")

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


def read_frames(path: str | Path) -> dict[str, pl.LazyFrame]:
    """Open an oceldb Parquet directory as lazy Polars frames.

    Args:
        path: Directory containing the native oceldb layout.

    Returns:
        A dictionary with the keys ``events``, ``objects``,
        ``object_changes``, ``e2o``, and ``o2o``. Each value is a
        :class:`polars.LazyFrame`. Missing optional relation files are returned
        as empty lazy frames with the expected schema.

    Notes:
        This function only builds lazy scans. It does not verify that relation
        ids are valid and it does not read Parquet row data until the returned
        frames are collected.

    Examples:
        >>> frames = read_frames("converted-log")
        >>> frames["events"].select("ocel_type").unique().collect()
    """
    base = Path(path)
    return {
        "events": _scan_partitioned(
            base / "events",
            {s.OCEL_ID: pl.String(), s.OCEL_TIME: pl.Datetime("us")},
        ),
        "objects": _scan_partitioned(base / "objects", {s.OCEL_ID: pl.String()}),
        "object_changes": _scan_partitioned(
            base / "object_changes",
            {
                s.OCEL_ID: pl.String(),
                s.OCEL_TIME: pl.Datetime("us"),
                s.OCEL_CHANGED_FIELD: pl.String(),
            },
        ),
        "e2o": _scan_relation(base / "event_object.parquet", _E2O_COLS),
        "o2o": _scan_relation(base / "object_object.parquet", _O2O_COLS),
    }


def write_frames(
    frames: Mapping[str, pl.LazyFrame],
    path: str | Path,
    *,
    overwrite: bool = False,
) -> None:
    """Write lazy frames to oceldb's native Parquet directory layout.

    Args:
        frames: Mapping with exactly the logical frames used by ``OCEL``:
            ``events``, ``objects``, ``object_changes``, ``e2o``, and ``o2o``.
            Extra keys are ignored. Values may be any lazy Polars computation
            that produces the expected columns.
        path: Destination directory.
        overwrite: Replace an existing file or directory at ``path`` when
            ``True``. The default raises :class:`FileExistsError`.

    Raises:
        ValueError: If one or more required frame keys are missing.
        FileExistsError: If ``path`` exists and ``overwrite`` is ``False``.

    Notes:
        Each frame is collected during the write. The function writes to a
        temporary sibling directory first and renames it into place only after
        all files have been produced, which prevents partial output directories
        on failed writes.

    Examples:
        >>> frames = read_frames("source-log")
        >>> write_frames(frames, "copy-log", overwrite=True)
    """
    missing = [name for name in _FRAME_NAMES if name not in frames]
    if missing:
        raise ValueError(f"write_frames is missing tables: {missing}")

    base = Path(path)
    if base.exists() and not overwrite:
        raise FileExistsError(
            f"Target already exists: {base}. Pass overwrite=True to replace it."
        )

    staging = base.with_name(f"{base.name}.tmp-{uuid4().hex}")
    staging.mkdir(parents=True)
    try:
        _write_partitioned(
            frames["events"], staging / "events", _EVENT_CORE, (s.OCEL_TIME,)
        )
        _write_partitioned(
            frames["objects"], staging / "objects", _OBJECT_CORE, (s.OCEL_ID,)
        )
        _write_partitioned(
            frames["object_changes"],
            staging / "object_changes",
            _CHANGE_CORE,
            (s.OCEL_ID, s.OCEL_TIME),
        )
        _write_relation(
            frames["e2o"],
            staging / "event_object.parquet",
            _E2O_COLS,
            (s.OCEL_OBJECT_ID, s.OCEL_EVENT_ID),
        )
        _write_relation(
            frames["o2o"],
            staging / "object_object.parquet",
            _O2O_COLS,
            (s.OCEL_SOURCE_ID, s.OCEL_TARGET_ID),
            skip_if_empty=True,
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
) -> None:
    base_dir.mkdir(parents=True)
    for type_name in _distinct_types(frame):
        df = (
            frame.filter(pl.col(s.OCEL_TYPE) == type_name)
            .drop(s.OCEL_TYPE)
            .sort(*sort_by)
            .collect()
        )
        df = _drop_all_null_attributes(df, core)
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
