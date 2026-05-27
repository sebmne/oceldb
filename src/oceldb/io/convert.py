"""Convert OCEL sources to persisted logs."""

from __future__ import annotations

from pathlib import Path

from oceldb.io.resolve import resolve_source
from oceldb.io.writer import ProgressCallback, write_log


def convert_ocel(
    source: object,
    target: str | Path,
    *,
    format: str | None = None,
    overwrite: bool = False,
    compression: str = "zstd",
    progress: ProgressCallback | None = None,
) -> None:
    """Convert *source* to the oceldb Parquet layout at *target*."""
    resolved = resolve_source(source, format=format, progress=progress)
    write_log(
        Path(target),
        resolved.source,
        source_kind=resolved.kind,
        source_path=resolved.path,
        overwrite=overwrite,
        compression=compression,
        progress=progress,
    )
