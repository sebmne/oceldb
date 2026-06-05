"""Convert OCEL sources to persisted logs."""

from __future__ import annotations

from pathlib import Path

from oceldb.io.writer import ProgressCallback, write_sqlite_log

_SQLITE_EXTENSIONS = {".db", ".sqlite", ".sqlite3"}


def convert_ocel(
    source: str | Path,
    target: str | Path,
    *,
    overwrite: bool = False,
    progress: ProgressCallback | None = None,
) -> None:
    """Convert an OCEL 2.0 SQLite export to the oceldb Parquet layout."""
    source_path = Path(source)
    if source_path.suffix.casefold() not in _SQLITE_EXTENSIONS:
        supported = ", ".join(sorted(_SQLITE_EXTENSIONS))
        raise ValueError(f"oceldb imports SQLite OCEL files only ({supported}).")

    if progress is not None:
        progress("Reading SQLite")

    write_sqlite_log(
        Path(target),
        source_path,
        overwrite=overwrite,
        progress=progress,
    )
