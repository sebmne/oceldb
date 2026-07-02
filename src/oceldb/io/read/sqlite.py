from pathlib import Path
from oceldb.io.convert_sqlite import convert_sqlite


from oceldb.ocel import OCEL
from oceldb.utils.cache import conversion_cache_dir


def read_sqlite(source: str | Path) -> OCEL:
    """Open an OCEL 2.0 SQLite export as an :class:`~oceldb.OCEL`.

    Converts the SQLite file to oceldb's native Hive-partitioned Parquet layout
    and returns an ``OCEL`` backed by those files. Unlike :func:`read_ocel_json`
    and :func:`read_ocel_xml`, the result is **file-backed and fully lazy** —
    no data is loaded into RAM until you call ``.collect()`` on a frame. This
    makes it suitable for logs that are too large to hold in memory.

    Args:
        source: Path to an OCEL 2.0 SQLite database.

    Returns:
        An ``OCEL`` backed by a cached native Parquet conversion of ``source``.

    Raises:
        FileNotFoundError: If ``source`` does not exist.
        duckdb.Error: If DuckDB cannot attach or query the SQLite database.
        sqlite3.Error: If SQLite schema inspection fails.

    Notes:
        The Parquet layout is cached on disk. The cache key includes the
        absolute source path, file size, and modification time, so re-reading
        an unchanged file skips the conversion. Use :func:`convert_sqlite`
        followed by :meth:`~oceldb.OCEL.read` if you want to control the cache
        location explicitly.

    Examples:
        >>> from oceldb.io import read_sqlite
        >>> ocel = read_sqlite("running-example.sqlite")
        >>> ocel.events().collect()
    """
    source_path = Path(source)
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    target_dir = conversion_cache_dir(source_path, name="read_sqlite")
    if not target_dir.exists():
        convert_sqlite(source_path, target_dir, overwrite=True)
    return OCEL.read(target_dir)
