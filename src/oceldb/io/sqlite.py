"""Convert OCEL 2.0 SQLite exports to oceldb's Parquet layout.

DuckDB attaches the SQLite log read-only and streams each per-type table into the
Hive-partitioned layout (see :mod:`oceldb.store`) via a per-type ``COPY``. This is
the only DuckDB-backed code in the core library; it is imported lazily so
``import oceldb`` stays pure-Polars.

Expected OCEL 2.0 SQLite schema: ``event``/``object`` master tables,
``event_map_type`` / ``object_map_type`` (type name -> table suffix), per-type
``event_<suffix>`` / ``object_<suffix>`` tables, and ``event_object`` /
``object_object`` relations. The cast logic mirrors the original battle-tested
converter (declared SQLite types -> DuckDB types, integers -> ``BIGINT``), and
the epoch-row trick puts initial object state at ``1970-01-01``.
"""

import shutil
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import duckdb

from oceldb.ocel import OCEL
from oceldb.store import encode_type_name
from oceldb.utils.cache import conversion_cache_dir

_EPOCH = "TIMESTAMP '1970-01-01 00:00:00'"
_EVENT_CORE = {"ocel_id", "ocel_time"}
_OBJECT_CHANGE_CORE = {"ocel_id", "ocel_time", "ocel_changed_field"}

_E2O_QUERY = (
    "SELECT eo.ocel_event_id, e.ocel_type AS ocel_event_type, "
    "eo.ocel_object_id, o.ocel_type AS ocel_object_type, eo.ocel_qualifier "
    "FROM src.event_object eo "
    "JOIN src.event  e ON eo.ocel_event_id  = e.ocel_id "
    "JOIN src.object o ON eo.ocel_object_id = o.ocel_id "
    "ORDER BY eo.ocel_object_id, eo.ocel_event_id"
)
_O2O_QUERY = (
    "SELECT oo.ocel_source_id, s.ocel_type AS ocel_source_type, "
    "oo.ocel_target_id, t.ocel_type AS ocel_target_type, oo.ocel_qualifier "
    "FROM src.object_object oo "
    "JOIN src.object s ON oo.ocel_source_id = s.ocel_id "
    "JOIN src.object t ON oo.ocel_target_id = t.ocel_id "
    "ORDER BY oo.ocel_source_id, oo.ocel_target_id"
)


def convert_sqlite(
    source: str | Path,
    target: str | Path,
    *,
    overwrite: bool = False,
) -> None:
    """Convert an OCEL 2.0 SQLite export to a native oceldb directory.

    Args:
        source: Path to an OCEL 2.0 SQLite database. Supported inputs are the
            standard OCEL 2.0 SQLite schema with ``event`` and ``object`` master
            tables, per-type event/object tables, and relation tables.
        target: Destination directory for the oceldb Parquet layout.
        overwrite: Replace an existing file or directory at ``target`` when
            ``True``. The default raises :class:`FileExistsError`.

    Raises:
        FileNotFoundError: If ``source`` does not exist.
        FileExistsError: If ``target`` exists and ``overwrite`` is ``False``.
        duckdb.Error: If DuckDB cannot attach or query the SQLite database.
        sqlite3.Error: If SQLite schema inspection fails.

    Notes:
        The conversion writes to a temporary sibling directory and renames it
        into place after all Parquet files have been produced. Integer columns
        are cast to DuckDB ``BIGINT`` to avoid 32-bit overflow in large logs.

    Examples:
        >>> from oceldb.io import convert_sqlite
        >>> convert_sqlite("running-example.sqlite", "running-example")
    """
    source = Path(source)
    target = Path(target)
    if not source.exists():
        raise FileNotFoundError(f"Source not found: {source}")
    if target.exists() and not overwrite:
        raise FileExistsError(
            f"Target already exists: {target}. Pass overwrite=True to replace it."
        )

    staging = target.with_name(f"{target.name}.tmp-{uuid4().hex}")
    staging.mkdir(parents=True)
    try:
        con = duckdb.connect()
        try:
            con.execute("INSTALL sqlite; LOAD sqlite")
            con.execute("SET sqlite_all_varchar = true")
            con.execute(
                f"ATTACH {_sql_string(str(source))} AS src (TYPE SQLITE, READ_ONLY)"
            )
            _write_events(con, source, staging)
            _write_objects_and_changes(con, source, staging)
            _copy(con, _E2O_QUERY, staging / "event_object.parquet")
            if "object_object" in _table_names(source) and _count(con, "object_object"):
                _copy(con, _O2O_QUERY, staging / "object_object.parquet")
        finally:
            con.close()
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise

    if target.is_dir():
        shutil.rmtree(target)
    elif target.exists():
        target.unlink()
    staging.rename(target)


def read_sqlite(source: str | Path) -> OCEL:
    """Open an OCEL 2.0 SQLite export as an :class:`oceldb.OCEL`.

    Args:
        source: Path to an OCEL 2.0 SQLite database.

    Returns:
        An ``OCEL`` backed by a cached native Parquet conversion of ``source``.

    Raises:
        FileNotFoundError: If ``source`` does not exist.
        duckdb.Error: If DuckDB cannot attach or query the SQLite database.
        sqlite3.Error: If SQLite schema inspection fails.

    Notes:
        The cache key includes the absolute source path, file size, and
        modification time. Re-reading an unchanged file reuses the cached
        conversion; changing the SQLite file creates a new cache entry.

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


@dataclass(frozen=True)
class _Mapping:
    type_name: str
    suffix: str


def _write_events(con: duckdb.DuckDBPyConnection, source: Path, staging: Path) -> None:
    base = staging / "events"
    base.mkdir()
    for mapping in _mappings(con, "event_map_type"):
        attrs = _attribute_columns(source, f"event_{mapping.suffix}", _EVENT_CORE)
        columns = [
            "ocel_id",
            "TRY_CAST(ocel_time AS TIMESTAMP) AS ocel_time",
            *(_cast_expr(name, sqlite_type) for name, sqlite_type in attrs),
        ]
        out_dir = base / f"ocel_type={encode_type_name(mapping.type_name)}"
        out_dir.mkdir()
        _copy(
            con,
            f"SELECT {', '.join(columns)} "
            f"FROM src.{_quote('event_' + mapping.suffix)} ORDER BY ocel_time",
            out_dir / "data.parquet",
        )


def _write_objects_and_changes(
    con: duckdb.DuckDBPyConnection, source: Path, staging: Path
) -> None:
    objects_base = staging / "objects"
    objects_base.mkdir()
    changes_base = staging / "object_changes"
    changes_base.mkdir()

    for mapping in _mappings(con, "object_map_type"):
        encoded = encode_type_name(mapping.type_name)

        obj_dir = objects_base / f"ocel_type={encoded}"
        obj_dir.mkdir()
        _copy(
            con,
            f"SELECT ocel_id FROM src.object "
            f"WHERE ocel_type = {_sql_string(mapping.type_name)} ORDER BY ocel_id",
            obj_dir / "data.parquet",
        )

        table = f"object_{mapping.suffix}"
        columns_info = _pragma_columns(source, table)
        names = {name for name, _ in columns_info}
        attrs = [
            (name, sqlite_type)
            for name, sqlite_type in columns_info
            if name not in _OBJECT_CHANGE_CORE
        ]
        if "ocel_changed_field" in names:
            time_expr = (
                "CASE WHEN ocel_changed_field IS NULL THEN "
                f"{_EPOCH} ELSE TRY_CAST(ocel_time AS TIMESTAMP) END AS ocel_time"
            )
            changed_expr = "ocel_changed_field"
        else:
            time_expr = f"{_EPOCH} AS ocel_time"
            changed_expr = "CAST(NULL AS VARCHAR) AS ocel_changed_field"
        columns = [
            "ocel_id",
            time_expr,
            changed_expr,
            *(_cast_expr(name, sqlite_type) for name, sqlite_type in attrs),
        ]
        ch_dir = changes_base / f"ocel_type={encoded}"
        ch_dir.mkdir()
        _copy(
            con,
            f"SELECT {', '.join(columns)} "
            f"FROM src.{_quote(table)} ORDER BY ocel_id, ocel_time",
            ch_dir / "data.parquet",
        )


def _copy(con: duckdb.DuckDBPyConnection, query: str, path: Path) -> None:
    con.execute(
        f"COPY ({query}) TO {_sql_string(str(path))} (FORMAT PARQUET, COMPRESSION ZSTD)"
    )


def _mappings(con: duckdb.DuckDBPyConnection, table: str) -> list[_Mapping]:
    rows = con.execute(
        f"SELECT ocel_type, ocel_type_map FROM src.{_quote(table)} ORDER BY ocel_type"
    ).fetchall()
    return [_Mapping(str(row[0]), str(row[1])) for row in rows]


def _count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    row = con.execute(f"SELECT COUNT(*) FROM src.{_quote(table)}").fetchone()
    return int(row[0]) if row else 0


def _table_names(source: Path) -> set[str]:
    with sqlite3.connect(source) as connection:
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    return {str(row[0]) for row in rows}


def _attribute_columns(
    source: Path, table: str, core: set[str]
) -> list[tuple[str, str]]:
    return [
        (name, sqlite_type)
        for name, sqlite_type in _pragma_columns(source, table)
        if name not in core
    ]


def _pragma_columns(source: Path, table: str) -> list[tuple[str, str]]:
    # DuckDB hides declared column types under sqlite_all_varchar, so read the
    # schema straight from SQLite to drive the casts.
    with sqlite3.connect(source) as connection:
        rows = connection.execute(f"PRAGMA table_info({_quote(table)})").fetchall()
    return [(str(row[1]), str(row[2])) for row in rows]


def _cast_expr(column: str, sqlite_type: str) -> str:
    duckdb_type = _duckdb_type(sqlite_type)
    identifier = _quote(column)
    if duckdb_type == "VARCHAR":
        return identifier
    return f"TRY_CAST({identifier} AS {duckdb_type}) AS {identifier}"


def _duckdb_type(sqlite_type: str) -> str:
    upper = sqlite_type.upper()
    if "INT" in upper:
        # int64: matches the existing converted logs and avoids 32-bit overflow.
        return "BIGINT"
    if any(key in upper for key in ("REAL", "FLOAT", "DOUBLE", "NUMERIC", "DECIMAL")):
        return "DOUBLE"
    if "BOOL" in upper:
        return "BOOLEAN"
    return "VARCHAR"


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
