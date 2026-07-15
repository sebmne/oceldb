"""Import OCEL 2.0 SQLite exports into oceldb's Parquet layout.

DuckDB attaches the SQLite log read-only and streams each per-type table into the
Hive-partitioned layout (see :mod:`oceldb.io.native`) via a per-type ``COPY``. This is
the only DuckDB-backed code in the core library; it is imported lazily so
``import oceldb`` stays pure-Polars.

Expected OCEL 2.0 SQLite schema: ``event``/``object`` master tables,
``event_map_type`` / ``object_map_type`` (type name -> table suffix), per-type
``event_<suffix>`` / ``object_<suffix>`` tables, and ``event_object`` /
``object_object`` relations. The cast logic mirrors the original battle-tested
converter (declared SQLite types -> DuckDB types, integers -> ``BIGINT``), and
the epoch-row trick puts initial object state at ``1970-01-01``.
"""

import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import AbstractSet

import duckdb
from oceldb.io._paths import DirectoryTransaction
from oceldb.io._schema import validate_frames_for_io
from oceldb.io.errors import ValidationMode, check_validation_mode, issue
from oceldb.io.native.layout import (
    COMPRESSION,
    EVENTS,
    EVENT_OBJECT,
    OBJECTS,
    OBJECT_CHANGES,
    OBJECT_OBJECT,
)
from oceldb.io.native.manifest import write_manifest
from oceldb.io.native.storage import infer_storage_schema, scan_native_tables

_EPOCH = "TIMESTAMPTZ '1970-01-01 00:00:00+00:00'"
_EVENT_CORE = set(EVENTS.columns)
_OBJECT_CHANGE_CORE = set(OBJECT_CHANGES.columns)

_E2O_QUERY = (
    "SELECT eo.ocel_event_id, e.ocel_type AS ocel_event_type, "
    "eo.ocel_object_id, o.ocel_type AS ocel_object_type, eo.ocel_qualifier "
    "FROM src.event_object eo "
    "JOIN src.event  e ON eo.ocel_event_id  = e.ocel_id "
    "JOIN src.object o ON eo.ocel_object_id = o.ocel_id "
    f"ORDER BY {', '.join(EVENT_OBJECT.sort_by)}"
)
_O2O_QUERY = (
    "SELECT oo.ocel_source_id, s.ocel_type AS ocel_source_type, "
    "oo.ocel_target_id, t.ocel_type AS ocel_target_type, oo.ocel_qualifier "
    "FROM src.object_object oo "
    "JOIN src.object s ON oo.ocel_source_id = s.ocel_id "
    "JOIN src.object t ON oo.ocel_target_id = t.ocel_id "
    f"ORDER BY {', '.join(OBJECT_OBJECT.sort_by)}"
)


def import_sqlite(
    source: str | Path,
    target: str | Path,
    *,
    overwrite: bool = False,
    validation: ValidationMode = "strict",
) -> None:
    """Import an OCEL 2.0 SQLite export into a native oceldb directory.

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
        >>> from oceldb.io import import_ocel
        >>> import_ocel("running-example.sqlite", "running-example")
    """
    validation = check_validation_mode(validation)
    source = Path(source)
    target = Path(target)
    if not source.exists():
        raise FileNotFoundError(f"Source not found: {source}")

    with DirectoryTransaction(target, overwrite=overwrite) as staging:
        con = duckdb.connect()
        try:
            con.execute("INSTALL sqlite; LOAD sqlite")
            con.execute("SET sqlite_all_varchar = true")
            con.execute(
                f"ATTACH {_sql_string(str(source))} AS src (TYPE SQLITE, READ_ONLY)"
            )
            cast_function = "CAST" if validation == "strict" else "TRY_CAST"
            _write_events(con, source, staging, cast_function, validation)
            _write_objects_and_changes(con, source, staging, cast_function, validation)
            _validate_relation_joins(con, source, validation)
            _copy(con, _E2O_QUERY, EVENT_OBJECT.file(staging))
            if "object_object" in _table_names(source) and _count(con, "object_object"):
                _copy(con, _O2O_QUERY, OBJECT_OBJECT.file(staging))
            schema = infer_storage_schema(staging)
            validate_frames_for_io(scan_native_tables(staging, schema), validation)
            write_manifest(staging, schema=schema)
        finally:
            con.close()


@dataclass(frozen=True)
class _Mapping:
    type_name: str
    suffix: str


def _write_events(
    con: duckdb.DuckDBPyConnection,
    source: Path,
    staging: Path,
    cast_function: str,
    validation: ValidationMode,
) -> None:
    base = EVENTS.root(staging)
    base.mkdir()
    mappings = _mappings(con, "event_map_type")
    per_type = {
        mapping.suffix: _attribute_columns(
            source, f"event_{mapping.suffix}", _EVENT_CORE
        )
        for mapping in mappings
    }
    shared = _widened_duckdb_types(per_type.values())
    for mapping in mappings:
        attrs = per_type[mapping.suffix]
        table_name = f"event_{mapping.suffix}"
        _check_cast(con, table_name, "ocel_time", "TIMESTAMPTZ", validation)
        for name, _ in attrs:
            _check_cast(con, table_name, name, shared[name], validation)
        columns = [
            "ocel_id",
            f"{cast_function}(ocel_time AS TIMESTAMPTZ) AS ocel_time",
            *(_cast_expr(name, shared[name], cast_function) for name, _ in attrs),
        ]
        output = EVENTS.file(staging, type_name=mapping.type_name)
        output.parent.mkdir()
        _copy(
            con,
            f"SELECT {', '.join(columns)} "
            f"FROM src.{_quote('event_' + mapping.suffix)} "
            f"ORDER BY {', '.join(EVENTS.sort_by)}",
            output,
        )


def _write_objects_and_changes(
    con: duckdb.DuckDBPyConnection,
    source: Path,
    staging: Path,
    cast_function: str,
    validation: ValidationMode,
) -> None:
    objects_base = OBJECTS.root(staging)
    objects_base.mkdir()
    changes_base = OBJECT_CHANGES.root(staging)
    changes_base.mkdir()

    mappings = _mappings(con, "object_map_type")
    shared = _widened_duckdb_types(
        [
            (name, sqlite_type)
            for name, sqlite_type in _pragma_columns(source, f"object_{mapping.suffix}")
            if name not in _OBJECT_CHANGE_CORE
        ]
        for mapping in mappings
    )
    for mapping in mappings:
        object_file = OBJECTS.file(staging, type_name=mapping.type_name)
        object_file.parent.mkdir()
        _copy(
            con,
            f"SELECT ocel_id FROM src.object "
            f"WHERE ocel_type = {_sql_string(mapping.type_name)} "
            f"ORDER BY {', '.join(OBJECTS.sort_by)}",
            object_file,
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
            _check_cast(
                con,
                table,
                "ocel_time",
                "TIMESTAMPTZ",
                validation,
                condition="ocel_changed_field IS NOT NULL",
            )
            time_expr = (
                "CASE WHEN ocel_changed_field IS NULL THEN "
                f"{_EPOCH} ELSE {cast_function}(ocel_time AS TIMESTAMPTZ) "
                "END AS ocel_time"
            )
            changed_expr = "ocel_changed_field"
        else:
            time_expr = f"{_EPOCH} AS ocel_time"
            changed_expr = "CAST(NULL AS VARCHAR) AS ocel_changed_field"
        columns = [
            "ocel_id",
            time_expr,
            changed_expr,
            *(_cast_expr(name, shared[name], cast_function) for name, _ in attrs),
        ]
        for name, _ in attrs:
            _check_cast(con, table, name, shared[name], validation)
        changes_file = OBJECT_CHANGES.file(staging, type_name=mapping.type_name)
        changes_file.parent.mkdir()
        _copy(
            con,
            f"SELECT {', '.join(columns)} "
            f"FROM src.{_quote(table)} "
            f"ORDER BY {', '.join(OBJECT_CHANGES.sort_by)}",
            changes_file,
        )


def _copy(con: duckdb.DuckDBPyConnection, query: str, path: Path) -> None:
    con.execute(
        f"COPY ({query}) TO {_sql_string(str(path))} "
        f"(FORMAT PARQUET, COMPRESSION {COMPRESSION.upper()})"
    )


def _mappings(con: duckdb.DuckDBPyConnection, table: str) -> list[_Mapping]:
    rows = con.execute(
        f"SELECT ocel_type, ocel_type_map FROM src.{_quote(table)} ORDER BY ocel_type"
    ).fetchall()
    return [_Mapping(str(row[0]), str(row[1])) for row in rows]


def _count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    row = con.execute(f"SELECT COUNT(*) FROM src.{_quote(table)}").fetchone()
    return int(row[0]) if row else 0


def _validate_relation_joins(
    con: duckdb.DuckDBPyConnection,
    source: Path,
    validation: ValidationMode,
) -> None:
    if validation == "none":
        return
    e2o_rows = _count(con, "event_object")
    joined_e2o = _query_count(con, _E2O_QUERY)
    if joined_e2o != e2o_rows:
        issue(
            validation,
            f"SQLite event_object contains {e2o_rows - joined_e2o} dangling relation(s).",
        )
    if "object_object" in _table_names(source):
        o2o_rows = _count(con, "object_object")
        joined_o2o = _query_count(con, _O2O_QUERY)
        if joined_o2o != o2o_rows:
            issue(
                validation,
                f"SQLite object_object contains {o2o_rows - joined_o2o} dangling relation(s).",
            )


def _query_count(con: duckdb.DuckDBPyConnection, query: str) -> int:
    row = con.execute(f"SELECT COUNT(*) FROM ({query}) AS checked").fetchone()
    return int(row[0]) if row else 0


def _table_names(source: Path) -> set[str]:
    with sqlite3.connect(source) as connection:
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    return {str(row[0]) for row in rows}


def _attribute_columns(
    source: Path, table: str, core: AbstractSet[str]
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


def _cast_expr(column: str, duckdb_type: str, cast_function: str) -> str:
    identifier = _quote(column)
    if duckdb_type == "VARCHAR":
        return identifier
    return f"{cast_function}({identifier} AS {duckdb_type}) AS {identifier}"


def _check_cast(
    con: duckdb.DuckDBPyConnection,
    table: str,
    column: str,
    target_type: str,
    validation: ValidationMode,
    *,
    condition: str | None = None,
) -> None:
    if validation == "none" or target_type == "VARCHAR":
        return
    identifier = _quote(column)
    predicates = [
        f"{identifier} IS NOT NULL",
        f"TRY_CAST({identifier} AS {target_type}) IS NULL",
    ]
    if condition is not None:
        predicates.append(condition)
    row = con.execute(
        f"SELECT COUNT(*) FROM src.{_quote(table)} WHERE {' AND '.join(predicates)}"
    ).fetchone()
    invalid = int(row[0]) if row else 0
    if invalid:
        issue(
            validation,
            f"SQLite {table}.{column} contains {invalid} value(s) that cannot "
            f"be converted to {target_type}.",
        )


def _widened_duckdb_types(
    groups: Iterable[Iterable[tuple[str, str]]],
) -> dict[str, str]:
    """Give attributes shared across per-type tables one physical DuckDB type.

    The native layout unions each partitioned table, so a column reused by
    several types must agree on its dtype. Numeric conflicts widen to DOUBLE,
    anything else to VARCHAR.
    """
    shared: dict[str, str] = {}
    for attrs in groups:
        for name, sqlite_type in attrs:
            duckdb_type = _duckdb_type(sqlite_type)
            previous = shared.get(name)
            if previous is None or previous == duckdb_type:
                shared[name] = duckdb_type
            elif {previous, duckdb_type} <= {"BIGINT", "DOUBLE"}:
                shared[name] = "DOUBLE"
            else:
                shared[name] = "VARCHAR"
    return shared


def _duckdb_type(sqlite_type: str) -> str:
    upper = sqlite_type.upper()
    if any(key in upper for key in ("TIMESTAMP", "DATETIME", "DATE")):
        return "TIMESTAMPTZ"
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
