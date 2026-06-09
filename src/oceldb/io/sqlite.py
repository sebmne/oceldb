"""SQLite SQL relations for OCEL 2.0 imports."""

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from oceldb.io.sql import quote_identifier, sql_string

if TYPE_CHECKING:
    import duckdb


@dataclass(frozen=True)
class _TypeMapping:
    type_name: str
    table_suffix: str


@dataclass(frozen=True)
class SqliteLog:
    """SQLite schema details needed to build canonical OCEL relations."""

    event_types: dict[str, dict[str, str]]
    object_types: dict[str, dict[str, str]]
    event_attr_cols: dict[str, list[tuple[str, str]]]
    object_attr_cols: dict[str, list[tuple[str, str]]]
    object_has_changed_field: dict[str, bool]
    event_suffix: dict[str, str]
    object_suffix: dict[str, str]


def attach_sqlite(con: "duckdb.DuckDBPyConnection", source: Path) -> SqliteLog:
    """Attach an OCEL 2.0 SQLite export and read its type mappings."""
    if not source.exists():
        raise FileNotFoundError(f"Source not found: {source}")

    con.execute("INSTALL sqlite; LOAD sqlite")
    con.execute("SET sqlite_all_varchar = true")
    con.execute(f"ATTACH {sql_string(str(source))} AS src (TYPE SQLITE, READ_ONLY)")

    event_mappings = _read_mapping(con, "event_map_type")
    object_mappings = _read_mapping(con, "object_map_type")

    event_types: dict[str, dict[str, str]] = {}
    event_attr_cols: dict[str, list[tuple[str, str]]] = {}
    for mapping in event_mappings:
        attr_cols = [
            (name, sqlite_type)
            for name, sqlite_type in _pragma_columns(
                source, f"event_{mapping.table_suffix}"
            )
            if name not in ("ocel_id", "ocel_time")
        ]
        event_attr_cols[mapping.type_name] = attr_cols
        event_types[mapping.type_name] = {
            name: _duckdb_type(sqlite_type) for name, sqlite_type in attr_cols
        }

    object_types: dict[str, dict[str, str]] = {}
    object_attr_cols: dict[str, list[tuple[str, str]]] = {}
    object_has_changed_field: dict[str, bool] = {}
    for mapping in object_mappings:
        cols = _pragma_columns(source, f"object_{mapping.table_suffix}")
        col_names = {name for name, _ in cols}
        object_has_changed_field[mapping.type_name] = "ocel_changed_field" in col_names
        attr_cols = [
            (name, sqlite_type)
            for name, sqlite_type in cols
            if name not in ("ocel_id", "ocel_time", "ocel_changed_field")
        ]
        object_attr_cols[mapping.type_name] = attr_cols
        object_types[mapping.type_name] = {
            name: _duckdb_type(sqlite_type) for name, sqlite_type in attr_cols
        }

    return SqliteLog(
        event_types=event_types,
        object_types=object_types,
        event_attr_cols=event_attr_cols,
        object_attr_cols=object_attr_cols,
        object_has_changed_field=object_has_changed_field,
        event_suffix={
            mapping.type_name: mapping.table_suffix for mapping in event_mappings
        },
        object_suffix={
            mapping.type_name: mapping.table_suffix for mapping in object_mappings
        },
    )


def events_relation(
    con: "duckdb.DuckDBPyConnection", log: SqliteLog, type_name: str
) -> "duckdb.DuckDBPyRelation":
    attrs = log.event_attr_cols[type_name]
    select = "ocel_id, TRY_CAST(ocel_time AS TIMESTAMP) AS ocel_time"
    for name, sqlite_type in attrs:
        select += f", {_cast_expr(name, sqlite_type)}"
    table = quote_identifier(f"event_{log.event_suffix[type_name]}")
    return con.sql(f"SELECT {select} FROM src.{table}")


def objects_relation(
    con: "duckdb.DuckDBPyConnection", type_name: str
) -> "duckdb.DuckDBPyRelation":
    return con.sql(
        f"SELECT ocel_id FROM src.object WHERE ocel_type = {sql_string(type_name)}"
    )


def object_changes_relation(
    con: "duckdb.DuckDBPyConnection", log: SqliteLog, type_name: str
) -> "duckdb.DuckDBPyRelation":
    attrs = log.object_attr_cols[type_name]
    if log.object_has_changed_field[type_name]:
        time_expr = (
            "CASE WHEN ocel_changed_field IS NULL "
            "THEN TIMESTAMP '1970-01-01 00:00:00' "
            "ELSE TRY_CAST(ocel_time AS TIMESTAMP) END AS ocel_time"
        )
        changed_field_expr = "ocel_changed_field"
    else:
        time_expr = "TIMESTAMP '1970-01-01 00:00:00' AS ocel_time"
        changed_field_expr = "CAST(NULL AS VARCHAR) AS ocel_changed_field"

    select = f"ocel_id, {time_expr}, {changed_field_expr}"
    for name, sqlite_type in attrs:
        select += f", {_cast_expr(name, sqlite_type)}"
    table = quote_identifier(f"object_{log.object_suffix[type_name]}")
    return con.sql(f"SELECT {select} FROM src.{table}")


def event_object_relation(
    con: "duckdb.DuckDBPyConnection",
) -> "duckdb.DuckDBPyRelation":
    return con.sql("""
        SELECT
            eo.ocel_event_id,
            e.ocel_type  AS ocel_event_type,
            eo.ocel_object_id,
            o.ocel_type  AS ocel_object_type,
            eo.ocel_qualifier
        FROM src.event_object eo
        JOIN src.event  e ON eo.ocel_event_id  = e.ocel_id
        JOIN src.object o ON eo.ocel_object_id = o.ocel_id
    """)


def object_object_relation(
    con: "duckdb.DuckDBPyConnection",
) -> "duckdb.DuckDBPyRelation | None":
    return _relation_if_nonempty(
        con,
        """
        SELECT
            oo.ocel_source_id,
            s.ocel_type AS ocel_source_type,
            oo.ocel_target_id,
            t.ocel_type AS ocel_target_type,
            oo.ocel_qualifier
        FROM src.object_object oo
        JOIN src.object s ON oo.ocel_source_id = s.ocel_id
        JOIN src.object t ON oo.ocel_target_id = t.ocel_id
    """,
    )


def _read_mapping(con: "duckdb.DuckDBPyConnection", table: str) -> list[_TypeMapping]:
    rows = con.execute(
        f"SELECT ocel_type, ocel_type_map FROM src.{quote_identifier(table)}"
    ).fetchall()
    return [_TypeMapping(type_name=row[0], table_suffix=row[1]) for row in rows]


def _pragma_columns(source: Path, table_name: str) -> list[tuple[str, str]]:
    # DuckDB hides declared types when sqlite_all_varchar is enabled.
    with sqlite3.connect(source) as sc:
        rows = sc.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    return [(row[1], row[2]) for row in rows]


def _relation_if_nonempty(
    con: "duckdb.DuckDBPyConnection", sql: str
) -> "duckdb.DuckDBPyRelation | None":
    row = con.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()
    if not row or int(row[0]) == 0:
        return None
    return con.sql(sql)


def _duckdb_type(sqlite_type: str) -> str:
    t = sqlite_type.upper()
    if "INT" in t:
        return "INTEGER"
    if any(k in t for k in ("REAL", "FLOAT", "DOUBLE", "NUMERIC", "DECIMAL")):
        return "DOUBLE"
    if "BOOL" in t:
        return "BOOLEAN"
    return "VARCHAR"


def _cast_expr(col: str, sqlite_type: str) -> str:
    duckdb_type = _duckdb_type(sqlite_type)
    identifier = quote_identifier(col)
    if duckdb_type == "VARCHAR":
        return identifier
    return f"TRY_CAST({identifier} AS {duckdb_type}) AS {identifier}"
