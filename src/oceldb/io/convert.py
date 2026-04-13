"""Convert strict OCEL 2.0 SQLite files into the canonical oceldb directory layout."""

from __future__ import annotations

import csv
import sqlite3
import shutil
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

from oceldb.core.manifest import OCELManifest, TableSchema
from oceldb.io._manifest import CORE_COLUMNS, STORAGE_VERSION, write_manifest

try:
    __version__ = version("oceldb")
except PackageNotFoundError:
    __version__ = "unknown"

_SOURCE_SCHEMA = "ocel_source"
_CSV_NULL_TOKEN = "__oceldb_null__"
_EVENT_BASE_COLUMNS = {"ocel_id", "ocel_type", "ocel_time"}
_OBJECT_BASE_COLUMNS = {"ocel_id", "ocel_type", "ocel_time", "ocel_changed_field"}


def convert_sqlite(
    source: str | Path,
    target: str | Path,
    *,
    overwrite: bool = False,
) -> Path:
    """
    Convert a strict OCEL 2.0 SQLite database into the canonical oceldb directory layout.

    Args:
        source: Path to the strict OCEL 2.0 SQLite source file.
        target: Target directory for the converted dataset.
        overwrite: Replace an existing target directory.

    Returns:
        The resolved target directory path.
    """
    source_path = Path(source).expanduser().resolve()
    target_path = Path(target).expanduser().resolve()

    if not source_path.exists():
        raise FileNotFoundError(f"Source SQLite file not found: {source_path}")

    if not source_path.is_file():
        raise FileNotFoundError(f"Source must be a SQLite file: {source_path}")

    if target_path.exists():
        if not overwrite:
            raise FileExistsError(
                f"Target already exists: {target_path} (use overwrite=True)"
            )
        _remove_target(target_path)

    _convert_to_directory(source_path, target_path)

    return target_path


def _convert_to_directory(source_path: Path, target_dir: Path) -> None:
    target_dir.mkdir(parents=True, exist_ok=False)

    try:
        with sqlite3.connect(source_path) as sqlite_con, duckdb.connect() as con:
            event_types = _fetch_type_rows(sqlite_con, "event_map_type")
            object_types = _fetch_type_rows(sqlite_con, "object_map_type")
            source_schema = _source_schema_map(
                sqlite_con,
                event_types=event_types,
                object_types=object_types,
            )

            if not _attach_sqlite_source(con, source_path):
                _stage_source_tables(sqlite_con, con, source_schema)

            event_schema = _resolve_custom_schema(source_schema, event_types, kind="event")
            object_schema = _resolve_custom_schema(source_schema, object_types, kind="object")

            _copy_relation_table(con, "event_object", target_dir / "event_object.parquet")
            _copy_relation_table(
                con,
                "object_object",
                target_dir / "object_object.parquet",
            )

            _export_entities(
                con,
                target_file=target_dir / "event.parquet",
                base_table="event",
                base_alias="e",
                payload_prefix="event",
                type_rows=event_types,
                custom_schema=event_schema,
                base_columns=_EVENT_BASE_COLUMNS,
            )
            _export_entities(
                con,
                target_file=target_dir / "object_change.parquet",
                base_table="object",
                base_alias="o",
                payload_prefix="object",
                type_rows=object_types,
                custom_schema=object_schema,
                base_columns=_OBJECT_BASE_COLUMNS,
            )
            _export_object_identities(
                con,
                target_file=target_dir / "object.parquet",
            )

        manifest = OCELManifest(
            oceldb_version=__version__,
            storage_version=STORAGE_VERSION,
            source=source_path.name,
            created_at=datetime.now(timezone.utc),
            tables={
                "event": TableSchema(
                    name="event",
                    core_columns=CORE_COLUMNS["event"],
                    custom_columns=event_schema,
                ),
                "object": TableSchema(
                    name="object",
                    core_columns=CORE_COLUMNS["object"],
                ),
                "object_change": TableSchema(
                    name="object_change",
                    core_columns=CORE_COLUMNS["object_change"],
                    custom_columns=object_schema,
                ),
                "event_object": TableSchema(
                    name="event_object",
                    core_columns=CORE_COLUMNS["event_object"],
                ),
                "object_object": TableSchema(
                    name="object_object",
                    core_columns=CORE_COLUMNS["object_object"],
                ),
            },
        )
        write_manifest(target_dir / "manifest.json", manifest)
    except Exception:
        if target_dir.exists():
            _remove_target(target_dir)
        raise


def _resolve_custom_schema(
    source_schema: dict[str, dict[str, str]],
    type_rows: list[tuple[str, str]],
    *,
    kind: str,
) -> dict[str, str]:
    discovered: dict[str, list[str]] = {}

    for _, type_map in type_rows:
        table_name = f'{kind}_{type_map}'
        payload_columns = source_schema[table_name]
        for name, sql_type in payload_columns.items():
            if name in _base_columns_for_kind(kind):
                continue
            discovered.setdefault(name, []).append(_normalize_type(sql_type))

    return {
        name: _resolve_common_type(types)
        for name, types in sorted(discovered.items())
    }


def _export_entities(
    con: duckdb.DuckDBPyConnection,
    *,
    target_file: Path,
    base_table: str,
    base_alias: str,
    payload_prefix: str,
    type_rows: list[tuple[str, str]],
    custom_schema: dict[str, str],
    base_columns: set[str],
) -> None:
    queries = [
        _build_entity_select(
            con,
            base_table=base_table,
            base_alias=base_alias,
            payload_table=f"{payload_prefix}_{type_map}",
            type_name=type_name,
            custom_schema=custom_schema,
            base_columns=base_columns,
        )
        for type_name, type_map in type_rows
    ]

    if queries:
        sql = " UNION ALL ".join(queries)
    else:
        null_columns = ", ".join(
            f"CAST(NULL AS {sql_type}) AS \"{name}\""
            for name, sql_type in custom_schema.items()
        )
        sql = f"""
            SELECT
                CAST(NULL AS VARCHAR) AS ocel_id,
                CAST(NULL AS VARCHAR) AS ocel_type,
                CAST(NULL AS TIMESTAMP) AS ocel_time
                {", CAST(NULL AS VARCHAR) AS ocel_changed_field" if base_table == "object" else ""}
                {f", {null_columns}" if null_columns else ""}
            WHERE FALSE
        """

    target = str(target_file).replace("'", "''")
    con.execute(f"COPY ({sql}) TO '{target}' (FORMAT PARQUET)")


def _build_entity_select(
    con: duckdb.DuckDBPyConnection,
    *,
    base_table: str,
    base_alias: str,
    payload_table: str,
    type_name: str,
    custom_schema: dict[str, str],
    base_columns: set[str],
) -> str:
    payload_columns = _table_columns(con, payload_table)
    escaped_type = type_name.replace("'", "''")

    select_parts = [
        f'{base_alias}."ocel_id" AS ocel_id',
        f'{base_alias}."ocel_type" AS ocel_type',
        _temporal_column_sql(payload_columns, "ocel_time"),
    ]

    if base_table == "object":
        if "ocel_changed_field" in payload_columns:
            select_parts.append('p."ocel_changed_field" AS ocel_changed_field')
        else:
            select_parts.append('CAST(NULL AS VARCHAR) AS ocel_changed_field')

    for name, resolved_type in custom_schema.items():
        if name in payload_columns and name not in base_columns:
            select_parts.append(
                f'TRY_CAST(p."{name}" AS {resolved_type}) AS "{name}"'
            )
        else:
            select_parts.append(f'CAST(NULL AS {resolved_type}) AS "{name}"')

    select_sql = ",\n            ".join(select_parts)

    return f"""
        SELECT
            {select_sql}
        FROM {_qualified_source_table(base_table)} {base_alias}
        LEFT JOIN {_qualified_source_table(payload_table)} p
          ON {base_alias}."ocel_id" = p."ocel_id"
        WHERE {base_alias}."ocel_type" = '{escaped_type}'
    """


def _copy_relation_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    target_file: Path,
) -> None:
    target = str(target_file).replace("'", "''")
    con.execute(f"""
        COPY (
            SELECT *
            FROM {_qualified_source_table(table_name)}
        ) TO '{target}' (FORMAT PARQUET)
    """)


def _export_object_identities(
    con: duckdb.DuckDBPyConnection,
    *,
    target_file: Path,
) -> None:
    target = str(target_file).replace("'", "''")
    con.execute(f"""
        COPY (
            SELECT DISTINCT
                o."ocel_id" AS "ocel_id",
                o."ocel_type" AS "ocel_type"
            FROM {_qualified_source_table("object")} o
        ) TO '{target}' (FORMAT PARQUET)
    """)


def _table_columns(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
) -> dict[str, str]:
    return {
        row[0]: row[1]
        for row in con.execute(
            f"DESCRIBE SELECT * FROM {_qualified_source_table(table_name)}"
        ).fetchall()
    }


def _fetch_type_rows(
    sqlite_con: sqlite3.Connection,
    table_name: str,
) -> list[tuple[str, str]]:
    rows = sqlite_con.execute(f"""
        SELECT ocel_type, ocel_type_map
        FROM {_quote_identifier(table_name)}
        ORDER BY ocel_type
    """).fetchall()
    return [(str(type_name), str(type_map)) for type_name, type_map in rows]


def _source_schema_map(
    sqlite_con: sqlite3.Connection,
    *,
    event_types: list[tuple[str, str]],
    object_types: list[tuple[str, str]],
) -> dict[str, dict[str, str]]:
    return {
        table_name: _sqlite_table_columns(sqlite_con, table_name)
        for table_name in _required_source_tables(event_types, object_types)
    }


def _required_source_tables(
    event_types: list[tuple[str, str]],
    object_types: list[tuple[str, str]],
) -> list[str]:
    return [
        "event",
        "object",
        "event_object",
        "object_object",
        *[f"event_{type_map}" for _, type_map in event_types],
        *[f"object_{type_map}" for _, type_map in object_types],
    ]


def _attach_sqlite_source(
    con: duckdb.DuckDBPyConnection,
    source_path: Path,
) -> bool:
    for extension_name in ("sqlite", "sqlite_scanner"):
        try:
            con.execute(f"LOAD {extension_name}")
            # SQLite payload tables may contain values that do not match their
            # declared type exactly. Read everything as VARCHAR first, then let
            # the converter normalize columns explicitly via TRY_CAST.
            con.execute("SET GLOBAL sqlite_all_varchar = true")
            escaped_source = str(source_path).replace("'", "''")
            con.execute(
                f"ATTACH '{escaped_source}' AS {_SOURCE_SCHEMA} "
                "(TYPE sqlite, READ_ONLY)"
            )
            return True
        except duckdb.Error:
            continue

    return False


def _stage_source_tables(
    sqlite_con: sqlite3.Connection,
    con: duckdb.DuckDBPyConnection,
    source_schema: dict[str, dict[str, str]],
) -> None:
    con.execute(f'CREATE SCHEMA "{_SOURCE_SCHEMA}"')

    with TemporaryDirectory(prefix="oceldb_sqlite_stage_") as tmpdir:
        staging_dir = Path(tmpdir)
        for table_name, columns in source_schema.items():
            staging_file = staging_dir / f"{table_name}.tsv"
            _write_sqlite_table_tsv(
                sqlite_con,
                table_name,
                columns=tuple(columns),
                target_file=staging_file,
            )
            _import_staged_table(con, table_name, staging_file)


def _write_sqlite_table_tsv(
    sqlite_con: sqlite3.Connection,
    table_name: str,
    *,
    columns: tuple[str, ...],
    target_file: Path,
) -> None:
    select_sql = f"SELECT * FROM {_quote_identifier(table_name)}"
    cursor = sqlite_con.execute(select_sql)

    with target_file.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(
            handle,
            delimiter="\t",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
        )
        writer.writerow(columns)
        while rows := cursor.fetchmany(50_000):
            writer.writerows(_staged_csv_rows(rows))


def _staged_csv_rows(rows: list[tuple[object, ...]]) -> list[tuple[object, ...]]:
    return [
        tuple(
            _CSV_NULL_TOKEN if value is None else _normalize_sqlite_value(value)
            for value in row
        )
        for row in rows
    ]


def _import_staged_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    source_file: Path,
) -> None:
    escaped_source = str(source_file).replace("'", "''")
    escaped_null = _CSV_NULL_TOKEN.replace("'", "''")
    con.execute(f"""
        CREATE TABLE {_qualified_source_table(table_name)} AS
        SELECT *
        FROM read_csv_auto(
            '{escaped_source}',
            delim = '\t',
            header = true,
            all_varchar = true,
            nullstr = '{escaped_null}'
        )
    """)


def _sqlite_table_columns(
    sqlite_con: sqlite3.Connection,
    table_name: str,
) -> dict[str, str]:
    rows = sqlite_con.execute(
        f"PRAGMA table_info({_quote_identifier(table_name)})"
    ).fetchall()
    if not rows:
        raise FileNotFoundError(f"Missing SQLite table: {table_name}")

    return {
        str(row[1]): str(row[2] or "VARCHAR")
        for row in rows
    }


def _normalize_sqlite_value(value: object) -> object:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _qualified_source_table(table_name: str) -> str:
    return f'{_quote_identifier(_SOURCE_SCHEMA)}.{_quote_identifier(table_name)}'


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _temporal_column_sql(payload_columns: dict[str, str], name: str) -> str:
    if name not in payload_columns:
        return f"CAST(NULL AS TIMESTAMP) AS {name}"
    return f'TRY_CAST(p."{name}" AS TIMESTAMP) AS {name}'


def _normalize_type(sql_type: str) -> str:
    upper = sql_type.upper()

    if any(token in upper for token in ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT")):
        return "BIGINT"
    if any(token in upper for token in ("REAL", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC")):
        return "DOUBLE"
    if "TIMESTAMP" in upper:
        return "TIMESTAMP"
    if upper == "DATE":
        return "DATE"
    if upper == "BOOLEAN":
        return "BOOLEAN"
    return "VARCHAR"


def _resolve_common_type(types: list[str]) -> str:
    unique = sorted(set(types))
    if len(unique) == 1:
        return unique[0]
    if set(unique) <= {"BIGINT", "DOUBLE"}:
        return "DOUBLE"
    if set(unique) <= {"DATE", "TIMESTAMP"}:
        return "TIMESTAMP"
    return "VARCHAR"


def _base_columns_for_kind(kind: str) -> set[str]:
    if kind == "event":
        return _EVENT_BASE_COLUMNS
    if kind == "object":
        return _OBJECT_BASE_COLUMNS
    raise TypeError(f"Unsupported entity kind: {kind!r}")


def _remove_target(target: Path) -> None:
    if target.is_dir():
        shutil.rmtree(target)
    else:
        target.unlink()
