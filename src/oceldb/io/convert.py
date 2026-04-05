"""Convert strict OCEL 2.0 SQLite files into the oceldb parquet directory format."""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Literal

import duckdb

try:
    __version__ = version("oceldb")
except PackageNotFoundError:
    __version__ = "unknown"


def convert_sqlite(
    source: str | Path,
    target: str | Path,
    *,
    overwrite: bool = False,
) -> Path:
    source = Path(source).expanduser().resolve()
    target = Path(target).expanduser().resolve()

    if not source.exists():
        raise FileNotFoundError(f"Source SQLite file not found: {source}")

    if not source.is_file():
        raise FileNotFoundError(f"Source must be a SQLite file: {source}")

    if target.exists():
        if not overwrite:
            raise FileExistsError(
                f"Target directory already exists: {target} (use overwrite=True)"
            )
        shutil.rmtree(target)

    target.mkdir(parents=True)

    with duckdb.connect() as con:
        con.execute("INSTALL sqlite; LOAD sqlite;")
        con.execute("SET sqlite_all_varchar=true")
        src = str(source).replace("'", "''")
        con.execute(f"ATTACH '{src}' AS db (TYPE sqlite, READ_ONLY)")

        event_types = con.execute("""
            SELECT ocel_type, ocel_type_map
            FROM db.event_map_type
        """).fetchall()

        object_types = con.execute("""
            SELECT ocel_type, ocel_type_map
            FROM db.object_map_type
        """).fetchall()

        con.execute(f"""
            COPY (
                SELECT ocel_type
                FROM db.event_map_type
            ) TO '{str(target / "event_type.parquet").replace("'", "''")}' (FORMAT PARQUET)
        """)

        con.execute(f"""
            COPY (
                SELECT ocel_type
                FROM db.object_map_type
            ) TO '{str(target / "object_type.parquet").replace("'", "''")}' (FORMAT PARQUET)
        """)

        con.execute(f"""
            COPY (
                SELECT *
                FROM db.event_object
            ) TO '{str(target / "event_object.parquet").replace("'", "''")}' (FORMAT PARQUET)
        """)

        con.execute(f"""
            COPY (
                SELECT *
                FROM db.object_object
            ) TO '{str(target / "object_object.parquet").replace("'", "''")}' (FORMAT PARQUET)
        """)

        _export_events(con, event_types, target / "event.parquet")
        _export_objects(con, object_types, target / "object.parquet")

    (target / "metadata.json").write_text(
        json.dumps(
            {
                "oceldb_version": __version__,
                "source": source.name,
                "converted_at": datetime.now(timezone.utc).isoformat(),
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    return target


def _export_events(
    con: duckdb.DuckDBPyConnection,
    event_types: list[tuple[str, str]],
    target_file: Path,
) -> None:
    queries = [
        _build_payload_select(
            con=con,
            type_name=type_name,
            table_name=f"event_{type_map}",
            kind="event",
        )
        for type_name, type_map in event_types
    ]

    if queries:
        sql = " UNION ALL ".join(queries)
    else:
        sql = """
            SELECT
                NULL AS ocel_id,
                NULL AS ocel_type,
                NULL AS ocel_time,
                CAST('{}' AS VARCHAR) AS attributes
            WHERE FALSE
        """

    trgt = str(target_file).replace("'", "''")
    con.execute(f"COPY ({sql}) TO '{trgt}' (FORMAT PARQUET)")


def _export_objects(
    con: duckdb.DuckDBPyConnection,
    object_types: list[tuple[str, str]],
    target_file: Path,
) -> None:
    queries: list[str] = []
    payload_ids: list[str] = []

    for type_name, type_map in object_types:
        table_name = f"object_{type_map}"
        queries.append(
            _build_payload_select(
                con=con,
                type_name=type_name,
                table_name=table_name,
                kind="object",
            )
        )
        payload_ids.append(f'SELECT "ocel_id" FROM db."{table_name}"')

    queries.append(_build_ghost_objects_select(payload_ids))
    sql = " UNION ALL ".join(queries)

    trgt = str(target_file).replace("'", "''")
    con.execute(f"COPY ({sql}) TO '{trgt}' (FORMAT PARQUET)")


def _build_payload_select(
    con: duckdb.DuckDBPyConnection,
    type_name: str,
    table_name: str,
    kind: Literal["event", "object"],
) -> str:
    columns = [
        row[0]
        for row in con.execute(f'DESCRIBE SELECT * FROM db."{table_name}"').fetchall()
    ]

    attribute_columns = [
        col
        for col in columns
        if col not in {"ocel_id", "ocel_type", "ocel_time", "ocel_changed_field"}
    ]

    if attribute_columns:
        parts = [f"'{col}': \"{col}\"" for col in attribute_columns]
        attributes_sql = f"CAST(to_json({{{', '.join(parts)}}}) AS VARCHAR)"
    else:
        attributes_sql = "CAST('{}' AS VARCHAR)"

    escaped_type = type_name.replace("'", "''")
    time_sql = (
        '"ocel_time" AS ocel_time' if "ocel_time" in columns else "NULL AS ocel_time"
    )

    if kind == "event":
        return f"""
            SELECT
                "ocel_id" AS ocel_id,
                '{escaped_type}' AS ocel_type,
                {time_sql},
                {attributes_sql} AS attributes
            FROM db."{table_name}"
        """

    changed_field_sql = (
        '"ocel_changed_field" AS ocel_changed_field'
        if "ocel_changed_field" in columns
        else "NULL AS ocel_changed_field"
    )

    return f"""
        SELECT
            "ocel_id" AS ocel_id,
            '{escaped_type}' AS ocel_type,
            {time_sql},
            {changed_field_sql},
            {attributes_sql} AS attributes
        FROM db."{table_name}"
    """


def _build_ghost_objects_select(payload_ids: list[str]) -> str:
    if payload_ids:
        known_ids = " UNION ALL ".join(payload_ids)
        predicate = f'"ocel_id" NOT IN ({known_ids})'
    else:
        predicate = "TRUE"

    return f"""
        SELECT
            "ocel_id" AS ocel_id,
            "ocel_type" AS ocel_type,
            NULL AS ocel_time,
            NULL AS ocel_changed_field,
            CAST('{{}}' AS VARCHAR) AS attributes
        FROM db.object
        WHERE {predicate}
    """
