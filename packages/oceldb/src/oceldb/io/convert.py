"""Convert strict OCEL 2.0 SQLite files into the oceldb parquet directory format."""

import json
import shutil
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

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
    """
    Convert a strict OCEL 2.0 SQLite database into the oceldb parquet format.

    The resulting target directory contains:
        - event.parquet
        - object.parquet
        - event_object.parquet
        - object_object.parquet
        - metadata.json

    Conversion semantics:
        - `event.parquet` is built from the authoritative `db.event` table and
          enriched with optional type-specific payload from `db.event_<map>`.
        - `object.parquet` is built from the authoritative `db.object` table and
          enriched with optional type-specific payload from `db.object_<map>`.
        - `event_object.parquet` and `object_object.parquet` are copied directly.
        - All declared event and object types are preserved implicitly through
          the exported `event.parquet` and `object.parquet`.

    Args:
        source: Path to the strict OCEL 2.0 SQLite source file.
        target: Target directory for the converted parquet-based OCEL.
        overwrite: Whether to remove an existing target directory first.

    Returns:
        The resolved target directory path.
    """
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

        event_object_target = str(target / "event_object.parquet").replace("'", "''")
        object_object_target = str(target / "object_object.parquet").replace("'", "''")

        con.execute(f"""
            COPY (
                SELECT *
                FROM db.event_object
            ) TO '{event_object_target}' (FORMAT PARQUET)
        """)

        con.execute(f"""
            COPY (
                SELECT *
                FROM db.object_object
            ) TO '{object_object_target}' (FORMAT PARQUET)
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
    """
    Export the unified event table.

    The exported table is built from the authoritative base `db.event` table and
    left-joined with the corresponding type-specific payload table for each
    declared event type.

    The resulting parquet schema is:
        - ocel_id
        - ocel_type
        - ocel_time
        - attributes

    Args:
        con: Open DuckDB connection with the SQLite database attached as `db`.
        event_types: Pairs of `(ocel_type, ocel_type_map)` from `db.event_map_type`.
        target_file: Output parquet file path.
    """
    queries = [
        _build_event_select(
            con=con,
            type_name=type_name,
            payload_table=f"event_{type_map}",
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
    """
    Export the unified object table.

    The exported table is built from the authoritative base `db.object` table and
    left-joined with the corresponding type-specific payload table for each
    declared object type.

    The resulting parquet schema is:
        - ocel_id
        - ocel_type
        - ocel_time
        - ocel_changed_field
        - attributes

    Objects without type-specific payload rows are still preserved because they
    exist in the base `db.object` table.

    Args:
        con: Open DuckDB connection with the SQLite database attached as `db`.
        object_types: Pairs of `(ocel_type, ocel_type_map)` from `db.object_map_type`.
        target_file: Output parquet file path.
    """
    queries = [
        _build_object_select(
            con=con,
            type_name=type_name,
            payload_table=f"object_{type_map}",
        )
        for type_name, type_map in object_types
    ]

    if queries:
        sql = " UNION ALL ".join(queries)
    else:
        sql = """
            SELECT
                NULL AS ocel_id,
                NULL AS ocel_type,
                NULL AS ocel_time,
                NULL AS ocel_changed_field,
                CAST('{}' AS VARCHAR) AS attributes
            WHERE FALSE
        """

    trgt = str(target_file).replace("'", "''")
    con.execute(f"COPY ({sql}) TO '{trgt}' (FORMAT PARQUET)")


def _build_event_select(
    con: duckdb.DuckDBPyConnection,
    type_name: str,
    payload_table: str,
) -> str:
    """
    Build the SELECT statement exporting one event type.

    Args:
        con: Open DuckDB connection with the SQLite database attached as `db`.
        type_name: The declared OCEL event type name.
        payload_table: The corresponding type-specific payload table name.

    Returns:
        A SQL SELECT statement returning event rows of the given type with
        optional event time and packed custom attributes.
    """
    payload_columns = _table_columns(con, payload_table)
    attribute_columns = [
        col
        for col in payload_columns
        if col not in {"ocel_id", "ocel_type", "ocel_time", "ocel_changed_field"}
    ]

    attributes_sql = _attributes_sql(attribute_columns, alias="p")
    escaped_type = type_name.replace("'", "''")

    time_sql = (
        'TRY_CAST(p."ocel_time" AS TIMESTAMP) AS ocel_time'
        if "ocel_time" in payload_columns
        else "CAST(NULL AS TIMESTAMP) AS ocel_time"
    )

    return f"""
        SELECT
            e."ocel_id" AS ocel_id,
            e."ocel_type" AS ocel_type,
            {time_sql},
            {attributes_sql} AS attributes
        FROM db.event e
        LEFT JOIN db."{payload_table}" p
          ON e."ocel_id" = p."ocel_id"
        WHERE e."ocel_type" = '{escaped_type}'
    """


def _build_object_select(
    con: duckdb.DuckDBPyConnection,
    type_name: str,
    payload_table: str,
) -> str:
    """
    Build the SELECT statement exporting one object type.

    Args:
        con: Open DuckDB connection with the SQLite database attached as `db`.
        type_name: The declared OCEL object type name.
        payload_table: The corresponding type-specific payload table name.

    Returns:
        A SQL SELECT statement returning object rows of the given type with
        optional temporal/change metadata and packed custom attributes.
    """
    payload_columns = _table_columns(con, payload_table)
    attribute_columns = [
        col
        for col in payload_columns
        if col not in {"ocel_id", "ocel_type", "ocel_time", "ocel_changed_field"}
    ]

    attributes_sql = _attributes_sql(attribute_columns, alias="p")
    escaped_type = type_name.replace("'", "''")

    time_sql = (
        'TRY_CAST(p."ocel_time" AS TIMESTAMP) AS ocel_time'
        if "ocel_time" in payload_columns
        else "CAST(NULL AS TIMESTAMP) AS ocel_time"
    )

    changed_field_sql = (
        'p."ocel_changed_field" AS ocel_changed_field'
        if "ocel_changed_field" in payload_columns
        else "NULL AS ocel_changed_field"
    )

    return f"""
        SELECT
            o."ocel_id" AS ocel_id,
            o."ocel_type" AS ocel_type,
            {time_sql},
            {changed_field_sql},
            {attributes_sql} AS attributes
        FROM db.object o
        LEFT JOIN db."{payload_table}" p
          ON o."ocel_id" = p."ocel_id"
        WHERE o."ocel_type" = '{escaped_type}'
    """


def _table_columns(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
) -> list[str]:
    """
    Return the column names of a SQLite table attached as `db`.

    Args:
        con: Open DuckDB connection with the SQLite database attached as `db`.
        table_name: Name of the table inside the attached SQLite database.

    Returns:
        A list of column names in table order.
    """
    return [
        row[0]
        for row in con.execute(f'DESCRIBE SELECT * FROM db."{table_name}"').fetchall()
    ]


def _attributes_sql(columns: list[str], *, alias: str) -> str:
    """
    Build the SQL expression packing custom columns into a JSON string.

    Args:
        columns: Custom attribute column names to include.
        alias: SQL alias of the payload table.

    Returns:
        A SQL expression yielding a JSON VARCHAR object. If `columns` is empty,
        returns an empty JSON object.
    """
    if not columns:
        return "CAST('{}' AS VARCHAR)"

    parts = [f"'{col}': {alias}.\"{col}\"" for col in columns]
    return f"CAST(to_json({{{', '.join(parts)}}}) AS VARCHAR)"
