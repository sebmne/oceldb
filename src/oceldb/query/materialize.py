from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import duckdb

from oceldb.core.ocel import (
    OCEL,
    logical_table_sql,
    ocel_connection,
)
from oceldb.io._manifest import (
    LOGICAL_TABLES,
    MANIFEST_FILE,
    build_manifest_from_tables,
    write_manifest,
)
from oceldb.io.read import open_ocel_directory
from oceldb.query.compiler import compile_query, query_output_columns
from oceldb.query.plan import GroupPlan, ProjectPlan, QueryPlan, contains_node, root_source, source_kind


def materialize_query(query: QueryPlan) -> OCEL:
    _validate_materialized_query(query)

    tempdir = TemporaryDirectory(prefix="oceldb_materialized_")
    target_dir = Path(tempdir.name) / "dataset"
    created_at = datetime.now(timezone.utc)

    try:
        _write_materialized_directory(query, target_dir, created_at=created_at)
        return open_ocel_directory(
            target_dir,
            source_path=target_dir,
            tempdir=tempdir,
        )
    except Exception:
        tempdir.cleanup()
        raise


def _validate_materialized_query(query: QueryPlan) -> None:
    if source_kind(root_source(query.node)) not in {"event", "object", "object_state"}:
        raise ValueError(
            "Only event-, object-, and object_state-rooted queries can be materialized to OCELs"
        )

    if contains_node(query.node, (ProjectPlan, GroupPlan)):
        raise ValueError(
            "to_ocel() is only valid for row-preserving queries; "
            "select(...) and group_by(...).agg(...) are not allowed"
        )

    output_columns = query_output_columns(query)
    if "ocel_id" not in output_columns:
        raise ValueError("to_ocel() requires the query result to contain 'ocel_id'")


def _write_materialized_directory(
    query: QueryPlan,
    target_dir: Path,
    *,
    created_at: datetime,
) -> None:
    target_dir.mkdir(parents=True, exist_ok=False)
    scratch_schema = f"materialize_{uuid4().hex[:8]}"
    con = ocel_connection(query.ocel)

    try:
        con.execute(f'CREATE SCHEMA "{scratch_schema}"')
        _create_materialized_views(query, scratch_schema)

        manifest = build_manifest_from_tables(
            con,
            oceldb_version=query.ocel.manifest.oceldb_version,
            source=str(query.ocel.path),
            created_at=created_at,
            schema=scratch_schema,
            drop_empty_custom_columns=True,
            source_manifest=query.ocel.manifest,
        )

        for table_name in LOGICAL_TABLES:
            _copy_schema_table(
                con,
                scratch_schema,
                table_name,
                target_dir / f"{table_name}.parquet",
                columns=tuple(manifest.table(table_name).columns),
            )

        write_manifest(target_dir / MANIFEST_FILE, manifest)
    except Exception:
        if target_dir.exists():
            for child in target_dir.iterdir():
                child.unlink()
            target_dir.rmdir()
        raise
    finally:
        con.execute(f'DROP SCHEMA IF EXISTS "{scratch_schema}" CASCADE')


def _create_materialized_views(query: QueryPlan, scratch_schema: str) -> None:
    root_kind = source_kind(root_source(query.node))

    if root_kind == "event":
        _create_event_rooted_views(query, scratch_schema)
        _publish_materialized_views(ocel_connection(query.ocel), scratch_schema)
        return

    if root_kind in {"object", "object_state"}:
        _create_object_rooted_views(query, scratch_schema)
        _publish_materialized_views(ocel_connection(query.ocel), scratch_schema)
        return

    raise TypeError(f"Unsupported root kind: {root_kind!r}")


def _create_event_rooted_views(query: QueryPlan, scratch_schema: str) -> None:
    con = ocel_connection(query.ocel)

    con.execute(f"""
        CREATE TABLE "{scratch_schema}"."_root" AS
        SELECT DISTINCT ocel_id
        FROM ({compile_query(query)}) q
    """)

    con.execute(f"""
        CREATE TABLE "{scratch_schema}"."_event" AS
        SELECT DISTINCT e.*
        FROM {logical_table_sql("event")} e
        JOIN "{scratch_schema}"."_root" r
          ON e."ocel_id" = r."ocel_id"
    """)

    con.execute(f"""
        CREATE TABLE "{scratch_schema}"."_event_object" AS
        SELECT DISTINCT eo.*
        FROM {logical_table_sql("event_object")} eo
        JOIN "{scratch_schema}"."_event" e
          ON eo."ocel_event_id" = e."ocel_id"
    """)

    con.execute(f"""
        CREATE TABLE "{scratch_schema}"."_object_ids" AS
        SELECT DISTINCT eo."ocel_object_id" AS "ocel_id"
        FROM "{scratch_schema}"."_event_object" eo
    """)

    _create_shared_object_tables(query, scratch_schema, con)


def _create_object_rooted_views(query: QueryPlan, scratch_schema: str) -> None:
    con = ocel_connection(query.ocel)

    con.execute(f"""
        CREATE TABLE "{scratch_schema}"."_root" AS
        SELECT DISTINCT ocel_id
        FROM ({compile_query(query)}) q
    """)

    con.execute(f"""
        CREATE TABLE "{scratch_schema}"."_event" AS
        SELECT DISTINCT e.*
        FROM {logical_table_sql("event")} e
        JOIN {logical_table_sql("event_object")} eo
          ON e."ocel_id" = eo."ocel_event_id"
        JOIN "{scratch_schema}"."_root" r
          ON eo."ocel_object_id" = r."ocel_id"
    """)

    con.execute(f"""
        CREATE TABLE "{scratch_schema}"."_event_object" AS
        SELECT DISTINCT eo.*
        FROM {logical_table_sql("event_object")} eo
        JOIN "{scratch_schema}"."_event" e
          ON eo."ocel_event_id" = e."ocel_id"
    """)

    con.execute(f"""
        CREATE TABLE "{scratch_schema}"."_object_ids" AS
        SELECT "ocel_id"
        FROM "{scratch_schema}"."_root"
        UNION
        SELECT eo."ocel_object_id" AS "ocel_id"
        FROM "{scratch_schema}"."_event_object" eo
    """)

    _create_shared_object_tables(query, scratch_schema, con)


def _create_shared_object_tables(
    query: QueryPlan,
    scratch_schema: str,
    con: duckdb.DuckDBPyConnection,
) -> None:
    con.execute(f"""
        CREATE TABLE "{scratch_schema}"."_object" AS
        SELECT DISTINCT o.*
        FROM {logical_table_sql("object")} o
        JOIN "{scratch_schema}"."_object_ids" ids
          ON o."ocel_id" = ids."ocel_id"
    """)

    con.execute(f"""
        CREATE TABLE "{scratch_schema}"."_object_change" AS
        SELECT DISTINCT oc.*
        FROM {logical_table_sql("object_change")} oc
        JOIN "{scratch_schema}"."_object_ids" ids
          ON oc."ocel_id" = ids."ocel_id"
    """)

    con.execute(f"""
        CREATE TABLE "{scratch_schema}"."_object_object" AS
        SELECT DISTINCT oo.*
        FROM {logical_table_sql("object_object")} oo
        JOIN "{scratch_schema}"."_object_ids" os
          ON oo."ocel_source_id" = os."ocel_id"
        JOIN "{scratch_schema}"."_object_ids" ot
          ON oo."ocel_target_id" = ot."ocel_id"
    """)


def _publish_materialized_views(
    con: duckdb.DuckDBPyConnection,
    scratch_schema: str,
) -> None:
    for table_name in LOGICAL_TABLES:
        con.execute(f"""
            CREATE VIEW "{scratch_schema}"."{table_name}" AS
            SELECT *
            FROM "{scratch_schema}"."_{table_name}"
        """)


def _copy_schema_table(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    table_name: str,
    target_file: Path,
    *,
    columns: tuple[str, ...],
) -> None:
    escaped_target = str(target_file).replace("'", "''")
    select_list = ", ".join(f'"{name.replace(chr(34), chr(34) * 2)}"' for name in columns)
    con.execute(f"""
        COPY (
            SELECT {select_list}
            FROM "{schema}"."{table_name}"
        ) TO '{escaped_target}' (FORMAT PARQUET)
    """)
