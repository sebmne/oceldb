"""Materialize a row-preserving query into a new OCEL directory.

``materialize_query`` compiles the query, runs it against the source dataset
to find the identities to keep, then projects the canonical tables down to
those identities and writes them as a new OCEL directory.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import duckdb

from oceldb.compile.plan import compile_query
from oceldb.compile.schema import query_output_columns
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
from oceldb.plan.nodes import (
    GroupPlan,
    PlanNode,
    ProjectPlan,
    contains_node,
    root_source,
)
from oceldb.plan.sources import (
    EventSource,
    ObjectSource,
    ObjectStateSource,
    SublogFilter,
)


def materialize_query(ocel: OCEL, node: PlanNode) -> OCEL:
    _validate_materialized_query(ocel, node)

    tempdir = TemporaryDirectory(prefix="oceldb_materialized_")
    target_dir = Path(tempdir.name) / "dataset"
    created_at = datetime.now(timezone.utc)

    try:
        _write_materialized_directory(
            ocel, node, target_dir, created_at=created_at
        )
        return open_ocel_directory(
            target_dir,
            source_path=target_dir,
            tempdir=tempdir,
        )
    except Exception:
        tempdir.cleanup()
        raise


def _validate_materialized_query(ocel: OCEL, node: PlanNode) -> None:
    source = root_source(node)
    if not isinstance(source, (EventSource, ObjectSource, ObjectStateSource)):
        raise ValueError(
            "Only event-, object-, and object_state-rooted queries can be "
            "materialized to OCELs"
        )

    if contains_node(node, (ProjectPlan, GroupPlan)):
        raise ValueError(
            "to_ocel() is only valid for row-preserving queries; "
            "select(...) and group_by(...).agg(...) are not allowed"
        )

    output_columns = query_output_columns(node, ocel.manifest)
    if "ocel_id" not in output_columns:
        raise ValueError("to_ocel() requires the query result to contain 'ocel_id'")


def _write_materialized_directory(
    ocel: OCEL,
    node: PlanNode,
    target_dir: Path,
    *,
    created_at: datetime,
) -> None:
    target_dir.mkdir(parents=True, exist_ok=False)
    scratch_schema = f"materialize_{uuid4().hex[:8]}"
    con = ocel_connection(ocel)
    sql = compile_query(node, ocel.manifest)
    source = root_source(node)

    try:
        con.execute(f'CREATE SCHEMA "{scratch_schema}"')
        if isinstance(source, EventSource):
            _create_event_rooted_views(con, sql, scratch_schema)
        elif isinstance(source, (ObjectSource, ObjectStateSource)):
            _create_object_rooted_views(con, sql, scratch_schema)
        else:
            raise TypeError(f"Unsupported root source: {type(source).__name__}")

        _publish_materialized_views(con, scratch_schema)

        manifest = build_manifest_from_tables(
            con,
            oceldb_version=ocel.manifest.oceldb_version,
            source=str(ocel.path),
            created_at=created_at,
            schema=scratch_schema,
            drop_empty_custom_columns=True,
            source_manifest=ocel.manifest,
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


def _create_event_rooted_views(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    scratch_schema: str,
) -> None:
    con.execute(f"""
        CREATE TABLE "{scratch_schema}"."_root" AS
        SELECT DISTINCT ocel_id
        FROM ({sql}) q
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

    _create_shared_object_tables(con, scratch_schema)


def _create_object_rooted_views(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    scratch_schema: str,
) -> None:
    con.execute(f"""
        CREATE TABLE "{scratch_schema}"."_root" AS
        SELECT DISTINCT ocel_id
        FROM ({sql}) q
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

    _create_shared_object_tables(con, scratch_schema)


def _create_shared_object_tables(
    con: duckdb.DuckDBPyConnection,
    scratch_schema: str,
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
    select_list = ", ".join(
        f'"{name.replace(chr(34), chr(34) * 2)}"' for name in columns
    )
    con.execute(f"""
        COPY (
            SELECT {select_list}
            FROM "{schema}"."{table_name}"
        ) TO '{escaped_target}' (FORMAT PARQUET)
    """)


# ---------------------------------------------------------------------------
# Sublog materialization
# ---------------------------------------------------------------------------


def materialize_sublog(ocel: OCEL, sublog: SublogFilter) -> OCEL:
    """Materialize a ``Sublog`` (type filters + drop_orphan_events) as an OCEL."""
    tempdir = TemporaryDirectory(prefix="oceldb_materialized_")
    target_dir = Path(tempdir.name) / "dataset"
    created_at = datetime.now(timezone.utc)

    try:
        _write_sublog_directory(ocel, sublog, target_dir, created_at=created_at)
        return open_ocel_directory(
            target_dir,
            source_path=target_dir,
            tempdir=tempdir,
        )
    except Exception:
        tempdir.cleanup()
        raise


def _write_sublog_directory(
    ocel: OCEL,
    sublog: SublogFilter,
    target_dir: Path,
    *,
    created_at: datetime,
) -> None:
    target_dir.mkdir(parents=True, exist_ok=False)
    scratch_schema = f"materialize_{uuid4().hex[:8]}"
    con = ocel_connection(ocel)

    try:
        con.execute(f'CREATE SCHEMA "{scratch_schema}"')
        _create_sublog_views(con, sublog, scratch_schema)
        _publish_materialized_views(con, scratch_schema)

        manifest = build_manifest_from_tables(
            con,
            oceldb_version=ocel.manifest.oceldb_version,
            source=str(ocel.path),
            created_at=created_at,
            schema=scratch_schema,
            drop_empty_custom_columns=True,
            source_manifest=ocel.manifest,
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


def _create_sublog_views(
    con: duckdb.DuckDBPyConnection,
    sublog: SublogFilter,
    scratch_schema: str,
) -> None:
    event_predicates: list[str] = []
    if sublog.event_types is not None:
        types_sql = ", ".join(_sql_literal(t) for t in sorted(sublog.event_types))
        event_predicates.append(f'e."ocel_type" IN ({types_sql})')
    if (
        sublog.object_types is not None
        and sublog.drop_orphan_events
    ):
        types_sql = ", ".join(_sql_literal(t) for t in sorted(sublog.object_types))
        event_predicates.append(
            f'EXISTS (SELECT 1 FROM {logical_table_sql("event_object")} eo '
            f'JOIN {logical_table_sql("object")} o '
            f'ON o."ocel_id" = eo."ocel_object_id" '
            f'WHERE eo."ocel_event_id" = e."ocel_id" '
            f'AND o."ocel_type" IN ({types_sql}))'
        )
    event_where = (
        f"WHERE {' AND '.join(event_predicates)}" if event_predicates else ""
    )

    con.execute(f"""
        CREATE TABLE "{scratch_schema}"."_event" AS
        SELECT DISTINCT e.*
        FROM {logical_table_sql("event")} e
        {event_where}
    """)

    object_predicates = ['eo."ocel_event_id" IN '
                         f'(SELECT "ocel_id" FROM "{scratch_schema}"."_event")']
    if sublog.object_types is not None:
        types_sql = ", ".join(_sql_literal(t) for t in sorted(sublog.object_types))
        object_predicates.append(
            f'eo."ocel_object_id" IN '
            f'(SELECT "ocel_id" FROM {logical_table_sql("object")} '
            f'WHERE "ocel_type" IN ({types_sql}))'
        )
    eo_where = "WHERE " + " AND ".join(object_predicates)

    con.execute(f"""
        CREATE TABLE "{scratch_schema}"."_event_object" AS
        SELECT DISTINCT eo.*
        FROM {logical_table_sql("event_object")} eo
        {eo_where}
    """)

    con.execute(f"""
        CREATE TABLE "{scratch_schema}"."_object_ids" AS
        SELECT DISTINCT eo."ocel_object_id" AS "ocel_id"
        FROM "{scratch_schema}"."_event_object" eo
    """)

    _create_shared_object_tables(con, scratch_schema)


def _sql_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


__all__ = ["materialize_query", "materialize_sublog"]
