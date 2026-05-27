"""Register persisted input and derived logical views for OCEL queries."""

from __future__ import annotations

from pathlib import Path

import ibis
import ibis.backends.duckdb

from oceldb.io.sql import encode_type_name, quote_identifier, sql_string
from oceldb.storage.manifest import Manifest


def _exec(con: ibis.backends.duckdb.Backend, sql: str) -> None:
    con.raw_sql(sql)  # pyright: ignore[reportUnknownMemberType]


def build_views(
    con: ibis.backends.duckdb.Backend, path: Path, manifest: Manifest
) -> None:
    """Register logical views over a persisted log."""
    _create_events_view(con, path)
    _create_objects_view(con, path)
    for type_name in manifest.object_types:
        _create_object_changes_view(con, path, type_name)
    _create_event_object_view(con, path)
    _create_object_object_view(con, path)
    build_derived_views(con, manifest)


def build_derived_views(con: ibis.backends.duckdb.Backend, manifest: Manifest) -> None:
    """Register views derived from logical OCEL tables."""
    for type_name, type_info in manifest.object_types.items():
        _create_state_history_view(con, type_name, list(type_info.attributes))


def _create_events_view(con: ibis.backends.duckdb.Backend, path: Path) -> None:
    glob = str(path / "events" / "**" / "*.parquet")
    sql = (
        f"CREATE OR REPLACE VIEW {quote_identifier('events')} AS "
        f"SELECT * FROM read_parquet({sql_string(glob)}, "
        f"hive_partitioning=true, union_by_name=true)"
    )
    _exec(con, sql)


def _create_objects_view(con: ibis.backends.duckdb.Backend, path: Path) -> None:
    glob = str(path / "objects" / "**" / "*.parquet")
    sql = (
        f"CREATE OR REPLACE VIEW {quote_identifier('objects')} AS "
        f"SELECT ocel_id, ocel_type "
        f"FROM read_parquet({sql_string(glob)}, hive_partitioning=true)"
    )
    _exec(con, sql)


def _create_object_changes_view(
    con: ibis.backends.duckdb.Backend, path: Path, type_name: str
) -> None:
    parquet = str(
        path
        / "object_changes"
        / f"ocel_type={encode_type_name(type_name)}"
        / "data.parquet"
    )
    view_name = f"object_changes_{type_name}"
    sql = (
        f"CREATE OR REPLACE VIEW {quote_identifier(view_name)} AS "
        f"SELECT * FROM read_parquet({sql_string(parquet)})"
    )
    _exec(con, sql)


def _create_state_history_view(
    con: ibis.backends.duckdb.Backend,
    type_name: str,
    attributes: list[str],
) -> None:
    source = quote_identifier(f"object_changes_{type_name}")
    view_name = f"{type_name}_state_history"

    if not attributes:
        sql = (
            f"CREATE OR REPLACE VIEW {quote_identifier(view_name)} AS "
            f"SELECT DISTINCT ocel_id, ocel_time "
            f"FROM {source}"
        )
    else:
        window_cols = ", ".join(
            f"LAST_VALUE({quote_identifier(attr)} IGNORE NULLS) OVER w AS {quote_identifier(attr)}"
            for attr in attributes
        )
        grouped_cols = ", ".join(
            f"MAX({quote_identifier(attr)}) AS {quote_identifier(attr)}"
            for attr in attributes
        )
        sql = (
            f"CREATE OR REPLACE VIEW {quote_identifier(view_name)} AS "
            f"SELECT ocel_id, ocel_time, {window_cols} "
            f"FROM ("
            f"  SELECT ocel_id, ocel_time, {grouped_cols} "
            f"  FROM {source} "
            f"  GROUP BY ocel_id, ocel_time"
            f") changes "
            f"WINDOW w AS ("
            f"  PARTITION BY ocel_id"
            f"  ORDER BY ocel_time"
            f"  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            f")"
        )
    _exec(con, sql)


def _create_event_object_view(con: ibis.backends.duckdb.Backend, path: Path) -> None:
    parquet = str(path / "event_object.parquet")
    sql = (
        f"CREATE OR REPLACE VIEW {quote_identifier('event_object')} AS "
        f"SELECT * FROM read_parquet({sql_string(parquet)})"
    )
    _exec(con, sql)


def _create_object_object_view(con: ibis.backends.duckdb.Backend, path: Path) -> None:
    parquet = path / "object_object.parquet"
    if not parquet.exists():
        return
    sql = (
        f"CREATE OR REPLACE VIEW {quote_identifier('object_object')} AS "
        f"SELECT * FROM read_parquet({sql_string(str(parquet))})"
    )
    _exec(con, sql)
