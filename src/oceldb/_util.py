"""Shared helpers used by multiple modules."""

from __future__ import annotations

from pathlib import Path

import duckdb


def sql_path(path: Path) -> str:
    """Escape a file path for use in a SQL string literal."""
    return str(path).replace("'", "''")


def query_type_map(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    entity: str,
) -> dict[str, str]:
    """Return ``{ocel_type: table_name}`` for an entity's per-type tables.

    Reads the ``event_map_type`` or ``object_map_type`` table and prefixes
    each ``ocel_type_map`` value with the entity name to form the actual
    table name (e.g. ``"event_CreateOrder"``).
    """
    rows = con.sql(
        f"SELECT ocel_type, ocel_type_map FROM {schema}.{entity}_map_type"
    ).fetchall()
    return {row[0]: f"{entity}_{row[1]}" for row in rows}
