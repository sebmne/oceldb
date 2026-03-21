"""Create filtered DuckDB views that mirror an OCEL 2.0 schema.

The OCEL 2.0 SQLite standard stores data across multiple tables:

- **Core tables**: ``event``, ``object`` — one row per event/object with
  ``ocel_id`` and ``ocel_type``.
- **Relationship tables**: ``event_object``, ``object_object`` — links
  between events and objects or between objects.
- **Map tables**: ``event_map_type``, ``object_map_type`` — map each
  ``ocel_type`` to its per-type attribute table name.
- **Per-type tables**: ``event_<TypeName>``, ``object_<TypeName>`` — store
  ``ocel_time`` and type-specific attributes (one row per entity).

:func:`create_views` produces a full set of views in a new DuckDB schema
that mirrors this structure, scoped to entities that pass the given filters.
"""

from __future__ import annotations

import itertools

import duckdb

from oceldb._util import query_type_map
from oceldb.expr import Expr

_view_counter = itertools.count(1)


def create_views(
    con: duckdb.DuckDBPyConnection,
    source: str,
    event_filters: list[Expr],
    object_filters: list[Expr],
) -> str:
    """Create a new DuckDB schema with filtered views of an OCEL 2.0 dataset.

    All views are lazy — they store SQL definitions, not data.  The
    returned schema prefix (e.g. ``"memory.ocel_view_1"``) can be passed
    to :class:`Ocel` to create a view-backed instance.

    Args:
        con: An open DuckDB connection that has the source schema attached.
        source: Schema prefix of the source tables (e.g. ``"ocel_db.main"``).
        event_filters: Expressions to apply to events.
        object_filters: Expressions to apply to objects.

    Returns:
        The schema prefix of the newly created views.
    """
    target = f"memory.ocel_view_{next(_view_counter)}"
    con.execute(f"CREATE SCHEMA {target}")

    def view(table: str, where: str = "TRUE") -> None:
        """Create a view in *target* as a filtered copy of a *source* table."""
        con.execute(
            f'CREATE VIEW {target}."{table}" AS '
            f'SELECT * FROM {source}."{table}" WHERE {where}'
        )

    def entity_view(entity: str, filters: list[Expr]) -> None:
        """Create the core event or object view.

        When filters are present, a unified subquery is built from all
        per-type tables via ``UNION ALL BY NAME``.  Each per-type table
        gets a synthesized ``ocel_type`` column so that type-level filters
        (e.g. ``event.type == "Order"``) resolve correctly.  Columns that
        exist only in some per-type tables are filled with ``NULL`` by
        DuckDB, which makes type-specific attribute filters (e.g.
        ``event.amount > 100``) naturally exclude non-matching types.
        """
        if not filters:
            view(entity)
            return

        type_map = query_type_map(con, source, entity)
        subqueries = [
            f"SELECT *, '{ocel_type.replace("'", "''")}' AS ocel_type "
            f'FROM {source}."{table}"'
            for ocel_type, table in type_map.items()
        ]
        union = " UNION ALL BY NAME ".join(subqueries)
        where = " AND ".join(f.to_sql() for f in filters)
        view(entity, f"ocel_id IN (SELECT ocel_id FROM ({union}) WHERE {where})")

    # Entity views — filtered event/object tables
    for entity, filters in [("event", event_filters), ("object", object_filters)]:
        entity_view(entity, filters)

    # Relationship views — only rows referencing surviving entities
    view(
        "event_object",
        f"ocel_event_id IN (SELECT ocel_id FROM {target}.event) "
        f"AND ocel_object_id IN (SELECT ocel_id FROM {target}.object)",
    )
    view(
        "object_object",
        f"ocel_source_id IN (SELECT ocel_id FROM {target}.object) "
        f"AND ocel_target_id IN (SELECT ocel_id FROM {target}.object)",
    )

    # Map + per-type views — scoped to types that still have entities
    for entity in ("event", "object"):
        view(
            f"{entity}_map_type",
            f"ocel_type IN (SELECT DISTINCT ocel_type FROM {target}.{entity})",
        )
        for ocel_type, table in query_type_map(con, source, entity).items():
            escaped = ocel_type.replace("'", "''")
            view(
                table,
                f"ocel_id IN (SELECT ocel_id FROM {target}.{entity} "
                f"WHERE ocel_type = '{escaped}')",
            )

    return target
