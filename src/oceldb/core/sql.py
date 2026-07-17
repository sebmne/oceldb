"""Run a SQL query over an OCEL's five tables via an ephemeral DuckDB connection."""

import polars as pl


def execute_sql(
    query: str,
    *,
    events: pl.LazyFrame,
    objects: pl.LazyFrame,
    object_changes: pl.LazyFrame,
    e2o: pl.LazyFrame,
    o2o: pl.LazyFrame,
) -> pl.DataFrame:
    """Register the five tables with DuckDB and run *query* eagerly.

    Args:
        query: A SQL query referencing ``events``, ``objects``,
            ``object_changes``, ``e2o``, and ``o2o``.
        events: The events table to register.
        objects: The objects table to register.
        object_changes: The object_changes table to register.
        e2o: The E2O relation table to register.
        o2o: The O2O relation table to register.

    Returns:
        The query result as an eager ``polars.DataFrame``.
    """
    import duckdb

    connection = duckdb.connect()
    try:
        connection.register("events", events)
        connection.register("objects", objects)
        connection.register("object_changes", object_changes)
        connection.register("e2o", e2o)
        connection.register("o2o", o2o)
        return connection.sql(query).pl()
    finally:
        connection.close()
