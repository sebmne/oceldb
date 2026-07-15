"""DuckDB SQL execution over the five logical OCEL frames."""

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
    """Register logical frames in a temporary DuckDB connection and query them."""
    import duckdb

    connection = duckdb.connect()
    try:
        connection.register("events", events)
        connection.register("objects", objects)
        connection.register("object_changes", object_changes)
        connection.register("event_object", e2o)
        connection.register("object_object", o2o)
        return connection.sql(query).pl()
    finally:
        connection.close()
