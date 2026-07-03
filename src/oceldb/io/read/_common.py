"""Shared helpers for in-memory OCEL readers."""

from typing import Any

import polars as pl

from oceldb import schema as s
from oceldb.ocel import OCEL

_EPOCH = "1970-01-01T00:00:00+00:00"

EVENTS_SCHEMA: dict[str, pl.DataType] = {
    s.OCEL_ID: pl.String(),
    s.OCEL_TIME: pl.Datetime("us", "UTC"),
    s.OCEL_TYPE: pl.String(),
}
OBJECTS_SCHEMA: dict[str, pl.DataType] = {
    s.OCEL_ID: pl.String(),
    s.OCEL_TYPE: pl.String(),
}
OBJECT_CHANGES_SCHEMA: dict[str, pl.DataType] = {
    s.OCEL_ID: pl.String(),
    s.OCEL_TIME: pl.Datetime("us", "UTC"),
    s.OCEL_TYPE: pl.String(),
    s.OCEL_CHANGED_FIELD: pl.String(),
}
E2O_SCHEMA: dict[str, pl.DataType] = {
    s.OCEL_EVENT_ID: pl.String(),
    s.OCEL_EVENT_TYPE: pl.String(),
    s.OCEL_OBJECT_ID: pl.String(),
    s.OCEL_OBJECT_TYPE: pl.String(),
    s.OCEL_QUALIFIER: pl.String(),
}
O2O_SCHEMA: dict[str, pl.DataType] = {
    s.OCEL_SOURCE_ID: pl.String(),
    s.OCEL_SOURCE_TYPE: pl.String(),
    s.OCEL_TARGET_ID: pl.String(),
    s.OCEL_TARGET_TYPE: pl.String(),
    s.OCEL_QUALIFIER: pl.String(),
}


def empty_lf(schema: dict[str, pl.DataType]) -> pl.LazyFrame:
    return pl.DataFrame({k: pl.Series([], dtype=v) for k, v in schema.items()}).lazy()


def rows_to_lf(rows: list[dict[str, Any]]) -> pl.LazyFrame:
    return pl.from_dicts(rows, infer_schema_length=None).lazy()


def build_ocel(
    *,
    event_rows: list[dict[str, Any]],
    object_rows: list[dict[str, Any]],
    object_change_rows: list[dict[str, Any]],
    e2o_rows: list[dict[str, Any]],
    o2o_rows: list[dict[str, Any]],
) -> OCEL:
    """Assemble an in-memory ``OCEL`` from parsed row dictionaries.

    Each list of rows is turned into a lazy frame; empty lists fall back to a
    correctly typed empty frame. Event and object-change timestamps are parsed
    from their ISO 8601 string form. Shared by the JSON and XML readers.
    """
    return OCEL(
        events=parse_timestamps(rows_to_lf(event_rows), s.OCEL_TIME)
        if event_rows
        else empty_lf(EVENTS_SCHEMA),
        objects=rows_to_lf(object_rows) if object_rows else empty_lf(OBJECTS_SCHEMA),
        object_changes=parse_timestamps(rows_to_lf(object_change_rows), s.OCEL_TIME)
        if object_change_rows
        else empty_lf(OBJECT_CHANGES_SCHEMA),
        e2o=rows_to_lf(e2o_rows) if e2o_rows else empty_lf(E2O_SCHEMA),
        o2o=rows_to_lf(o2o_rows) if o2o_rows else empty_lf(O2O_SCHEMA),
    )


def parse_timestamps(lf: pl.LazyFrame, col: str) -> pl.LazyFrame:
    # OCEL timestamps are ISO 8601 with optional fractional seconds and
    # a colon-separated UTC offset (e.g. "2021-01-01T00:00:00+00:00").
    # Polars 1.x requires an explicit format when the string contains a TZ offset.
    return lf.with_columns(
        pl.col(col)
        .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%:z", time_unit="us", strict=False)
        .fill_null(
            # Fallback for fractional-second variants (e.g. ".000")
            pl.col(col).str.to_datetime(
                format="%Y-%m-%dT%H:%M:%S%.f%:z", time_unit="us", strict=False
            )
        )
    )
