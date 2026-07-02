"""Shared helpers for in-memory OCEL readers."""

from typing import Any

import polars as pl

from oceldb import schema as s

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
OC_SCHEMA: dict[str, pl.DataType] = {
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
    return pl.DataFrame(
        {k: pl.Series([], dtype=v) for k, v in schema.items()}
    ).lazy()


def rows_to_lf(rows: list[dict[str, Any]]) -> pl.LazyFrame:
    return pl.from_dicts(rows, infer_schema_length=None).lazy()


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
