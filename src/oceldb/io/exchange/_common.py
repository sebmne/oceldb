"""Shared normalization helpers for in-memory OCEL readers."""

from datetime import datetime, timezone
from typing import Any
from collections.abc import Mapping

import polars as pl

from oceldb.schema import OCELSchema
from oceldb.schema._layout import (
    E2O_SCHEMA,
    EVENTS_SCHEMA,
    OBJECT_CHANGES_SCHEMA,
    OBJECTS_SCHEMA,
    O2O_SCHEMA,
)
from oceldb.ocel import OCEL

EPOCH = "1970-01-01T00:00:00+00:00"
EPOCH_DATETIME = datetime(1970, 1, 1, tzinfo=timezone.utc)


def empty_lf(schema: Mapping[str, pl.DataType]) -> pl.LazyFrame:
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
    schema: OCELSchema | None = None,
) -> OCEL:
    """Assemble an in-memory ``OCEL`` from parsed row dictionaries.

    Each list of rows is turned into a lazy frame; empty lists fall back to a
    correctly typed empty frame. Event and object-change timestamps are parsed
    from their ISO 8601 string form. Shared by the JSON and XML readers.
    """
    return OCEL.from_frames(
        events=rows_to_lf(event_rows) if event_rows else empty_lf(EVENTS_SCHEMA),
        objects=rows_to_lf(object_rows) if object_rows else empty_lf(OBJECTS_SCHEMA),
        object_changes=rows_to_lf(object_change_rows)
        if object_change_rows
        else empty_lf(OBJECT_CHANGES_SCHEMA),
        event_object=rows_to_lf(e2o_rows) if e2o_rows else empty_lf(E2O_SCHEMA),
        object_object=rows_to_lf(o2o_rows) if o2o_rows else empty_lf(O2O_SCHEMA),
        schema=schema,
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
