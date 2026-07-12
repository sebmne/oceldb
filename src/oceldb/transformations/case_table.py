"""case_table: one row per object summarising its lifecycle."""

from collections.abc import Iterable

import polars as pl

from oceldb import schema as s
from oceldb.utils import to_list
from oceldb.utils.step import step
from oceldb.ocel import OCEL

_FIXED = {s.OCEL_ID, s.OCEL_TIME, s.OCEL_EVENT_ID, s.OCEL_EVENT_TYPE, s.OCEL_TYPE}


@step
def case_table(
    ocel: OCEL, *, object_types: str | Iterable[str] | None = None
) -> pl.LazyFrame:
    """Summarise each object's lifecycle as a single row.

    Produces one row per object with trace-level aggregates and the object's
    last known attribute values (forward-filled from ``object_changes``). This
    is the standard case-feature table used as input for machine-learning and
    decision-mining on process data.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        object_types: Restrict the table to these object types. ``None``
            includes all types.

    Returns:
        A lazy frame with columns ``ocel_id``, ``ocel_type``, ``start_time``,
        ``end_time``, ``duration``, ``event_count``, ``first_event_type``,
        ``last_event_type``, and one column per object attribute (last known
        value, null if the attribute was never recorded for that object).
        Sorted by ``ocel_type``, then ``ocel_id``.

    Examples:
        >>> from oceldb.transformations import case_table
        >>> tbl = case_table(ocel)
        >>> tbl = case_table(ocel, object_types="order")
        >>> tbl = ocel >> case_table(object_types=["order", "item"])
    """
    e2o = ocel.event_object().join(
        ocel.events().select(
            pl.col(s.OCEL_ID).alias(s.OCEL_EVENT_ID),
            s.OCEL_TIME,
        ),
        on=s.OCEL_EVENT_ID,
        how="inner",
    )

    if object_types is not None:
        e2o = e2o.filter(pl.col(s.OCEL_OBJECT_TYPE).is_in(to_list(object_types)))

    trace = (
        e2o.group_by(s.OCEL_OBJECT_ID, s.OCEL_OBJECT_TYPE)
        .agg(
            pl.col(s.OCEL_TIME).min().alias("start_time"),
            pl.col(s.OCEL_TIME).max().alias("end_time"),
            pl.len().alias("event_count"),
            pl.col(s.OCEL_EVENT_TYPE)
            .sort_by(s.OCEL_TIME, s.OCEL_EVENT_ID)
            .first()
            .alias("first_event_type"),
            pl.col(s.OCEL_EVENT_TYPE)
            .sort_by(s.OCEL_TIME, s.OCEL_EVENT_ID)
            .last()
            .alias("last_event_type"),
        )
        .with_columns((pl.col("end_time") - pl.col("start_time")).alias("duration"))
        .rename({s.OCEL_OBJECT_ID: s.OCEL_ID, s.OCEL_OBJECT_TYPE: s.OCEL_TYPE})
    )

    types_arg = (
        (object_types,)
        if isinstance(object_types, str)
        else tuple(object_types)
        if object_types is not None
        else ()
    )
    states = ocel.object_states(*types_arg)
    attr_cols = [c for c in states.collect_schema().names() if c not in _FIXED]

    if attr_cols:
        last_attrs = (
            states.sort(s.OCEL_ID, s.OCEL_TIME)
            .unique(subset=[s.OCEL_ID], keep="last")
            .select(s.OCEL_ID, *attr_cols)
        )
        trace = trace.join(last_attrs, on=s.OCEL_ID, how="left")

    return trace.select(
        s.OCEL_ID,
        s.OCEL_TYPE,
        "start_time",
        "end_time",
        "duration",
        "event_count",
        "first_event_type",
        "last_event_type",
        *attr_cols,
    ).sort(s.OCEL_TYPE, s.OCEL_ID)
