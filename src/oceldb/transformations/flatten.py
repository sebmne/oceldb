"""flatten: project an OCEL onto one object type as a classical event log."""

from collections.abc import Callable
from typing import overload

import polars as pl

from oceldb import schema as s
from oceldb.filters._step import _step
from oceldb.ocel import OCEL

_FIXED = {s.OCEL_ID, s.OCEL_TIME, s.OCEL_EVENT_ID, s.OCEL_EVENT_TYPE, s.OCEL_TYPE}


@overload
def flatten(ocel: OCEL, object_type: str) -> pl.LazyFrame: ...


@overload
def flatten(object_type: str) -> Callable[[OCEL], pl.LazyFrame]: ...


@_step
def flatten(ocel: OCEL, object_type: str) -> pl.LazyFrame:
    """Flatten the log to a classical XES-style event log for one object type.

    Objects of *object_type* become cases; each event an object takes part in
    becomes a case event, annotated with the object's attribute state at that
    event (forward-filled). This is the standard input for classical,
    control-flow process discovery.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        object_type: The object type to use as the case notion.

    Returns:
        A lazy frame using XES column names: ``case:concept:name`` (the object
        id / case), ``concept:name`` (the event type / activity),
        ``time:timestamp`` (the event time), one ``case:<attribute>`` column
        per object attribute (its value as of that event), and
        ``ocel_event_id``. There is one row per ``(object, event)`` — an event
        involving two objects of the type yields two rows (flattening
        convergence). Sorted by case, then time.

    Notes:
        A ``case:<attribute>`` is null for events that precede the object's
        first recorded value. Event payload attributes are not included.

    Examples:
        >>> from oceldb.transformations import flatten
        >>> log = flatten(ocel, "Container")
        >>> log = ocel >> flatten("Container")
        >>> variants = (ocel >> flatten("order")).group_by("case:concept:name").agg("concept:name")
    """
    states = ocel.object_states(object_type)
    attrs = [col for col in states.collect_schema().names() if col not in _FIXED]

    participations = (
        ocel.event_object()
        .filter(pl.col(s.OCEL_OBJECT_TYPE) == object_type)
        .select(
            pl.col(s.OCEL_OBJECT_ID).alias("case:concept:name"),
            s.OCEL_EVENT_ID,
            pl.col(s.OCEL_EVENT_TYPE).alias("concept:name"),
        )
        .join(
            ocel.events().select(
                pl.col(s.OCEL_ID).alias(s.OCEL_EVENT_ID),
                pl.col(s.OCEL_TIME).alias("time:timestamp"),
            ),
            on=s.OCEL_EVENT_ID,
            how="inner",
        )
    )
    state_attrs = states.select(
        pl.col(s.OCEL_ID).alias("case:concept:name"),
        pl.col(s.OCEL_TIME).alias("time:timestamp"),
        *(pl.col(attr) for attr in attrs),
    ).sort("time:timestamp")
    result = participations.sort("time:timestamp").join_asof(
        state_attrs,
        on="time:timestamp",
        by="case:concept:name",
        strategy="backward",
        check_sortedness=False,
    )
    return result.select(
        "case:concept:name",
        "concept:name",
        "time:timestamp",
        *attrs,
        s.OCEL_EVENT_ID,
    ).sort("case:concept:name", "time:timestamp", s.OCEL_EVENT_ID)
