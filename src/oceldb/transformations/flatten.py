"""flatten: project an OCEL onto one object type as a classical event log."""

from collections.abc import Callable
from typing import cast, overload

import polars as pl

from oceldb import schema as s
from oceldb.utils._step import _step
from oceldb.ocel import OCEL

_STATE_FIXED = {
    s.OCEL_ID,
    s.OCEL_TIME,
    s.OCEL_EVENT_ID,
    s.OCEL_EVENT_TYPE,
    s.OCEL_TYPE,
}
_EVENT_FIXED = {s.OCEL_ID, s.OCEL_TIME, s.OCEL_TYPE}


@overload
def flatten(ocel: OCEL, object_type: str) -> pl.LazyFrame: ...


@overload
def flatten(object_type: str) -> Callable[[OCEL], pl.LazyFrame]: ...


@_step
def flatten(ocel: OCEL, object_type: str) -> pl.LazyFrame:
    """Flatten the log to a classical XES-style event log for one object type.

    Objects of *object_type* become cases; each event an object takes part in
    becomes a case event, annotated with the object's attribute state at that
    event, the event's own payload attributes, and any static case attributes.
    This is the standard input for classical, control-flow process discovery.

    Object attributes are split by whether they change over an object's life:

    * A **static** attribute — one that never takes more than one value for
      *any* object of the type — is a genuine case attribute and is emitted once
      per case as ``case:<attribute>``.
    * A **dynamic** attribute — one that changes for at least one object — is
      emitted as a plain ``<attribute>`` column holding the object's value *as
      of that event* (forward-filled), because it is event-level, not constant
      across the case.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        object_type: The object type to use as the case notion.

    Returns:
        A lazy frame using XES column names: ``case:concept:name`` (the object
        id / case), one ``case:<attribute>`` column per static object attribute,
        ``concept:name`` (the event type / activity), ``time:timestamp`` (the
        event time), one ``<attribute>`` column per dynamic object attribute,
        one ``<attribute>`` column per event payload attribute, and
        ``ocel_event_id``. There is one row per ``(object, event)`` — an event
        involving two objects of the type yields two rows (flattening
        convergence). Sorted by case, then time.

    Notes:
        A dynamic object attribute is null for events that precede its first
        recorded value; a static case attribute is null only for objects that
        never recorded it; an event payload attribute is null for event types
        that do not define it. Classifying attributes reads a small per-object
        summary, so this executes an eager step even though the result is lazy.

    Raises:
        ValueError: If a dynamic object attribute and an event payload attribute
            share a name, which would be ambiguous in the flattened row. Rename
            one side before flattening.

    Examples:
        >>> from oceldb.transformations import flatten
        >>> log = flatten(ocel, "Container")
        >>> log = ocel >> flatten("Container")
        >>> variants = (ocel >> flatten("order")).group_by("case:concept:name").agg("concept:name")
    """
    states = ocel.object_states(object_type)
    object_attrs = [
        col for col in states.collect_schema().names() if col not in _STATE_FIXED
    ]
    static_attrs = _static_attributes(states, object_attrs)
    dynamic_attrs = [attr for attr in object_attrs if attr not in static_attrs]
    event_attrs = [
        col for col in ocel.events().collect_schema().names() if col not in _EVENT_FIXED
    ]

    clash = sorted(set(dynamic_attrs) & set(event_attrs))
    if clash:
        raise ValueError(
            f"flatten: object and event attributes share names {clash}; "
            "rename one side before flattening."
        )

    result = (
        ocel.event_object()
        .filter(pl.col(s.OCEL_OBJECT_TYPE) == object_type)
        .select(
            pl.col(s.OCEL_OBJECT_ID).alias("case:concept:name"),
            s.OCEL_EVENT_ID,
            pl.col(s.OCEL_EVENT_TYPE).alias("concept:name"),
        )
        .unique(subset=["case:concept:name", s.OCEL_EVENT_ID])
        .join(
            ocel.events().select(
                pl.col(s.OCEL_ID).alias(s.OCEL_EVENT_ID),
                pl.col(s.OCEL_TIME).alias("time:timestamp"),
                *(pl.col(attr) for attr in event_attrs),
            ),
            on=s.OCEL_EVENT_ID,
            how="inner",
        )
        .sort("time:timestamp")
    )

    if dynamic_attrs:
        state_attrs = states.select(
            pl.col(s.OCEL_ID).alias("case:concept:name"),
            pl.col(s.OCEL_TIME).alias("time:timestamp"),
            *(pl.col(attr) for attr in dynamic_attrs),
        ).sort("time:timestamp")
        result = result.join_asof(
            state_attrs,
            on="time:timestamp",
            by="case:concept:name",
            strategy="backward",
            check_sortedness=False,
        )

    if static_attrs:
        case_values = (
            states.group_by(s.OCEL_ID)
            .agg(
                pl.col(attr).drop_nulls().first().alias(f"case:{attr}")
                for attr in static_attrs
            )
            .rename({s.OCEL_ID: "case:concept:name"})
        )
        result = result.join(case_values, on="case:concept:name", how="left")

    return result.select(
        "case:concept:name",
        *(f"case:{attr}" for attr in static_attrs),
        "concept:name",
        "time:timestamp",
        *dynamic_attrs,
        *event_attrs,
        s.OCEL_EVENT_ID,
    ).sort("case:concept:name", "time:timestamp", s.OCEL_EVENT_ID)


def _static_attributes(states: pl.LazyFrame, object_attrs: list[str]) -> list[str]:
    """Object attributes that never take more than one value for any object.

    Such attributes are constant within every case, so they are true case
    attributes. Reads a one-row-per-attribute summary of the maximum distinct
    non-null value count across objects.
    """
    if not object_attrs:
        return []
    per_object = states.group_by(s.OCEL_ID).agg(
        pl.col(attr).drop_nulls().n_unique().alias(attr) for attr in object_attrs
    )
    maxima = per_object.select(
        pl.col(attr).max().alias(attr) for attr in object_attrs
    ).collect()
    row = maxima.row(0, named=True)
    return [attr for attr in object_attrs if cast(int, row[attr] or 0) <= 1]
