"""flatten: project an OCEL onto one object type as a classical event log."""

from collections import Counter
from typing import cast

import polars as pl

from oceldb import schema as s
from oceldb.core.frames import reconstruct_attribute_states
from oceldb.core.step import step
from oceldb.ocel import OCEL

_CHANGE_FIXED = {
    s.OCEL_ID,
    s.OCEL_TIME,
    s.OCEL_CHANGED_FIELD,
    s.OCEL_TYPE,
}
_EVENT_FIXED = {s.OCEL_ID, s.OCEL_TIME, s.OCEL_TYPE}


@step
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
        ValueError: If ``object_type`` is unknown or generated XES, object, and
            event columns would collide in the flattened output.

    Examples:
        >>> from oceldb.transformations import flatten
        >>> log = flatten(ocel, "Container")
        >>> log = ocel >> flatten("Container")
        >>> variants = (ocel >> flatten("order")).group_by("case:concept:name").agg("concept:name")
    """
    _require_object_type(ocel, object_type)
    relations = (
        ocel.event_object()
        .filter(pl.col(s.OCEL_OBJECT_TYPE) == object_type)
        .select(
            pl.col(s.OCEL_OBJECT_ID).alias("case:concept:name"),
            s.OCEL_EVENT_ID,
            pl.col(s.OCEL_EVENT_TYPE).alias("concept:name"),
        )
        .unique(subset=["case:concept:name", s.OCEL_EVENT_ID])
    )
    related_objects = relations.select(
        pl.col("case:concept:name").alias(s.OCEL_ID)
    ).unique()
    changes = ocel.object_changes(object_type).join(
        related_objects,
        on=s.OCEL_ID,
        how="semi",
    )
    object_attrs = [
        col for col in changes.collect_schema().names() if col not in _CHANGE_FIXED
    ]
    # Forward-filling repeats existing values, so sparse changes are sufficient
    # to classify attributes without reconstructing the state history twice.
    static_attrs = _static_attributes(changes, object_attrs)
    dynamic_attrs = [attr for attr in object_attrs if attr not in static_attrs]
    event_attrs = [
        col for col in ocel.events().collect_schema().names() if col not in _EVENT_FIXED
    ]

    output_columns = [
        "case:concept:name",
        *(f"case:{attr}" for attr in static_attrs),
        "concept:name",
        "time:timestamp",
        *dynamic_attrs,
        *event_attrs,
        s.OCEL_EVENT_ID,
    ]
    collisions = sorted(
        name for name, count in Counter(output_columns).items() if count > 1
    )
    if collisions:
        raise ValueError(
            f"flatten output columns collide: {collisions}. "
            "Rename the conflicting source attributes before flattening."
        )

    events = ocel.events()
    result = relations.join(
        events.select(
            pl.col(s.OCEL_ID).alias(s.OCEL_EVENT_ID),
            pl.col(s.OCEL_TIME).alias("time:timestamp"),
        ),
        on=s.OCEL_EVENT_ID,
        how="inner",
    ).sort("case:concept:name", "time:timestamp", s.OCEL_EVENT_ID)

    if dynamic_attrs:
        states = reconstruct_attribute_states(changes)
        # ``changes`` contains one selected type, so the state helper's
        # type/id/time order is already case/time order for the as-of join.
        state_attrs = states.select(
            pl.col(s.OCEL_ID).alias("case:concept:name"),
            pl.col(s.OCEL_TIME).alias("time:timestamp"),
            *(pl.col(attr) for attr in dynamic_attrs),
        )
        result = result.join_asof(
            state_attrs,
            on="time:timestamp",
            by="case:concept:name",
            strategy="backward",
            check_sortedness=False,
        )

    if static_attrs:
        case_values = (
            changes.group_by(s.OCEL_ID)
            .agg(
                pl.col(attr).drop_nulls().first().alias(f"case:{attr}")
                for attr in static_attrs
            )
            .rename({s.OCEL_ID: "case:concept:name"})
        )
        result = result.join(
            case_values,
            on="case:concept:name",
            how="left",
            maintain_order="left",
        )

    if event_attrs:
        event_payload = events.select(
            pl.col(s.OCEL_ID).alias(s.OCEL_EVENT_ID),
            *(pl.col(attr) for attr in event_attrs),
        )
        result = result.join(
            event_payload,
            on=s.OCEL_EVENT_ID,
            how="left",
            maintain_order="left",
        )

    return result.select(
        "case:concept:name",
        *(f"case:{attr}" for attr in static_attrs),
        "concept:name",
        "time:timestamp",
        *dynamic_attrs,
        *event_attrs,
        s.OCEL_EVENT_ID,
    )


def _static_attributes(changes: pl.LazyFrame, object_attrs: list[str]) -> list[str]:
    """Object attributes that never take more than one value for any object.

    Such attributes are constant within every case, so they are true case
    attributes. Reads a one-row-per-attribute summary of the maximum distinct
    non-null value count across objects.
    """
    if not object_attrs:
        return []
    per_object = changes.group_by(s.OCEL_ID).agg(
        pl.col(attr).drop_nulls().n_unique().alias(attr) for attr in object_attrs
    )
    maxima = per_object.select(
        pl.col(attr).max().alias(attr) for attr in object_attrs
    ).collect()
    row = maxima.row(0, named=True)
    return [attr for attr in object_attrs if cast(int, row[attr] or 0) <= 1]


def _require_object_type(ocel: OCEL, object_type: object) -> None:
    """Reject malformed or unknown case notions before building a lazy plan."""
    if not isinstance(object_type, str):
        raise TypeError("object_type must be a string.")
    if not object_type:
        raise ValueError("object_type must not be empty.")
    if ocel._presence is not None:
        if object_type not in ocel._presence.object_types:
            raise ValueError(f"Unknown object type {object_type!r}.")
        return
    exists = (
        ocel.objects()
        .filter(pl.col(s.OCEL_TYPE) == object_type)
        .limit(1)
        .collect()
        .height
    )
    if not exists:
        raise ValueError(f"Unknown object type {object_type!r}.")
