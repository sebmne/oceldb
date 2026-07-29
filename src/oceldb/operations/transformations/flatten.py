"""Flatten an OCEL onto one object type as a classical event log."""

from collections import Counter

import polars as pl

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations.step import step
from oceldb.types import OneOrMany, normalize_strings


@step
def flatten(
    ocel: OCEL,
    object_type: str,
    *,
    case_attributes: OneOrMany[str] | None = None,
) -> pl.LazyFrame:
    """Flatten an OCEL to a classical event log for one object type.

    Objects of ``object_type`` become cases. Each related event produces one
    row per case and carries the object's attribute state at the event time.
    Attributes listed in ``case_attributes`` are instead read from the
    object's initial state and emitted once per row with a ``case:`` prefix.
    Event attribute columns are limited to event types related to the selected
    object type.

    The function never guesses whether an object attribute is static. This
    keeps the output schema stable across datasets and avoids an eager
    per-object cardinality scan.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        object_type: Object type to use as the case notion.
        case_attributes: Object attributes to expose as case attributes.
            ``None`` leaves every object attribute as a point-in-time state
            column.

    Returns:
        A lazy frame containing ``case:concept:name``, requested
        ``case:<attribute>`` columns, ``concept:name``, ``time:timestamp``,
        remaining object-state attributes, event attributes, and
        ``ocel_event_id``. Rows are sorted by case, timestamp, and event id.

    Raises:
        TypeError: If ``object_type`` or an attribute selector is malformed.
        ValueError: If ``object_type`` is empty or unknown, a requested case
            attribute does not belong to that object type, or output column
            names collide.

    Examples:
        >>> from oceldb.operations import flatten
        >>> log = flatten(ocel, "Container")
        >>> log = ocel >> flatten("Order", case_attributes=["customer"])
    """
    _require_object_type(ocel, object_type)
    object_attributes = ocel.object_attribute_names(object_type)
    selected_case_attributes = normalize_strings(
        case_attributes,
        name="case_attributes",
    )
    selected_case_attributes = selected_case_attributes or []
    if unknown := sorted(set(selected_case_attributes) - set(object_attributes)):
        raise ValueError(f"Unknown attributes for the selected object type: {unknown}.")
    case_set = set(selected_case_attributes)
    case_attrs = [name for name in object_attributes if name in case_set]
    state_attrs = [name for name in object_attributes if name not in case_set]

    relation_scope = ocel.e2o(object_types=object_type)
    related_event_types = sorted(
        relation_scope.select(s.OCEL_EVENT_TYPE)
        .unique()
        .collect(engine="streaming")
        .get_column(s.OCEL_EVENT_TYPE)
        .to_list()
    )
    event_attrs = list(
        dict.fromkeys(
            attribute
            for event_type in related_event_types
            for attribute in ocel.event_attribute_names(event_type)
        )
    )
    output_columns = [
        "case:concept:name",
        *(f"case:{name}" for name in case_attrs),
        "concept:name",
        "time:timestamp",
        *state_attrs,
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

    relations = (
        relation_scope
        .select(
            pl.col(s.OCEL_OBJECT_ID).alias("case:concept:name"),
            s.OCEL_EVENT_ID,
        )
        .unique(subset=["case:concept:name", s.OCEL_EVENT_ID])
    )
    selected_events = (
        ocel.events(*related_event_types)
        if related_event_types
        else ocel.events().filter(pl.lit(False))
    )
    events = selected_events.select(
        pl.col(s.OCEL_ID).alias(s.OCEL_EVENT_ID),
        pl.col(s.OCEL_TYPE).alias("concept:name"),
        pl.col(s.OCEL_TIME).alias("time:timestamp"),
        *(pl.col(name) for name in event_attrs),
    )
    result = relations.join(events, on=s.OCEL_EVENT_ID, how="inner").sort(
        "case:concept:name",
        "time:timestamp",
        s.OCEL_EVENT_ID,
    )

    related_objects = relations.select(
        pl.col("case:concept:name").alias(s.OCEL_ID)
    ).unique()
    changes = (
        ocel.object_changes()
        .filter(pl.col(s.OCEL_TYPE) == object_type)
        .join(related_objects, on=s.OCEL_ID, how="semi")
    )

    if state_attrs:
        states = ocel.object_states(object_type).join(
            related_objects,
            on=s.OCEL_ID,
            how="semi",
            maintain_order="left",
        )
        state_values = states.select(
            pl.col(s.OCEL_ID).alias("case:concept:name"),
            pl.col(s.OCEL_TIME).alias("time:timestamp"),
            *(pl.col(name) for name in state_attrs),
        )
        result = result.join_asof(
            state_values,
            on="time:timestamp",
            by="case:concept:name",
            strategy="backward",
            check_sortedness=False,
        )

    if case_attrs:
        initial_values = changes.filter(pl.col(s.OCEL_IS_INITIAL)).select(
            pl.col(s.OCEL_ID).alias("case:concept:name"),
            *(pl.col(name).alias(f"case:{name}") for name in case_attrs),
        )
        result = result.join(
            initial_values,
            on="case:concept:name",
            how="left",
            maintain_order="left",
        )

    return result.select(*output_columns)


def _require_object_type(ocel: OCEL, object_type: object) -> None:
    if not isinstance(object_type, str):
        raise TypeError("object_type must be a string.")
    if not object_type:
        raise ValueError("object_type must not be empty.")
    if not ocel.objects(object_type).limit(1).collect().height:
        raise ValueError(f"Unknown object type {object_type!r}.")
