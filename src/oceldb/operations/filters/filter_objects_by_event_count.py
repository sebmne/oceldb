"""filter_objects_by_event_count: keep or remove objects by related-event count."""

import polars as pl

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations.filters._utils import (
    Mode,
    distinct_counts,
    scoped_match,
    within_bounds,
)
from oceldb.operations.pruning import sublog_from_object_ids
from oceldb.operations.step import step
from oceldb.types import OneOrMany, normalize_strings


@step
def filter_objects_by_event_count(
    ocel: OCEL,
    *,
    object_types: OneOrMany[str] | None = None,
    event_types: OneOrMany[str] | None = None,
    min_count: int | None = None,
    max_count: int | None = None,
    mode: Mode = "include",
) -> OCEL:
    """Keep or remove objects by their number of distinct related events.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        object_types: Limits which object types the count applies to;
            objects of other types are left untouched. ``None`` applies it
            to every type.
        event_types: Only relations to these event types count toward an
            object's count. ``None`` counts every event type.
        min_count: Inclusive lower bound on distinct related events. Omit
            for no lower bound.
        max_count: Inclusive upper bound on distinct related events. Omit
            for no upper bound.
        mode: ``"include"`` keeps objects within bounds; ``"exclude"``
            removes them.

    Returns:
        A new ``OCEL`` pruned to the surviving objects and their connected
        core.

    Raises:
        ValueError: If neither *min_count* nor *max_count* is given, or
            *min_count* exceeds *max_count*.

    Examples:
        >>> from oceldb.operations.filters import filter_objects_by_event_count
        >>> sub = filter_objects_by_event_count(ocel, min_count=2)
        >>> sub = ocel >> filter_objects_by_event_count(max_count=1, event_types="Pay")
    """
    in_bounds = within_bounds(min_count=min_count, max_count=max_count)
    event_scope = normalize_strings(event_types, name="event_types", non_empty=True)
    e2o, sorted_pairs = ocel._e2o_object_oriented()
    e2o = OCEL._filter_relation(
        e2o,
        ((s.OCEL_EVENT_TYPE, event_scope),),
    )
    counts = distinct_counts(
        e2o,
        group=s.OCEL_OBJECT_ID,
        value=s.OCEL_EVENT_ID,
        sorted_pairs=sorted_pairs,
    )
    object_counts = (
        ocel.objects()
        .select(pl.col(s.OCEL_ID).alias(s.OCEL_OBJECT_ID), s.OCEL_TYPE)
        .join(counts, on=s.OCEL_OBJECT_ID, how="left")
        .with_columns(pl.col("_count").fill_null(0))
    )
    keep = scoped_match(
        in_bounds,
        type_col=s.OCEL_TYPE,
        scope=normalize_strings(object_types, name="object_types", non_empty=True),
        mode=mode,
    )
    return sublog_from_object_ids(
        ocel,
        object_counts.filter(keep).select(s.OCEL_OBJECT_ID),
    )
