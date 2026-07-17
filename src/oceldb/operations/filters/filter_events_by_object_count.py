"""filter_events_by_object_count: keep or remove events by related-object count."""

import polars as pl

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations.filters._utils import (
    Mode,
    TypeScope,
    normalize_scope,
    scoped_match,
    within_bounds,
)
from oceldb.operations.pruning import sublog_from_event_ids
from oceldb.operations.step import step


@step
def filter_events_by_object_count(
    ocel: OCEL,
    *,
    min_count: int | None = None,
    max_count: int | None = None,
    object_types: TypeScope = None,
    event_types: TypeScope = None,
    mode: Mode = "include",
) -> OCEL:
    """Keep or remove events by their number of distinct related objects.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        min_count: Inclusive lower bound on distinct related objects. Omit
            for no lower bound.
        max_count: Inclusive upper bound on distinct related objects. Omit
            for no upper bound.
        object_types: Only relations to these object types count toward an
            event's count. ``None`` counts every object type.
        event_types: Limits which event types the count applies to; events
            of other types are left untouched. ``None`` applies it to every
            type.
        mode: ``"include"`` keeps events within bounds; ``"exclude"``
            removes them.

    Returns:
        A new ``OCEL`` pruned to the surviving events and their connected
        core.

    Raises:
        ValueError: If neither *min_count* nor *max_count* is given, or
            *min_count* exceeds *max_count*.

    Examples:
        >>> from oceldb.operations.filters import filter_events_by_object_count
        >>> sub = filter_events_by_object_count(ocel, min_count=2)
        >>> sub = ocel >> filter_events_by_object_count(max_count=1, object_types="order")
    """
    in_bounds = within_bounds(min_count=min_count, max_count=max_count)
    e2o = ocel.e2o()
    if object_types is not None:
        object_scope = normalize_scope(object_types)
        assert object_scope is not None
        e2o = e2o.filter(pl.col(s.OCEL_OBJECT_TYPE).is_in(object_scope))

    counts = e2o.group_by(s.OCEL_EVENT_ID).agg(
        pl.col(s.OCEL_OBJECT_ID).n_unique().alias("_count")
    )
    event_counts = (
        ocel.events()
        .select(pl.col(s.OCEL_ID).alias(s.OCEL_EVENT_ID), s.OCEL_TYPE)
        .join(counts, on=s.OCEL_EVENT_ID, how="left")
        .with_columns(pl.col("_count").fill_null(0))
    )
    keep = scoped_match(
        in_bounds,
        type_col=s.OCEL_TYPE,
        scope=normalize_scope(event_types),
        mode=mode,
    )
    return sublog_from_event_ids(
        ocel,
        event_counts.filter(keep).select(s.OCEL_EVENT_ID),
    )
