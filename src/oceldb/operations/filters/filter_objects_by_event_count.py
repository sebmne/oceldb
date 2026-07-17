"""filter_objects_by_event_count: keep or remove objects by related-event count."""

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
from oceldb.operations.pruning import sublog_from_object_ids
from oceldb.operations.step import step


@step
def filter_objects_by_event_count(
    ocel: OCEL,
    *,
    min_count: int | None = None,
    max_count: int | None = None,
    event_types: TypeScope = None,
    object_types: TypeScope = None,
    mode: Mode = "include",
) -> OCEL:
    """Keep or remove objects by their number of distinct related events.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        min_count: Inclusive lower bound on distinct related events. Omit
            for no lower bound.
        max_count: Inclusive upper bound on distinct related events. Omit
            for no upper bound.
        event_types: Only relations to these event types count toward an
            object's count. ``None`` counts every event type.
        object_types: Limits which object types the count applies to;
            objects of other types are left untouched. ``None`` applies it
            to every type.
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
    e2o = ocel.e2o()
    if event_types is not None:
        event_scope = normalize_scope(event_types)
        assert event_scope is not None
        e2o = e2o.filter(pl.col(s.OCEL_EVENT_TYPE).is_in(event_scope))

    counts = e2o.group_by(s.OCEL_OBJECT_ID).agg(
        pl.col(s.OCEL_EVENT_ID).n_unique().alias("_count")
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
        scope=normalize_scope(object_types),
        mode=mode,
    )
    return sublog_from_object_ids(
        ocel,
        object_counts.filter(keep).select(s.OCEL_OBJECT_ID),
    )
