"""filter_objects_by_event_count: keep objects by number of related events."""

from collections.abc import Iterable
from typing import Literal

import polars as pl

from oceldb import schema as s
from oceldb.utils import to_list
from oceldb.utils.step import step
from oceldb.core.pruning import sublog_from_object_ids
from oceldb.filters._utils import normalize_scope, scoped_match, within_bounds
from oceldb.ocel import OCEL


@step
def filter_objects_by_event_count(
    ocel: OCEL,
    *,
    min_count: int | None = None,
    max_count: int | None = None,
    event_types: str | Iterable[str] | None = None,
    object_types: str | Iterable[str] | None = None,
    mode: Literal["include", "exclude"] = "include",
) -> OCEL:
    """Keep objects whose event count satisfies the given bounds.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        min_count: Inclusive lower bound on the number of events per object.
        max_count: Inclusive upper bound on the number of events per object.
        event_types: Restrict the *count* to events of these types only.
            ``None`` counts all events regardless of type.
        object_types: Object type(s) the filter applies to. ``None`` (default)
            applies it to all objects; objects of other types pass through
            unchanged.
        mode: ``"include"`` (default) keeps objects whose count is within bounds;
            ``"exclude"`` keeps objects whose count is outside them.

    Examples:
        >>> from oceldb.filters import filter_objects_by_event_count
        >>> sub = filter_objects_by_event_count(ocel, min_count=3)
        >>> sub = ocel >> filter_objects_by_event_count(min_count=1, event_types="Pay Order")
    """
    e2o = ocel.event_object()
    if event_types is not None:
        e2o = e2o.filter(pl.col(s.OCEL_EVENT_TYPE).is_in(to_list(event_types)))

    counts = e2o.group_by(s.OCEL_OBJECT_ID).agg(pl.len().alias("_count"))
    object_counts = (
        ocel.objects()
        .select(pl.col(s.OCEL_ID).alias(s.OCEL_OBJECT_ID), s.OCEL_TYPE)
        .join(counts, on=s.OCEL_OBJECT_ID, how="left")
        .with_columns(pl.col("_count").fill_null(0))
    )

    in_bounds = within_bounds(min_count=min_count, max_count=max_count)
    scope = normalize_scope(object_types)
    keep = scoped_match(in_bounds, type_col=s.OCEL_TYPE, scope=scope, mode=mode)

    kept_objects = object_counts.filter(keep).select(s.OCEL_OBJECT_ID)
    return sublog_from_object_ids(ocel, kept_objects)
