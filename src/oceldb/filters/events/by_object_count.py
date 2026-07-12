"""filter_events_by_object_count: keep events by number of related objects."""

from collections.abc import Iterable
from typing import Literal

import polars as pl

from oceldb import schema as s
from oceldb.utils import to_list
from oceldb.utils.step import step
from oceldb.core.pruning import sublog_from_event_ids
from oceldb.filters._utils import normalize_scope, scoped_match, within_bounds
from oceldb.ocel import OCEL


@step
def filter_events_by_object_count(
    ocel: OCEL,
    *,
    min_count: int | None = None,
    max_count: int | None = None,
    object_types: str | Iterable[str] | None = None,
    event_types: str | Iterable[str] | None = None,
    mode: Literal["include", "exclude"] = "include",
) -> OCEL:
    """Keep events whose object count satisfies the given bounds.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        min_count: Inclusive lower bound on the number of objects per event.
        max_count: Inclusive upper bound on the number of objects per event.
        object_types: Restrict the *count* to objects of these types only.
            ``None`` counts all objects regardless of type.
        event_types: Event type(s) the filter applies to. ``None`` (default)
            applies it to all events; events of other types pass through
            unchanged.
        mode: ``"include"`` (default) keeps events whose count is within bounds;
            ``"exclude"`` keeps events whose count is outside them.

    Examples:
        >>> from oceldb.filters import filter_events_by_object_count
        >>> sub = filter_events_by_object_count(ocel, min_count=2)
        >>> sub = ocel >> filter_events_by_object_count(min_count=1, object_types="order")
    """
    e2o = ocel.event_object()
    if object_types is not None:
        e2o = e2o.filter(pl.col(s.OCEL_OBJECT_TYPE).is_in(to_list(object_types)))

    counts = e2o.group_by(s.OCEL_EVENT_ID).agg(pl.len().alias("_count"))
    event_counts = (
        ocel.events()
        .select(pl.col(s.OCEL_ID).alias(s.OCEL_EVENT_ID), s.OCEL_TYPE)
        .join(counts, on=s.OCEL_EVENT_ID, how="left")
        .with_columns(pl.col("_count").fill_null(0))
    )

    in_bounds = within_bounds(min_count=min_count, max_count=max_count)
    scope = normalize_scope(event_types)
    keep = scoped_match(in_bounds, type_col=s.OCEL_TYPE, scope=scope, mode=mode)

    kept_events = event_counts.filter(keep).select(s.OCEL_EVENT_ID)
    return sublog_from_event_ids(ocel, kept_events)
