"""filter_events_by_object_count: keep events by number of related objects."""

from collections.abc import Callable, Iterable
from typing import overload

import polars as pl

from oceldb import schema as s
from oceldb.filters._step import _step
from oceldb.filters._utils import _to_list
from oceldb.ocel import OCEL


@overload
def filter_events_by_object_count(
    ocel: OCEL,
    *,
    min_count: int | None = ...,
    max_count: int | None = ...,
    object_types: str | Iterable[str] | None = ...,
) -> OCEL: ...


@overload
def filter_events_by_object_count(
    *,
    min_count: int | None = ...,
    max_count: int | None = ...,
    object_types: str | Iterable[str] | None = ...,
) -> Callable[[OCEL], OCEL]: ...


@_step
def filter_events_by_object_count(
    ocel: OCEL,
    *,
    min_count: int | None = None,
    max_count: int | None = None,
    object_types: str | Iterable[str] | None = None,
) -> OCEL:
    """Keep events whose object count satisfies the given bounds.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        min_count: Inclusive lower bound on the number of objects per event.
        max_count: Inclusive upper bound on the number of objects per event.
        object_types: Restrict the count to objects of these types only.
            ``None`` counts all objects regardless of type.

    Examples:
        >>> from oceldb.filters import filter_events_by_object_count
        >>> sub = filter_events_by_object_count(ocel, min_count=2)
        >>> sub = ocel >> filter_events_by_object_count(min_count=1, max_count=3, object_types="order")
    """
    e2o = ocel.event_object()
    if object_types is not None:
        e2o = e2o.filter(pl.col(s.OCEL_OBJECT_TYPE).is_in(_to_list(object_types)))

    counts = e2o.group_by(s.OCEL_EVENT_ID).agg(pl.len().alias("_count"))
    event_counts = (
        ocel.events()
        .select(pl.col(s.OCEL_ID).alias(s.OCEL_EVENT_ID))
        .join(counts, on=s.OCEL_EVENT_ID, how="left")
        .with_columns(pl.col("_count").fill_null(0))
    )
    if min_count is not None:
        event_counts = event_counts.filter(pl.col("_count") >= min_count)
    if max_count is not None:
        event_counts = event_counts.filter(pl.col("_count") <= max_count)

    kept_events = event_counts.select(s.OCEL_EVENT_ID)
    relations = ocel.event_object().join(kept_events, on=s.OCEL_EVENT_ID, how="semi")
    kept_objects = relations.select(s.OCEL_OBJECT_ID).unique()
    return OCEL(
        events=ocel.events().join(
            kept_events, left_on=s.OCEL_ID, right_on=s.OCEL_EVENT_ID, how="semi"
        ),
        objects=ocel.objects().join(
            kept_objects, left_on=s.OCEL_ID, right_on=s.OCEL_OBJECT_ID, how="semi"
        ),
        object_changes=ocel.object_changes().join(
            kept_objects, left_on=s.OCEL_ID, right_on=s.OCEL_OBJECT_ID, how="semi"
        ),
        o2o=ocel.object_object()
        .join(kept_objects, left_on=s.OCEL_SOURCE_ID, right_on=s.OCEL_OBJECT_ID, how="semi")
        .join(kept_objects, left_on=s.OCEL_TARGET_ID, right_on=s.OCEL_OBJECT_ID, how="semi"),
        e2o=relations,
    )
