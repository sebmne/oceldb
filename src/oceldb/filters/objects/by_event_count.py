"""filter_objects_by_event_count: keep objects by number of related events."""

from collections.abc import Callable, Iterable
from typing import overload

import polars as pl

from oceldb import schema as s
from oceldb.filters._step import _step
from oceldb.filters._utils import _to_list
from oceldb.ocel import OCEL


@overload
def filter_objects_by_event_count(
    ocel: OCEL,
    *,
    min_count: int | None = ...,
    max_count: int | None = ...,
    event_types: str | Iterable[str] | None = ...,
) -> OCEL: ...


@overload
def filter_objects_by_event_count(
    *,
    min_count: int | None = ...,
    max_count: int | None = ...,
    event_types: str | Iterable[str] | None = ...,
) -> Callable[[OCEL], OCEL]: ...


@_step
def filter_objects_by_event_count(
    ocel: OCEL,
    *,
    min_count: int | None = None,
    max_count: int | None = None,
    event_types: str | Iterable[str] | None = None,
) -> OCEL:
    """Keep objects whose event count satisfies the given bounds.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        min_count: Inclusive lower bound on the number of events per object.
        max_count: Inclusive upper bound on the number of events per object.
        event_types: Restrict the count to events of these types only.
            ``None`` counts all events regardless of type.

    Examples:
        >>> from oceldb.filters import filter_objects_by_event_count
        >>> sub = filter_objects_by_event_count(ocel, min_count=3)
        >>> sub = ocel >> filter_objects_by_event_count(min_count=1, event_types="Pay Order")
    """
    e2o = ocel.event_object()
    if event_types is not None:
        e2o = e2o.filter(pl.col(s.OCEL_EVENT_TYPE).is_in(_to_list(event_types)))

    counts = e2o.group_by(s.OCEL_OBJECT_ID).agg(pl.len().alias("_count"))
    object_counts = (
        ocel.objects()
        .select(pl.col(s.OCEL_ID).alias(s.OCEL_OBJECT_ID))
        .join(counts, on=s.OCEL_OBJECT_ID, how="left")
        .with_columns(pl.col("_count").fill_null(0))
    )
    if min_count is not None:
        object_counts = object_counts.filter(pl.col("_count") >= min_count)
    if max_count is not None:
        object_counts = object_counts.filter(pl.col("_count") <= max_count)

    kept_objects = object_counts.select(s.OCEL_OBJECT_ID)
    relations = ocel.event_object().join(kept_objects, on=s.OCEL_OBJECT_ID, how="semi")
    kept_events = relations.select(s.OCEL_EVENT_ID).unique()
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
