"""filter_events_by_attribute: keep events satisfying a predicate."""

from collections.abc import Callable, Iterable
from typing import overload

import polars as pl

from oceldb import schema as s
from oceldb.filters._step import _step
from oceldb.filters._utils import _to_list
from oceldb.ocel import OCEL


@overload
def filter_events_by_attribute(
    ocel: OCEL,
    predicate: pl.Expr,
    *,
    event_types: str | Iterable[str] | None = ...,
) -> OCEL: ...


@overload
def filter_events_by_attribute(
    predicate: pl.Expr,
    *,
    event_types: str | Iterable[str] | None = ...,
) -> Callable[[OCEL], OCEL]: ...


@_step
def filter_events_by_attribute(
    ocel: OCEL,
    predicate: pl.Expr,
    *,
    event_types: str | Iterable[str] | None = None,
) -> OCEL:
    """Keep events satisfying *predicate*, optionally scoped to *event_types*.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        predicate: A Polars expression evaluated against the events frame.
        event_types: Event type(s) to scope the filter to. When ``None``
            (default) the predicate is applied to all events. When supplied,
            only events of those types are filtered; events of other types
            pass through unchanged.

    Returns:
        A new ``OCEL`` with non-matching events removed and objects left
        unconnected pruned.

    Examples:
        >>> from oceldb.filters import filter_events_by_attribute
        >>> sub = filter_events_by_attribute(ocel, pl.col("amount") > 1000)
        >>> sub = filter_events_by_attribute(ocel, pl.col("amount") > 1000, event_types="Pay Order")
        >>> sub = ocel >> filter_events_by_attribute(pl.col("amount") > 1000, event_types=["Pay Order", "Ship"])
    """
    if event_types is None:
        events = ocel.events().filter(predicate)
    else:
        types = _to_list(event_types)
        events = ocel.events().filter((~pl.col(s.OCEL_TYPE).is_in(types)) | predicate)
    kept_events = events.select(pl.col(s.OCEL_ID).alias(s.OCEL_EVENT_ID))
    relations = ocel.event_object().join(kept_events, on=s.OCEL_EVENT_ID, how="semi")
    kept_objects = relations.select(s.OCEL_OBJECT_ID).unique()
    return OCEL(
        events=events,
        objects=ocel.objects().join(
            kept_objects, left_on=s.OCEL_ID, right_on=s.OCEL_OBJECT_ID, how="semi"
        ),
        object_changes=ocel.object_changes().join(
            kept_objects, left_on=s.OCEL_ID, right_on=s.OCEL_OBJECT_ID, how="semi"
        ),
        o2o=ocel.object_object()
        .join(
            kept_objects,
            left_on=s.OCEL_SOURCE_ID,
            right_on=s.OCEL_OBJECT_ID,
            how="semi",
        )
        .join(
            kept_objects,
            left_on=s.OCEL_TARGET_ID,
            right_on=s.OCEL_OBJECT_ID,
            how="semi",
        ),
        e2o=relations,
    )
