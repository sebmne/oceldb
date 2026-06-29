"""filter_time: keep events within a time window."""

from collections.abc import Callable
from typing import overload

import polars as pl

from oceldb import schema as s
from oceldb.filters._step import _step
from oceldb.ocel import OCEL


@overload
def filter_time(
    ocel: OCEL,
    *,
    start: pl.Series | str | None = ...,
    end: pl.Series | str | None = ...,
) -> OCEL: ...


@overload
def filter_time(
    *,
    start: pl.Series | str | None = ...,
    end: pl.Series | str | None = ...,
) -> Callable[[OCEL], OCEL]: ...


@_step
def filter_time(
    ocel: OCEL,
    *,
    start: pl.Series | str | None = None,
    end: pl.Series | str | None = None,
) -> OCEL:
    """Keep events within a time window and prune everything left unconnected.

    Convenience wrapper around :func:`filter_events_by_attribute` for the
    common case of filtering on ``ocel_time`` across all event types.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        start: Inclusive lower bound on ``ocel_time``. ``None`` means no lower
            bound.
        end: Inclusive upper bound on ``ocel_time``. ``None`` means no upper
            bound.

    Examples:
        >>> from oceldb.filters import filter_time
        >>> sub = filter_time(ocel, start="2023-01-01", end="2023-06-30")
        >>> sub = ocel >> filter_time(start="2023-01-01")
    """
    from oceldb.filters.events.by_attribute import filter_events_by_attribute

    pred = pl.lit(True)
    if start is not None:
        pred = pred & (pl.col(s.OCEL_TIME) >= start)
    if end is not None:
        pred = pred & (pl.col(s.OCEL_TIME) <= end)
    return filter_events_by_attribute(ocel, pred)
