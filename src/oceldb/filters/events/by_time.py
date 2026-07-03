"""filter_events_by_time: keep or remove events within a time window."""

from collections.abc import Callable, Iterable
from typing import Literal, overload

import polars as pl

from oceldb import schema as s
from oceldb.utils._step import _step
from oceldb.ocel import OCEL


@overload
def filter_events_by_time(
    ocel: OCEL,
    *,
    start: pl.Series | str | None = ...,
    end: pl.Series | str | None = ...,
    event_types: str | Iterable[str] | None = ...,
    mode: Literal["include", "exclude"] = ...,
) -> OCEL: ...


@overload
def filter_events_by_time(
    *,
    start: pl.Series | str | None = ...,
    end: pl.Series | str | None = ...,
    event_types: str | Iterable[str] | None = ...,
    mode: Literal["include", "exclude"] = ...,
) -> Callable[[OCEL], OCEL]: ...


@_step
def filter_events_by_time(
    ocel: OCEL,
    *,
    start: pl.Series | str | None = None,
    end: pl.Series | str | None = None,
    event_types: str | Iterable[str] | None = None,
    mode: Literal["include", "exclude"] = "include",
) -> OCEL:
    """Keep or remove events within a time window and prune the connected core.

    Convenience wrapper around :func:`filter_events_by_attribute` for filtering
    on ``ocel_time``.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        start: Inclusive lower bound on ``ocel_time``. ``None`` means no lower
            bound.
        end: Inclusive upper bound on ``ocel_time``. ``None`` means no upper
            bound.
        event_types: Event type(s) the filter applies to. ``None`` (default)
            applies it to all events; events of other types pass through
            unchanged.
        mode: ``"include"`` (default) keeps events inside the window;
            ``"exclude"`` keeps events outside it.

    Examples:
        >>> from oceldb.filters import filter_events_by_time
        >>> sub = filter_events_by_time(ocel, start="2023-01-01", end="2023-06-30")
        >>> sub = ocel >> filter_events_by_time(end="2023-01-01", mode="exclude")
    """
    from oceldb.filters.events.by_attribute import filter_events_by_attribute

    pred = pl.lit(True)
    if start is not None:
        pred = pred & (pl.col(s.OCEL_TIME) >= _bound(start))
    if end is not None:
        pred = pred & (pl.col(s.OCEL_TIME) <= _bound(end))
    return filter_events_by_attribute(ocel, pred, event_types=event_types, mode=mode)


def _bound(value: pl.Series | str) -> pl.Expr | pl.Series:
    """Parse an ISO 8601 string bound to a datetime; pass a Series through."""
    if isinstance(value, str):
        return pl.lit(value).str.to_datetime()
    return value
