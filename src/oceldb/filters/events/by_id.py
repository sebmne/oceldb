"""filter_events_by_id: keep or remove events by ocel_id."""

from collections.abc import Callable, Iterable
from typing import Literal, overload

import polars as pl

from oceldb import schema as s
from oceldb.utils._step import _step
from oceldb.ocel import OCEL


@overload
def filter_events_by_id(
    ocel: OCEL,
    *ids: str,
    event_types: str | Iterable[str] | None = ...,
    mode: Literal["include", "exclude"] = ...,
) -> OCEL: ...


@overload
def filter_events_by_id(
    *ids: str,
    event_types: str | Iterable[str] | None = ...,
    mode: Literal["include", "exclude"] = ...,
) -> Callable[[OCEL], OCEL]: ...


@_step
def filter_events_by_id(
    ocel: OCEL,
    *ids: str,
    event_types: str | Iterable[str] | None = None,
    mode: Literal["include", "exclude"] = "include",
) -> OCEL:
    """Keep or remove events whose ``ocel_id`` is in *ids*.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        *ids: Event identifiers to include or exclude.
        event_types: Event type(s) the filter applies to. ``None`` (default)
            applies it to all events; events of other types pass through
            unchanged.
        mode: ``"include"`` (default) keeps matching events; ``"exclude"``
            removes them.

    Examples:
        >>> from oceldb.filters import filter_events_by_id
        >>> sub = filter_events_by_id(ocel, "e-001", "e-042")
        >>> sub = ocel >> filter_events_by_id("e-001", mode="exclude")
    """
    from oceldb.filters.events.by_attribute import filter_events_by_attribute

    return filter_events_by_attribute(
        ocel,
        pl.col(s.OCEL_ID).is_in(list(ids)),
        event_types=event_types,
        mode=mode,
    )
