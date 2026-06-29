"""filter_event_types: keep or remove events by ocel_type."""

from collections.abc import Callable
from typing import Literal, overload

import polars as pl

from oceldb import schema as s
from oceldb.filters._step import _step
from oceldb.ocel import OCEL


@overload
def filter_event_types(ocel: OCEL, *types: str, mode: Literal["include", "exclude"] = ...) -> OCEL: ...


@overload
def filter_event_types(*types: str, mode: Literal["include", "exclude"] = ...) -> Callable[[OCEL], OCEL]: ...


@_step
def filter_event_types(
    ocel: OCEL, *types: str, mode: Literal["include", "exclude"] = "include"
) -> OCEL:
    """Keep or remove events whose ``ocel_type`` is in *types*.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        *types: Event type names to include or exclude.
        mode: ``"include"`` (default) keeps matching events; ``"exclude"``
            removes them.

    Examples:
        >>> from oceldb.filters import filter_event_types
        >>> sub = filter_event_types(ocel, "Pay Order", "Ship")
        >>> sub = ocel >> filter_event_types("Pay Order", mode="exclude")
    """
    from oceldb.filters.events.by_attribute import filter_events_by_attribute

    predicate = pl.col(s.OCEL_TYPE).is_in(list(types))
    if mode == "exclude":
        predicate = ~predicate
    return filter_events_by_attribute(ocel, predicate)
