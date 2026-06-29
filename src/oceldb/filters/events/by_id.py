"""filter_event_ids: keep or remove events by ocel_id."""

from collections.abc import Callable
from typing import Literal, overload

import polars as pl

from oceldb import schema as s
from oceldb.filters._step import _step
from oceldb.ocel import OCEL


@overload
def filter_event_ids(ocel: OCEL, *ids: str, mode: Literal["include", "exclude"] = ...) -> OCEL: ...


@overload
def filter_event_ids(*ids: str, mode: Literal["include", "exclude"] = ...) -> Callable[[OCEL], OCEL]: ...


@_step
def filter_event_ids(
    ocel: OCEL, *ids: str, mode: Literal["include", "exclude"] = "include"
) -> OCEL:
    """Keep or remove events whose ``ocel_id`` is in *ids*.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        *ids: Event identifiers to include or exclude.
        mode: ``"include"`` (default) keeps matching events; ``"exclude"``
            removes them.

    Examples:
        >>> from oceldb.filters import filter_event_ids
        >>> sub = filter_event_ids(ocel, "e-001", "e-042")
        >>> sub = ocel >> filter_event_ids("e-001", mode="exclude")
    """
    from oceldb.filters.events.by_attribute import filter_events_by_attribute

    predicate = pl.col(s.OCEL_ID).is_in(list(ids))
    if mode == "exclude":
        predicate = ~predicate
    return filter_events_by_attribute(ocel, predicate)
