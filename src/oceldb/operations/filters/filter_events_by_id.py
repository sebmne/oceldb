"""filter_events_by_id: keep or remove events by ``ocel_id``."""

import polars as pl

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations.filters._utils import Mode, TypeScope
from oceldb.operations.filters.filter_events_by_attribute import (
    filter_events_by_attribute,
)
from oceldb.operations.step import step


@step
def filter_events_by_id(
    ocel: OCEL,
    *ids: str,
    event_types: TypeScope = None,
    mode: Mode = "include",
) -> OCEL:
    """Keep or remove events whose ``ocel_id`` is in *ids*.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        *ids: One or more ``ocel_id`` values.
        event_types: Limits which event types *ids* is matched against;
            events of other types are left untouched. ``None`` matches
            every type.
        mode: ``"include"`` keeps matching events; ``"exclude"`` removes
            them.

    Returns:
        A new ``OCEL`` pruned to the surviving events and their connected
        core.

    Examples:
        >>> from oceldb.operations.filters import filter_events_by_id
        >>> sub = filter_events_by_id(ocel, "e1", "e2")
        >>> sub = ocel >> filter_events_by_id("e1", mode="exclude")
    """
    return filter_events_by_attribute(
        ocel,
        pl.col(s.OCEL_ID).is_in(list(ids)),
        event_types=event_types,
        mode=mode,
    )
