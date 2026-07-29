"""filter_events_by_id: keep or remove events by ``ocel_id``."""

import polars as pl

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations.filters._utils import (
    Mode,
    _filter_events_direct,
    scoped_match,
)
from oceldb.operations.step import step
from oceldb.types import OneOrMany, normalize_strings


@step
def filter_events_by_id(
    ocel: OCEL,
    *ids: str,
    event_types: OneOrMany[str] | None = None,
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
    selected = normalize_strings(ids, name="ids", non_empty=True)
    scope = normalize_strings(event_types, name="event_types", non_empty=True)
    return _filter_events_direct(
        ocel,
        event_predicate=scoped_match(
            pl.col(s.OCEL_ID).is_in(selected),
            type_col=s.OCEL_TYPE,
            scope=scope,
            mode=mode,
        ),
        relation_predicate=scoped_match(
            pl.col(s.OCEL_EVENT_ID).is_in(selected),
            type_col=s.OCEL_EVENT_TYPE,
            scope=scope,
            mode=mode,
        ),
    )
