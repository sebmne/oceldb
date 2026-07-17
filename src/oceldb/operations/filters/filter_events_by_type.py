"""filter_events_by_type: keep or remove events by ``ocel_type``."""

import polars as pl

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations.filters._utils import Mode, match_decision
from oceldb.operations.pruning import sublog_from_relations
from oceldb.operations.step import step


@step
def filter_events_by_type(
    ocel: OCEL,
    *types: str,
    mode: Mode = "include",
) -> OCEL:
    """Keep or remove events whose ``ocel_type`` is in *types*.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        *types: One or more event type names.
        mode: ``"include"`` keeps matching types; ``"exclude"`` removes
            them.

    Returns:
        A new ``OCEL`` pruned to the surviving events and their connected
        core.

    Examples:
        >>> from oceldb.operations.filters import filter_events_by_type
        >>> sub = filter_events_by_type(ocel, "Place Order")
        >>> sub = ocel >> filter_events_by_type("Place Order", mode="exclude")
    """
    keep = match_decision(pl.col(s.OCEL_EVENT_TYPE).is_in(list(types)), mode)
    events = ocel.events().filter(
        match_decision(pl.col(s.OCEL_TYPE).is_in(list(types)), mode)
    )
    relations = ocel.e2o().filter(keep)
    return sublog_from_relations(ocel, relations, events=events)
