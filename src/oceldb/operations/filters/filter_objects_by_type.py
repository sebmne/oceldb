"""filter_objects_by_type: keep or remove objects by ``ocel_type``."""

import polars as pl

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations.filters._utils import Mode, match_decision
from oceldb.operations.pruning import prune_log
from oceldb.operations.step import step


@step
def filter_objects_by_type(
    ocel: OCEL,
    *types: str,
    mode: Mode = "include",
) -> OCEL:
    """Keep or remove objects whose ``ocel_type`` is in *types*.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        *types: One or more object type names.
        mode: ``"include"`` keeps matching types; ``"exclude"`` removes
            them.

    Returns:
        A new ``OCEL`` pruned to the surviving objects and their connected
        core.

    Examples:
        >>> from oceldb.operations.filters import filter_objects_by_type
        >>> sub = filter_objects_by_type(ocel, "order")
        >>> sub = ocel >> filter_objects_by_type("order", mode="exclude")
    """
    keep = match_decision(pl.col(s.OCEL_OBJECT_TYPE).is_in(list(types)), mode)
    objects = ocel.objects().filter(
        match_decision(pl.col(s.OCEL_TYPE).is_in(list(types)), mode)
    )
    relations = ocel.e2o().filter(keep)
    kept_events = relations.select(s.OCEL_EVENT_ID).unique()
    events = ocel.events().join(
        kept_events,
        left_on=s.OCEL_ID,
        right_on=s.OCEL_EVENT_ID,
        how="semi",
    )
    return prune_log(ocel, events=events, objects=objects, e2o=relations)
