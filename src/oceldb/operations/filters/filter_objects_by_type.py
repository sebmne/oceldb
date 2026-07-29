"""filter_objects_by_type: keep or remove objects by ``ocel_type``."""

import polars as pl

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations.filters._utils import (
    Mode,
    _filter_objects_direct,
    match_decision,
)
from oceldb.operations.step import step
from oceldb.types import normalize_strings


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
    selected = normalize_strings(types, name="types", non_empty=True)
    return _filter_objects_direct(
        ocel,
        object_predicate=match_decision(
            pl.col(s.OCEL_TYPE).is_in(selected),
            mode,
        ),
        relation_predicate=match_decision(
            pl.col(s.OCEL_OBJECT_TYPE).is_in(selected),
            mode,
        ),
    )
