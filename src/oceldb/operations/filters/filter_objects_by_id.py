"""filter_objects_by_id: keep or remove objects by ``ocel_id``."""

import polars as pl

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations.filters._utils import (
    Mode,
    _filter_objects_direct,
    scoped_match,
)
from oceldb.operations.step import step
from oceldb.types import OneOrMany, normalize_strings


@step
def filter_objects_by_id(
    ocel: OCEL,
    *ids: str,
    object_types: OneOrMany[str] | None = None,
    mode: Mode = "include",
) -> OCEL:
    """Keep or remove objects whose ``ocel_id`` is in *ids*.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        *ids: One or more ``ocel_id`` values.
        object_types: Limits which object types *ids* is matched against;
            objects of other types are left untouched. ``None`` matches
            every type.
        mode: ``"include"`` keeps matching objects; ``"exclude"`` removes
            them.

    Returns:
        A new ``OCEL`` pruned to the surviving objects and their connected
        core.

    Examples:
        >>> from oceldb.operations.filters import filter_objects_by_id
        >>> sub = filter_objects_by_id(ocel, "o1", "o2")
        >>> sub = ocel >> filter_objects_by_id("o1", mode="exclude")
    """
    selected = normalize_strings(ids, name="ids", non_empty=True)
    scope = normalize_strings(object_types, name="object_types", non_empty=True)
    keep = scoped_match(
        pl.col(s.OCEL_ID).is_in(selected),
        type_col=s.OCEL_TYPE,
        scope=scope,
        mode=mode,
    )
    relation_keep = scoped_match(
        pl.col(s.OCEL_OBJECT_ID).is_in(selected),
        type_col=s.OCEL_OBJECT_TYPE,
        scope=scope,
        mode=mode,
    )
    return _filter_objects_direct(
        ocel,
        object_predicate=keep,
        relation_predicate=relation_keep,
    )
