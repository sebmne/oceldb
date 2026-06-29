"""filter_object_types: keep or remove objects by ocel_type."""

from collections.abc import Callable
from typing import Literal, overload

import polars as pl

from oceldb import schema as s
from oceldb.filters._step import _step
from oceldb.filters._utils import _filter_objects_direct
from oceldb.ocel import OCEL


@overload
def filter_object_types(ocel: OCEL, *types: str, mode: Literal["include", "exclude"] = ...) -> OCEL: ...


@overload
def filter_object_types(*types: str, mode: Literal["include", "exclude"] = ...) -> Callable[[OCEL], OCEL]: ...


@_step
def filter_object_types(
    ocel: OCEL, *types: str, mode: Literal["include", "exclude"] = "include"
) -> OCEL:
    """Keep or remove objects whose ``ocel_type`` is in *types*.

    Filters the objects table directly (no object-state traversal needed since
    ``ocel_type`` is static), then prunes events and relations.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        *types: Object type names to include or exclude.
        mode: ``"include"`` (default) keeps matching objects; ``"exclude"``
            removes them.

    Examples:
        >>> from oceldb.filters import filter_object_types
        >>> sub = filter_object_types(ocel, "order", "item")
        >>> sub = ocel >> filter_object_types("order", mode="exclude")
    """
    predicate = pl.col(s.OCEL_TYPE).is_in(list(types))
    if mode == "exclude":
        predicate = ~predicate
    return _filter_objects_direct(ocel, predicate)
