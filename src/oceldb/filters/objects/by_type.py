"""filter_objects_by_type: keep or remove objects by ocel_type."""

from collections.abc import Callable
from typing import Literal, overload

import polars as pl

from oceldb import schema as s
from oceldb.utils._step import _step
from oceldb.filters._utils import _filter_objects_direct, _scoped_match
from oceldb.ocel import OCEL


@overload
def filter_objects_by_type(
    ocel: OCEL, *types: str, mode: Literal["include", "exclude"] = ...
) -> OCEL: ...


@overload
def filter_objects_by_type(
    *types: str, mode: Literal["include", "exclude"] = ...
) -> Callable[[OCEL], OCEL]: ...


@_step
def filter_objects_by_type(
    ocel: OCEL, *types: str, mode: Literal["include", "exclude"] = "include"
) -> OCEL:
    """Keep or remove objects whose ``ocel_type`` is in *types*.

    Object types are this filter's subject, so it takes no separate type scope —
    only ``mode``. It filters the objects table directly (no object-state
    traversal needed since ``ocel_type`` is static), then prunes events and
    relations.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        *types: Object type names to include or exclude.
        mode: ``"include"`` (default) keeps matching objects; ``"exclude"``
            removes them.

    Examples:
        >>> from oceldb.filters import filter_objects_by_type
        >>> sub = filter_objects_by_type(ocel, "order", "item")
        >>> sub = ocel >> filter_objects_by_type("order", mode="exclude")
    """
    keep = _scoped_match(
        pl.col(s.OCEL_TYPE).is_in(list(types)),
        type_col=s.OCEL_TYPE,
        scope=None,
        mode=mode,
    )
    return _filter_objects_direct(ocel, keep)
