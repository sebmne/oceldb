"""filter_object_ids: keep or remove objects by ocel_id."""

from collections.abc import Callable
from typing import Literal, overload

import polars as pl

from oceldb import schema as s
from oceldb.filters._step import _step
from oceldb.filters._utils import _filter_objects_direct
from oceldb.ocel import OCEL


@overload
def filter_object_ids(ocel: OCEL, *ids: str, mode: Literal["include", "exclude"] = ...) -> OCEL: ...


@overload
def filter_object_ids(*ids: str, mode: Literal["include", "exclude"] = ...) -> Callable[[OCEL], OCEL]: ...


@_step
def filter_object_ids(
    ocel: OCEL, *ids: str, mode: Literal["include", "exclude"] = "include"
) -> OCEL:
    """Keep or remove objects whose ``ocel_id`` is in *ids*.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        *ids: Object identifiers to include or exclude.
        mode: ``"include"`` (default) keeps matching objects; ``"exclude"``
            removes them.

    Examples:
        >>> from oceldb.filters import filter_object_ids
        >>> sub = filter_object_ids(ocel, "order-42", "order-99")
        >>> sub = ocel >> filter_object_ids("order-42", mode="exclude")
    """
    predicate = pl.col(s.OCEL_ID).is_in(list(ids))
    if mode == "exclude":
        predicate = ~predicate
    return _filter_objects_direct(ocel, predicate)
