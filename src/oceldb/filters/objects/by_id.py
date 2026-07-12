"""filter_objects_by_id: keep or remove objects by ocel_id."""

from collections.abc import Iterable
from typing import Literal

import polars as pl

from oceldb import schema as s
from oceldb.utils.step import step
from oceldb.filters._utils import (
    _filter_objects_direct,
    normalize_scope,
    scoped_match,
)
from oceldb.ocel import OCEL


@step
def filter_objects_by_id(
    ocel: OCEL,
    *ids: str,
    object_types: str | Iterable[str] | None = None,
    mode: Literal["include", "exclude"] = "include",
) -> OCEL:
    """Keep or remove objects whose ``ocel_id`` is in *ids*.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        *ids: Object identifiers to include or exclude.
        object_types: Object type(s) the filter applies to. ``None`` (default)
            applies it to all objects; objects of other types pass through
            unchanged.
        mode: ``"include"`` (default) keeps matching objects; ``"exclude"``
            removes them.

    Examples:
        >>> from oceldb.filters import filter_objects_by_id
        >>> sub = filter_objects_by_id(ocel, "order-42", "order-99")
        >>> sub = ocel >> filter_objects_by_id("order-42", mode="exclude")
    """
    scope = normalize_scope(object_types)
    keep = scoped_match(
        pl.col(s.OCEL_ID).is_in(list(ids)),
        type_col=s.OCEL_TYPE,
        scope=scope,
        mode=mode,
    )
    return _filter_objects_direct(ocel, keep)
