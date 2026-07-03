"""rename_types: relabel event and object type names across a log."""

from collections.abc import Callable, Mapping
from typing import overload

import polars as pl

from oceldb import schema as s
from oceldb.utils._step import _step
from oceldb.ocel import OCEL


@overload
def rename_types(
    ocel: OCEL,
    *,
    events: Mapping[str, str] | None = ...,
    objects: Mapping[str, str] | None = ...,
) -> OCEL: ...


@overload
def rename_types(
    *,
    events: Mapping[str, str] | None = ...,
    objects: Mapping[str, str] | None = ...,
) -> Callable[[OCEL], OCEL]: ...


@_step
def rename_types(
    ocel: OCEL,
    *,
    events: Mapping[str, str] | None = None,
    objects: Mapping[str, str] | None = None,
) -> OCEL:
    """Rename event and/or object types consistently across every table.

    Event-type renames update the ``events`` table and the E2O event type;
    object-type renames update ``objects``, ``object_changes``, the E2O object
    type, and both O2O endpoint types. Type names not present in a mapping are
    left unchanged, and identifiers, attributes, and relations are untouched.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        events: Mapping of old event type name to new event type name.
        objects: Mapping of old object type name to new object type name.

    Returns:
        A new ``OCEL`` with the requested type names replaced.

    Examples:
        >>> from oceldb.transformations import rename_types
        >>> renamed = rename_types(ocel, events={"place order": "Place Order"})
        >>> renamed = ocel >> rename_types(objects={"orders": "Order"})
    """
    event_map = dict(events) if events else None
    object_map = dict(objects) if objects else None
    return OCEL(
        events=_remap(ocel.events(), s.OCEL_TYPE, event_map),
        objects=_remap(ocel.objects(), s.OCEL_TYPE, object_map),
        object_changes=_remap(ocel.object_changes(), s.OCEL_TYPE, object_map),
        e2o=_remap(
            _remap(ocel.event_object(), s.OCEL_EVENT_TYPE, event_map),
            s.OCEL_OBJECT_TYPE,
            object_map,
        ),
        o2o=_remap(
            _remap(ocel.object_object(), s.OCEL_SOURCE_TYPE, object_map),
            s.OCEL_TARGET_TYPE,
            object_map,
        ),
    )


def _remap(
    frame: pl.LazyFrame, column: str, mapping: dict[str, str] | None
) -> pl.LazyFrame:
    if not mapping:
        return frame
    return frame.with_columns(pl.col(column).replace(mapping))
