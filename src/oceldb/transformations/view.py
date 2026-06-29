"""View filter: restrict an OCEL to selected event and object types."""

from collections.abc import Callable, Iterable
from typing import overload

import polars as pl

from oceldb import schema as s
from oceldb.filters._step import _step
from oceldb.filters._utils import _to_list
from oceldb.ocel import OCEL


@overload
def view(
    ocel: OCEL,
    *,
    event_types: str | Iterable[str] | None = ...,
    object_types: str | Iterable[str] | None = ...,
) -> OCEL: ...


@overload
def view(
    *,
    event_types: str | Iterable[str] | None = ...,
    object_types: str | Iterable[str] | None = ...,
) -> Callable[[OCEL], OCEL]: ...


@_step
def view(
    ocel: OCEL,
    *,
    event_types: str | Iterable[str] | None = None,
    object_types: str | Iterable[str] | None = None,
) -> OCEL:
    """Create an object-centric view (sub-log) of *ocel* by selecting types.

    Restricts the log to the chosen event and object types and removes
    everything left unconnected: an event is kept only if it shares an
    event-to-object relation with a kept object, and an object is kept only if
    it shares one with a kept event. Events, objects, object changes and the
    E2O / O2O relations are all pruned to the survivors. This is the standard
    OCEL "view" / sublog construction.

    Args:
        ocel: The log to take a view of. Omit to get a pipe step instead.
        event_types: Event types to keep. ``None`` keeps every event type.
        object_types: Object types to keep. ``None`` keeps every object type.

    Returns:
        A new ``OCEL`` holding the connected core of the selection.

    Examples:
        >>> from oceldb.filters import view
        >>> orders = view(ocel, object_types=["order", "item"])
        >>> paid = view(ocel, event_types=["Pay Order"], object_types=["order"])
        >>> result = ocel >> view(object_types="order") >> view(event_types="Pay Order")
    """
    relations = ocel.event_object()
    if event_types is not None:
        relations = relations.filter(pl.col(s.OCEL_EVENT_TYPE).is_in(_to_list(event_types)))
    if object_types is not None:
        relations = relations.filter(pl.col(s.OCEL_OBJECT_TYPE).is_in(_to_list(object_types)))
    kept_events = relations.select(s.OCEL_EVENT_ID).unique()
    kept_objects = relations.select(s.OCEL_OBJECT_ID).unique()
    return OCEL(
        events=ocel.events().join(
            kept_events, left_on=s.OCEL_ID, right_on=s.OCEL_EVENT_ID, how="semi"
        ),
        objects=ocel.objects().join(
            kept_objects, left_on=s.OCEL_ID, right_on=s.OCEL_OBJECT_ID, how="semi"
        ),
        object_changes=ocel.object_changes().join(
            kept_objects, left_on=s.OCEL_ID, right_on=s.OCEL_OBJECT_ID, how="semi"
        ),
        o2o=ocel.object_object()
        .join(kept_objects, left_on=s.OCEL_SOURCE_ID, right_on=s.OCEL_OBJECT_ID, how="semi")
        .join(kept_objects, left_on=s.OCEL_TARGET_ID, right_on=s.OCEL_OBJECT_ID, how="semi"),
        e2o=relations,
    )
