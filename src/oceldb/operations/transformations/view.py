"""Compose event-type and object-type filters into an OCEL view."""

from oceldb.ocel import OCEL
from oceldb.operations.filters import filter_events_by_type, filter_objects_by_type
from oceldb.operations.step import step
from oceldb.types import OneOrMany, normalize_strings


@step
def view(
    ocel: OCEL,
    *,
    event_types: OneOrMany[str] | None = None,
    object_types: OneOrMany[str] | None = None,
) -> OCEL:
    """Create a view by composing the canonical type filters.

    Event types are filtered first, followed by object types. Omitting either
    scope skips that filter; omitting both returns the source log unchanged.
    This keeps view semantics identical to the corresponding public filters.

    Args:
        ocel: The log to take a view of. Omit to get a pipe step instead.
        event_types: Event types to keep. ``None`` keeps every event type.
        object_types: Object types to keep. ``None`` keeps every object type.

    Returns:
        The ``OCEL`` produced by applying the requested type filters.

    Examples:
        >>> from oceldb.operations.transformations import view
        >>> orders = view(ocel, object_types=["order", "item"])
        >>> paid = view(ocel, event_types=["Pay Order"], object_types=["order"])
        >>> result = ocel >> view(object_types="order") >> view(event_types="Pay Order")
    """
    result = ocel
    if event_types is not None:
        scope = normalize_strings(event_types, name="event_types", non_empty=True)
        result = filter_events_by_type(result, *scope)
    if object_types is not None:
        scope = normalize_strings(object_types, name="object_types", non_empty=True)
        result = filter_objects_by_type(result, *scope)
    return result
