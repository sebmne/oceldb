"""Compose event-type and object-type filters into an OCEL view."""

from collections.abc import Iterable

from oceldb.filters import filter_events_by_type, filter_objects_by_type
from oceldb.filters._utils import normalize_scope
from oceldb.core.step import step
from oceldb.ocel import OCEL


@step
def view(
    ocel: OCEL,
    *,
    event_types: str | Iterable[str] | None = None,
    object_types: str | Iterable[str] | None = None,
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
        >>> from oceldb.transformations import view
        >>> orders = view(ocel, object_types=["order", "item"])
        >>> paid = view(ocel, event_types=["Pay Order"], object_types=["order"])
        >>> result = ocel >> view(object_types="order") >> view(event_types="Pay Order")
    """
    result = ocel
    if event_types is not None:
        scope = normalize_scope(event_types)
        assert scope is not None
        result = filter_events_by_type(result, *scope)
    if object_types is not None:
        scope = normalize_scope(object_types)
        assert scope is not None
        result = filter_objects_by_type(result, *scope)
    return result
