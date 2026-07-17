"""rename_types: relabel event and object type names across a log."""

from collections.abc import Mapping

import polars as pl

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations._utils import replace
from oceldb.operations.step import step


@step
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

    Raises:
        ValueError: If both mappings are omitted or empty, or a type name is
            empty.

    Examples:
        >>> from oceldb.operations.transformations import rename_types
        >>> renamed = rename_types(ocel, events={"place order": "Place Order"})
        >>> renamed = ocel >> rename_types(objects={"orders": "Order"})
    """
    event_map = _normalize_mapping("events", events)
    object_map = _normalize_mapping("objects", objects)
    if event_map is None and object_map is None:
        raise ValueError("Pass a non-empty events or objects mapping.")
    return replace(
        ocel,
        events=_remap(ocel.events(), {s.OCEL_TYPE: event_map}),
        objects=_remap(ocel.objects(), {s.OCEL_TYPE: object_map}),
        object_changes=_remap(ocel.object_changes(), {s.OCEL_TYPE: object_map}),
        e2o=_remap(
            ocel.e2o(),
            {s.OCEL_EVENT_TYPE: event_map, s.OCEL_OBJECT_TYPE: object_map},
        ),
        o2o=_remap(
            ocel.o2o(),
            {s.OCEL_SOURCE_TYPE: object_map, s.OCEL_TARGET_TYPE: object_map},
        ),
        # Renaming relabels type names, so narrowing's per-type keys must be
        # relabeled the same way or they'd silently stop matching.
        event_attributes=_remap_attributes(ocel._event_attributes, event_map),
        change_attributes=_remap_attributes(ocel._change_attributes, object_map),
    )


def _remap(
    frame: pl.LazyFrame,
    columns: Mapping[str, dict[str, str] | None],
) -> pl.LazyFrame:
    replacements = [
        pl.col(column).replace(mapping)
        for column, mapping in columns.items()
        if mapping
    ]
    if not replacements:
        return frame
    return frame.with_columns(replacements)


def _remap_attributes(
    attributes: dict[str, list[str]] | None, mapping: dict[str, str] | None
) -> dict[str, list[str]] | None:
    if attributes is None or not mapping:
        return attributes
    return {
        mapping.get(type_name, type_name): columns
        for type_name, columns in attributes.items()
    }


def _normalize_mapping(name: str, mapping: object) -> dict[str, str] | None:
    if mapping is None:
        return None
    if not isinstance(mapping, Mapping):
        raise TypeError(f"{name} must be a mapping or None.")
    result: dict[str, str] = {}
    for source, target in mapping.items():
        if not isinstance(source, str) or not isinstance(target, str):
            raise TypeError(f"{name} mappings must contain only strings.")
        if not source or not target:
            raise ValueError(f"{name} mappings must not contain empty type names.")
        result[source] = target
    if not result:
        return None
    return result
