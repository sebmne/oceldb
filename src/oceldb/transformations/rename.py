"""rename_types: relabel event and object type names across a log."""

from collections.abc import Mapping

import polars as pl

from oceldb import schema as s
from oceldb.core.dataset import OCELTable
from oceldb.core.step import step
from oceldb.ocel import OCEL


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
        >>> from oceldb.transformations import rename_types
        >>> renamed = rename_types(ocel, events={"place order": "Place Order"})
        >>> renamed = ocel >> rename_types(objects={"orders": "Order"})
    """
    event_map = _normalize_mapping("events", events)
    object_map = _normalize_mapping("objects", objects)
    if event_map is None and object_map is None:
        raise ValueError("Pass a non-empty events or objects mapping.")
    tables = ocel._dataset.tables
    return OCEL(
        ocel._dataset.with_tables(
            events=_remap_table(
                tables.events,
                s.OCEL_TYPE,
                event_map,
                partition_names=event_map,
            ),
            objects=_remap_table(
                tables.objects,
                s.OCEL_TYPE,
                object_map,
                partition_names=object_map,
            ),
            object_changes=_remap_table(
                tables.object_changes,
                s.OCEL_TYPE,
                object_map,
                partition_names=object_map,
            ),
            event_object=_remap_table(
                _remap_table(tables.event_object, s.OCEL_EVENT_TYPE, event_map),
                s.OCEL_OBJECT_TYPE,
                object_map,
            ),
            object_object=_remap_table(
                _remap_table(tables.object_object, s.OCEL_SOURCE_TYPE, object_map),
                s.OCEL_TARGET_TYPE,
                object_map,
            ),
        )
    )


def _remap_table(
    table: OCELTable,
    column: str,
    mapping: dict[str, str] | None,
    *,
    partition_names: dict[str, str] | None = None,
) -> OCELTable:
    if not mapping:
        return table
    return table.map_partitions(
        lambda frame: frame.with_columns(pl.col(column).replace(mapping)),
        names=partition_names,
    )


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
