"""Reconstruct forward-filled object attribute states from ``object_changes``."""

import polars as pl

from oceldb.core import schema as s


def reconstruct_attribute_states(
    object_changes: pl.LazyFrame,
    types: tuple[str, ...] = (),
) -> pl.LazyFrame:
    """Forward-fill each object's attributes into one row per recorded change.

    Each returned row is that object's complete attribute state as of that
    change's ``ocel_time`` — not just the field that changed. Used by
    ``filter_objects_by_attribute`` and ``flatten`` to query or join against
    object state at a point in time.

    Args:
        object_changes: An object_changes frame (e.g. ``ocel.object_changes()``).
        types: Object types to include. Empty (default) includes every type.

    Returns:
        A lazy frame of ``ocel_id``, ``ocel_time``, ``ocel_type``, and the
        forward-filled attribute columns, sorted by
        ``(ocel_type, ocel_id, ocel_time)``.
    """
    states, attributes = _forward_fill(object_changes, types)
    return states.select(s.OCEL_ID, s.OCEL_TIME, *attributes, s.OCEL_TYPE).sort(
        s.OCEL_TYPE, s.OCEL_ID, s.OCEL_TIME
    )


def _forward_fill(
    object_changes: pl.LazyFrame, types: tuple[str, ...]
) -> tuple[pl.LazyFrame, list[str]]:
    """Collapse same-instant changes and forward-fill each attribute per object."""
    core = {s.OCEL_ID, s.OCEL_TIME, s.OCEL_CHANGED_FIELD, s.OCEL_IS_INITIAL, s.OCEL_TYPE}
    attributes = [
        name for name in object_changes.collect_schema().names() if name not in core
    ]
    frame = object_changes
    if types:
        frame = frame.filter(pl.col(s.OCEL_TYPE).is_in(types))
    keys = [s.OCEL_TYPE, s.OCEL_ID, s.OCEL_TIME]
    if not attributes:
        return frame.select(keys).unique(), attributes
    collapsed = frame.group_by(keys).agg(
        pl.col(attribute).drop_nulls().last().alias(attribute)
        for attribute in attributes
    )
    return (
        collapsed.with_columns(
            pl.col(attribute)
            .forward_fill()
            .over([s.OCEL_TYPE, s.OCEL_ID], order_by=s.OCEL_TIME)
            for attribute in attributes
        ),
        attributes,
    )
