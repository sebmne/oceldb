"""Lazy reconstruction of object attribute states."""

import polars as pl

from oceldb.core import schema as s


def reconstruct_object_states(object_changes: pl.LazyFrame) -> pl.LazyFrame:
    """Return complete object states at every recorded change timestamp.

    Multiple attribute changes for the same object and timestamp are collapsed
    into one row. Attribute values are then carried forward independently for
    each object. A change whose named attribute is null is an explicit
    tombstone and resets that attribute instead of carrying its old value
    forward.
    """
    attributes = [
        name
        for name in object_changes.collect_schema().names()
        if name not in s.CHANGE_SCHEMA
    ]
    keys = [s.OCEL_TYPE, s.OCEL_ID, s.OCEL_TIME]
    if not attributes:
        return (
            object_changes.select(keys)
            .unique()
            .sort(*keys)
            .select(
                s.OCEL_ID,
                s.OCEL_TIME,
                s.OCEL_TYPE,
            )
        )

    updates: list[pl.Expr] = []
    for attribute in attributes:
        value = pl.col(attribute).drop_nulls().last()
        has_value = pl.col(attribute).is_not_null().any()
        is_tombstone = (
            (pl.col(s.OCEL_CHANGED_FIELD) == attribute)
            & pl.col(attribute).is_null()
        ).any()
        updates.append(
            pl.when(has_value | is_tombstone)
            .then(pl.struct(value.alias("value")))
            .otherwise(None)
            .alias(attribute)
        )

    states = object_changes.group_by(keys).agg(updates).with_columns(
        pl.col(attribute)
        .forward_fill()
        .over([s.OCEL_TYPE, s.OCEL_ID], order_by=s.OCEL_TIME)
        .struct.field("value")
        .alias(attribute)
        for attribute in attributes
    )
    states = states.sort(*keys)
    return states.select(s.OCEL_ID, s.OCEL_TIME, *attributes, s.OCEL_TYPE)
