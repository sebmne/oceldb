"""Shared utilities for filter implementations."""

from collections.abc import Iterable

import polars as pl

from oceldb import schema as s
from oceldb.ocel import OCEL


def _to_list(value: str | Iterable[str]) -> list[str]:
    return [value] if isinstance(value, str) else list(value)


def _filter_objects_direct(ocel: OCEL, predicate: pl.Expr) -> OCEL:
    """Filter the objects table by *predicate*, then prune the connected core."""
    satisfying = (
        ocel.objects()
        .filter(predicate)
        .select(pl.col(s.OCEL_ID).alias(s.OCEL_OBJECT_ID))
    )
    relations = ocel.event_object().join(satisfying, on=s.OCEL_OBJECT_ID, how="semi")
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
