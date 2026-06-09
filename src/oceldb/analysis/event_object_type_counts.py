"""Event-object relation count helper."""

import polars as pl

from oceldb.expr import col, desc
from oceldb.ocel import OCEL


def event_object_type_counts(ocel: OCEL) -> pl.DataFrame:
    """Return E2O relation counts by event and object type."""
    return (
        ocel.event_object.group_by("ocel_event_type", "ocel_object_type")
        .aggregate(n=col("ocel_event_id").count())
        .order_by(desc("n"), "ocel_event_type", "ocel_object_type")
        .execute()
    )
