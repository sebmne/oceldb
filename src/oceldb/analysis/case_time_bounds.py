"""Per-case time bound helper."""

import polars as pl

from oceldb.expr import col
from oceldb.ocel import OCEL


def case_time_bounds(ocel: OCEL, object_type: str) -> pl.DataFrame:
    """Return first/last event timestamps and event counts per object case."""
    flat = ocel.flatten(object_type)
    return (
        flat.group_by("case:concept:name")
        .aggregate(
            event_count=col("ocel_event_id").count(),
            start_time=col("time:timestamp").min(),
            end_time=col("time:timestamp").max(),
        )
        .order_by("case:concept:name")
        .execute()
    )
