"""Activity count helper."""

import polars as pl

from oceldb.expr import col, desc
from oceldb.ocel import OCEL


def activity_counts(ocel: OCEL, *event_types: str) -> pl.DataFrame:
    """Return event counts per activity."""
    return (
        ocel.events(*event_types)
        .group_by("ocel_type")
        .aggregate(n=col("ocel_id").count())
        .order_by(desc("n"), "ocel_type")
        .execute()
    )
