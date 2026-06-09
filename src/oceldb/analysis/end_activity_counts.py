"""End activity count helper."""

import polars as pl

from oceldb.expr import col, desc, row_number
from oceldb.ocel import OCEL


def end_activity_counts(ocel: OCEL, object_type: str) -> pl.DataFrame:
    """Return counts of last activities for the selected object type."""
    flat = ocel.flatten(object_type)
    rn = row_number().over(
        group_by="case:concept:name",
        order_by=[desc("time:timestamp"), desc("ocel_event_id")],
    )
    return (
        flat.mutate(_oceldb_rn=rn)
        .filter(col("_oceldb_rn") == 0)
        .group_by("concept:name")
        .aggregate(n=col("case:concept:name").count())
        .order_by(desc("n"), "concept:name")
        .execute()
    )
