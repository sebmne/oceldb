"""Object type count helper."""

import polars as pl

from oceldb.expr import col, desc
from oceldb.ocel import OCEL


def object_type_counts(ocel: OCEL) -> pl.DataFrame:
    """Return object counts per object type."""
    return (
        ocel.objects()
        .group_by("ocel_type")
        .aggregate(n=col("ocel_id").count())
        .order_by(desc("n"), "ocel_type")
        .execute()
    )
