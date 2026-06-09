"""Object timeline helper."""

import polars as pl

from oceldb.expr import col, row_number
from oceldb.ocel import OCEL


def object_timeline(ocel: OCEL, object_type: str) -> pl.DataFrame:
    """Return a flattened event timeline for one object type.

    The result contains one row per case event, ordered by object id and time,
    zero-based sequence numbers, previous/next event type columns, and the
    selected object's state at the event timestamp.
    """
    flat = ocel.flatten(object_type)
    order = ["time:timestamp", "ocel_event_id"]
    by_case = "case:concept:name"
    state_cols = [
        c
        for c in flat.columns
        if c.startswith("case:")
        and c not in {"case:concept:name", "case:ocel_type", "case:ocel_changed_field"}
    ]
    return (
        flat.mutate(
            seq=row_number().over(group_by=by_case, order_by=order),
            previous_ocel_type=col("concept:name")
            .lag()
            .over(group_by=by_case, order_by=order),
            next_ocel_type=col("concept:name")
            .lead()
            .over(group_by=by_case, order_by=order),
        )
        .select(
            col(by_case).name("ocel_object_id"),
            "seq",
            col("ocel_event_id").name("ocel_id"),
            col("concept:name").name("ocel_type"),
            col("time:timestamp").name("ocel_time"),
            "previous_ocel_type",
            "next_ocel_type",
            *(col(c).name(f"object_{c.removeprefix('case:')}") for c in state_cols),
        )
        .order_by("ocel_object_id", "ocel_time", "ocel_id")
        .execute()
    )
