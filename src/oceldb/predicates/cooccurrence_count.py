from oceldb.expr import col
from oceldb.ocel import OCEL
from oceldb.predicates.utils import CountPredicate


def cooccurrence_count(ocel: OCEL, object_type: str) -> CountPredicate:
    """Count co-occurring objects of *object_type* via shared events."""
    eo = ocel.event_object
    eo_typed = eo.filter(col("ocel_object_type") == object_type).select(
        ev_id=col("ocel_event_id"),
        co_object_id=col("ocel_object_id"),
    )
    joined = eo.join(eo_typed, eo["ocel_event_id"] == eo_typed["ev_id"])
    counts = (
        joined.group_by("ocel_object_id")
        .aggregate(n=col("co_object_id").nunique())
        .rename(ocel_id="ocel_object_id")
    )
    return CountPredicate(counts)
