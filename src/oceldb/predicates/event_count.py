from oceldb.expr import col
from oceldb.ocel import OCEL
from oceldb.predicates.utils import CountPredicate


def event_count(ocel: OCEL, event_type: str) -> CountPredicate:
    """Count *event_type* events for each participating object."""
    counts = (
        ocel.event_object.filter(col("ocel_event_type") == event_type)
        .group_by("ocel_object_id")
        .aggregate(n=col("ocel_event_id").count())
        .rename(ocel_id="ocel_object_id")
    )
    return CountPredicate(counts)
