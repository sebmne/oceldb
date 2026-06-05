from typing import Literal

from oceldb.expr import col
from oceldb.ocel import OCEL
from oceldb.predicates.utils import CountPredicate


def e2o_count(
    ocel: OCEL,
    target_type: str,
    *,
    target: Literal["event", "object"] = "object",
) -> CountPredicate:
    """Count E2O-linked entities of *target_type* from the opposite side.

    ``target="object"`` counts objects per event; ``target="event"`` counts
    events per object.
    """
    eo = ocel.event_object
    if target == "object":
        counts = (
            eo.filter(eo["ocel_object_type"] == target_type)
            .group_by("ocel_event_id")
            .aggregate(n=col("ocel_object_id").count())
            .rename(ocel_id="ocel_event_id")
        )
    else:
        counts = (
            eo.filter(eo["ocel_event_type"] == target_type)
            .group_by("ocel_object_id")
            .aggregate(n=col("ocel_event_id").count())
            .rename(ocel_id="ocel_object_id")
        )
    return CountPredicate(counts)
