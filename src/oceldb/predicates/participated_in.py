from oceldb.expr import Predicate, col
from oceldb.ocel import OCEL


def participated_in(ocel: OCEL, event_type: str) -> Predicate:
    """Match objects participating in an event of *event_type*."""
    object_ids = ocel.event_object.filter(col("ocel_event_type") == event_type)[
        "ocel_object_id"
    ]
    return col("ocel_id").isin(object_ids)
