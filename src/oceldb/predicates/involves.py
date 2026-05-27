from oceldb.expr import Predicate, col
from oceldb.ocel import OCEL


def involves(ocel: OCEL, object_type: str) -> Predicate:
    """Match events involving an object of *object_type*."""
    event_ids = ocel.event_object.filter(col("ocel_object_type") == object_type)[
        "ocel_event_id"
    ]
    return col("ocel_id").isin(event_ids)
