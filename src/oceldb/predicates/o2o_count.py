from typing import Literal

from oceldb.expr import col
from oceldb.ocel import OCEL
from oceldb.predicates.utils import CountPredicate


def o2o_count(
    ocel: OCEL,
    target_type: str,
    *,
    direction: Literal["forward", "backward"] = "forward",
) -> CountPredicate:
    """Count O2O-linked objects of *target_type*."""
    oo = ocel.object_object
    if direction == "forward":
        counts = (
            oo.filter(oo["ocel_target_type"] == target_type)
            .group_by("ocel_source_id")
            .aggregate(n=col("ocel_target_id").count())
            .rename(ocel_id="ocel_source_id")
        )
    else:
        counts = (
            oo.filter(oo["ocel_source_type"] == target_type)
            .group_by("ocel_target_id")
            .aggregate(n=col("ocel_source_id").count())
            .rename(ocel_id="ocel_target_id")
        )
    return CountPredicate(counts)
