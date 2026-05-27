from typing import Literal

from oceldb.expr import Predicate, col
from oceldb.ocel import OCEL
from oceldb.predicates.utils import o2o_reachable_bfs, o2o_reachable_recursive


def o2o_reachable(
    ocel: OCEL,
    object_type: str,
    *,
    direction: Literal["forward", "backward", "both"] = "both",
    max_hops: int | None = 1,
) -> Predicate:
    """Match objects connected to *object_type* through O2O relations."""
    if max_hops is not None and max_hops < 1:
        raise ValueError(f"max_hops must be >= 1 or None, got {max_hops!r}")

    if max_hops is None:
        linked_ids = o2o_reachable_recursive(ocel, object_type, direction)
    else:
        linked_ids = o2o_reachable_bfs(
            ocel.object_object, object_type, direction, max_hops
        )

    return col("ocel_id").isin(linked_ids["ocel_id"])
