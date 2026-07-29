"""Filter objects by their number of distinct O2O neighbors."""

from typing import Literal

import polars as pl

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations.filters._utils import (
    Mode,
    scoped_match,
    within_bounds,
)
from oceldb.operations.pruning import sublog_from_object_ids
from oceldb.operations.step import step
from oceldb.types import OneOrMany, normalize_strings

Direction = Literal["in", "out", "both"]


@step
def filter_objects_by_o2o_count(
    ocel: OCEL,
    *,
    object_types: OneOrMany[str] | None = None,
    related_types: OneOrMany[str] | None = None,
    direction: Direction = "both",
    min_count: int | None = None,
    max_count: int | None = None,
    mode: Mode = "include",
) -> OCEL:
    """Keep or remove objects by their number of distinct O2O neighbors.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        object_types: Limits which object types the count applies to;
            objects of other types are left untouched. ``None`` applies it
            to every type.
        related_types: Only relations to/from these object types count.
            ``None`` counts every object type.
        direction: ``"out"`` counts relations where the object is the
            source, ``"in"`` where it is the target, and ``"both"`` counts
            neighbors in either direction. Parallel relations and self-loops
            count each related object once.
        min_count: Inclusive lower bound on distinct related objects. Omit
            for no lower bound.
        max_count: Inclusive upper bound on distinct related objects. Omit
            for no upper bound.
        mode: ``"include"`` keeps objects within bounds; ``"exclude"``
            removes them.

    Returns:
        A new ``OCEL`` pruned to the surviving objects and their connected
        core.

    Raises:
        ValueError: If *direction* is invalid, neither *min_count* nor
            *max_count* is given, or *min_count* exceeds *max_count*.

    Examples:
        >>> from oceldb.operations.filters import filter_objects_by_o2o_count
        >>> sub = filter_objects_by_o2o_count(ocel, min_count=1, direction="out")
        >>> sub = ocel >> filter_objects_by_o2o_count(max_count=0, related_types="item")
    """
    if direction not in {"in", "out", "both"}:
        raise ValueError("direction must be 'in', 'out', or 'both'.")
    in_bounds = within_bounds(min_count=min_count, max_count=max_count)
    related_scope = normalize_strings(
        related_types,
        name="related_types",
        non_empty=True,
    )
    o2o = ocel.o2o()

    outgoing = o2o.select(
        pl.col(s.OCEL_SOURCE_ID).alias(s.OCEL_ID),
        pl.col(s.OCEL_TARGET_ID).alias("_related_id"),
        pl.col(s.OCEL_TARGET_TYPE).alias("_related_type"),
    )
    incoming = o2o.select(
        pl.col(s.OCEL_TARGET_ID).alias(s.OCEL_ID),
        pl.col(s.OCEL_SOURCE_ID).alias("_related_id"),
        pl.col(s.OCEL_SOURCE_TYPE).alias("_related_type"),
    )
    if direction == "both":
        adjacency = pl.concat([outgoing, incoming])
    elif direction == "out":
        adjacency = outgoing
    else:
        adjacency = incoming
    if related_scope is not None:
        adjacency = adjacency.filter(pl.col("_related_type").is_in(related_scope))
    counts = adjacency.group_by(s.OCEL_ID).agg(
        pl.col("_related_id").n_unique().alias("_count")
    )

    object_counts = (
        ocel.objects()
        .select(s.OCEL_ID, s.OCEL_TYPE)
        .join(counts, on=s.OCEL_ID, how="left")
        .with_columns(pl.col("_count").fill_null(0))
    )
    keep = scoped_match(
        in_bounds,
        type_col=s.OCEL_TYPE,
        scope=normalize_strings(object_types, name="object_types", non_empty=True),
        mode=mode,
    )
    return sublog_from_object_ids(
        ocel,
        object_counts.filter(keep).select(pl.col(s.OCEL_ID).alias(s.OCEL_OBJECT_ID)),
    )
