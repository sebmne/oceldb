"""filter_objects_by_o2o_count: keep or remove objects by O2O relation count."""

from typing import Literal

import polars as pl

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations.filters._utils import (
    Mode,
    TypeScope,
    normalize_scope,
    scoped_match,
    within_bounds,
)
from oceldb.operations.pruning import sublog_from_object_ids
from oceldb.operations.step import step

Direction = Literal["in", "out", "both"]


@step
def filter_objects_by_o2o_count(
    ocel: OCEL,
    *,
    min_count: int | None = None,
    max_count: int | None = None,
    related_types: TypeScope = None,
    direction: Direction = "both",
    object_types: TypeScope = None,
    mode: Mode = "include",
) -> OCEL:
    """Keep or remove objects by their incoming, outgoing, or total O2O count.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        min_count: Inclusive lower bound on the O2O relation count. Omit for
            no lower bound.
        max_count: Inclusive upper bound on the O2O relation count. Omit for
            no upper bound.
        related_types: Only relations to/from these object types count.
            ``None`` counts every object type.
        direction: ``"out"`` counts relations where the object is the
            source, ``"in"`` where it is the target, ``"both"`` sums both.
        object_types: Limits which object types the count applies to;
            objects of other types are left untouched. ``None`` applies it
            to every type.
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
    related_scope = normalize_scope(related_types)
    o2o = ocel.o2o()

    out_relations = o2o
    in_relations = o2o
    if related_scope is not None:
        out_relations = out_relations.filter(
            pl.col(s.OCEL_TARGET_TYPE).is_in(related_scope)
        )
        in_relations = in_relations.filter(
            pl.col(s.OCEL_SOURCE_TYPE).is_in(related_scope)
        )
    out_counts = (
        out_relations.group_by(s.OCEL_SOURCE_ID)
        .agg(pl.len().alias("_count"))
        .rename({s.OCEL_SOURCE_ID: s.OCEL_ID})
    )
    in_counts = (
        in_relations.group_by(s.OCEL_TARGET_ID)
        .agg(pl.len().alias("_count"))
        .rename({s.OCEL_TARGET_ID: s.OCEL_ID})
    )

    if direction == "both":
        combined = (
            pl.concat([out_counts, in_counts])
            .group_by(s.OCEL_ID)
            .agg(pl.col("_count").sum())
        )
    elif direction == "out":
        combined = out_counts
    else:
        combined = in_counts

    object_counts = (
        ocel.objects()
        .select(s.OCEL_ID, s.OCEL_TYPE)
        .join(combined, on=s.OCEL_ID, how="left")
        .with_columns(pl.col("_count").fill_null(0))
    )
    keep = scoped_match(
        in_bounds,
        type_col=s.OCEL_TYPE,
        scope=normalize_scope(object_types),
        mode=mode,
    )
    return sublog_from_object_ids(
        ocel,
        object_counts.filter(keep).select(pl.col(s.OCEL_ID).alias(s.OCEL_OBJECT_ID)),
    )
