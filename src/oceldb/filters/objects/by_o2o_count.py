"""filter_objects_by_o2o_count: keep objects by number of O2O relations."""

from collections.abc import Iterable
from typing import Literal

import polars as pl

from oceldb import schema as s
from oceldb.utils import to_list
from oceldb.utils.step import step
from oceldb.core.pruning import sublog_from_object_ids
from oceldb.filters._utils import normalize_scope, scoped_match, within_bounds
from oceldb.ocel import OCEL


@step
def filter_objects_by_o2o_count(
    ocel: OCEL,
    *,
    min_count: int | None = None,
    max_count: int | None = None,
    related_types: str | Iterable[str] | None = None,
    direction: Literal["in", "out", "both"] = "both",
    object_types: str | Iterable[str] | None = None,
    mode: Literal["include", "exclude"] = "include",
) -> OCEL:
    """Keep objects whose O2O relation count satisfies the given bounds.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        min_count: Inclusive lower bound on the number of O2O relations.
        max_count: Inclusive upper bound on the number of O2O relations.
        related_types: Restrict the count to relations whose other endpoint
            is of these types. ``None`` counts all O2O relations.
        direction: Which relations to count — ``"out"`` (object is source),
            ``"in"`` (object is target), or ``"both"`` (default).
        object_types: Object type(s) the filter applies to. ``None`` (default)
            applies it to all objects; objects of other types pass through
            unchanged.
        mode: ``"include"`` (default) keeps objects whose count is within bounds;
            ``"exclude"`` keeps objects whose count is outside them.

    Examples:
        >>> from oceldb.filters import filter_objects_by_o2o_count
        >>> sub = filter_objects_by_o2o_count(ocel, min_count=1)
        >>> sub = ocel >> filter_objects_by_o2o_count(min_count=2, related_types="item", direction="out")
    """
    o2o = ocel.object_object()

    out_counts = pl.LazyFrame(
        {
            s.OCEL_ID: pl.Series([], dtype=pl.String),
            "_count": pl.Series([], dtype=pl.UInt32),
        }
    )
    in_counts = pl.LazyFrame(
        {
            s.OCEL_ID: pl.Series([], dtype=pl.String),
            "_count": pl.Series([], dtype=pl.UInt32),
        }
    )

    if direction in ("out", "both"):
        src = o2o
        if related_types is not None:
            src = src.filter(pl.col(s.OCEL_TARGET_TYPE).is_in(to_list(related_types)))
        out_counts = (
            src.group_by(s.OCEL_SOURCE_ID)
            .agg(pl.len().alias("_count"))
            .rename({s.OCEL_SOURCE_ID: s.OCEL_ID})
        )

    if direction in ("in", "both"):
        tgt = o2o
        if related_types is not None:
            tgt = tgt.filter(pl.col(s.OCEL_SOURCE_TYPE).is_in(to_list(related_types)))
        in_counts = (
            tgt.group_by(s.OCEL_TARGET_ID)
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
    in_bounds = within_bounds(min_count=min_count, max_count=max_count)
    scope = normalize_scope(object_types)
    keep = scoped_match(in_bounds, type_col=s.OCEL_TYPE, scope=scope, mode=mode)

    kept_objects = object_counts.filter(keep).select(
        pl.col(s.OCEL_ID).alias(s.OCEL_OBJECT_ID)
    )
    return sublog_from_object_ids(ocel, kept_objects)
