"""filter_objects_by_o2o_count: keep objects by number of O2O relations."""

from collections.abc import Callable, Iterable
from typing import Literal, overload

import polars as pl

from oceldb import schema as s
from oceldb.filters._step import _step
from oceldb.filters._utils import _to_list
from oceldb.ocel import OCEL


@overload
def filter_objects_by_o2o_count(
    ocel: OCEL,
    *,
    min_count: int | None = ...,
    max_count: int | None = ...,
    related_types: str | Iterable[str] | None = ...,
    direction: Literal["in", "out", "both"] = ...,
) -> OCEL: ...


@overload
def filter_objects_by_o2o_count(
    *,
    min_count: int | None = ...,
    max_count: int | None = ...,
    related_types: str | Iterable[str] | None = ...,
    direction: Literal["in", "out", "both"] = ...,
) -> Callable[[OCEL], OCEL]: ...


@_step
def filter_objects_by_o2o_count(
    ocel: OCEL,
    *,
    min_count: int | None = None,
    max_count: int | None = None,
    related_types: str | Iterable[str] | None = None,
    direction: Literal["in", "out", "both"] = "both",
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

    Examples:
        >>> from oceldb.filters import filter_objects_by_o2o_count
        >>> sub = filter_objects_by_o2o_count(ocel, min_count=1)
        >>> sub = ocel >> filter_objects_by_o2o_count(min_count=2, related_types="item", direction="out")
    """
    o2o = ocel.object_object()

    out_counts = pl.LazyFrame({s.OCEL_ID: pl.Series([], dtype=pl.String), "_count": pl.Series([], dtype=pl.UInt32)})
    in_counts = pl.LazyFrame({s.OCEL_ID: pl.Series([], dtype=pl.String), "_count": pl.Series([], dtype=pl.UInt32)})

    if direction in ("out", "both"):
        src = o2o
        if related_types is not None:
            src = src.filter(pl.col(s.OCEL_TARGET_TYPE).is_in(_to_list(related_types)))
        out_counts = src.group_by(s.OCEL_SOURCE_ID).agg(pl.len().alias("_count")).rename({s.OCEL_SOURCE_ID: s.OCEL_ID})

    if direction in ("in", "both"):
        tgt = o2o
        if related_types is not None:
            tgt = tgt.filter(pl.col(s.OCEL_SOURCE_TYPE).is_in(_to_list(related_types)))
        in_counts = tgt.group_by(s.OCEL_TARGET_ID).agg(pl.len().alias("_count")).rename({s.OCEL_TARGET_ID: s.OCEL_ID})

    if direction == "both":
        combined = pl.concat([out_counts, in_counts]).group_by(s.OCEL_ID).agg(pl.col("_count").sum())
    elif direction == "out":
        combined = out_counts
    else:
        combined = in_counts

    object_counts = (
        ocel.objects()
        .select(s.OCEL_ID)
        .join(combined, on=s.OCEL_ID, how="left")
        .with_columns(pl.col("_count").fill_null(0))
    )
    if min_count is not None:
        object_counts = object_counts.filter(pl.col("_count") >= min_count)
    if max_count is not None:
        object_counts = object_counts.filter(pl.col("_count") <= max_count)

    kept_objects = object_counts.select(pl.col(s.OCEL_ID).alias(s.OCEL_OBJECT_ID))
    relations = ocel.event_object().join(kept_objects, on=s.OCEL_OBJECT_ID, how="semi")
    kept_events = relations.select(s.OCEL_EVENT_ID).unique()
    return OCEL(
        events=ocel.events().join(
            kept_events, left_on=s.OCEL_ID, right_on=s.OCEL_EVENT_ID, how="semi"
        ),
        objects=ocel.objects().join(
            kept_objects, left_on=s.OCEL_ID, right_on=s.OCEL_OBJECT_ID, how="semi"
        ),
        object_changes=ocel.object_changes().join(
            kept_objects, left_on=s.OCEL_ID, right_on=s.OCEL_OBJECT_ID, how="semi"
        ),
        o2o=ocel.object_object()
        .join(kept_objects, left_on=s.OCEL_SOURCE_ID, right_on=s.OCEL_OBJECT_ID, how="semi")
        .join(kept_objects, left_on=s.OCEL_TARGET_ID, right_on=s.OCEL_OBJECT_ID, how="semi"),
        e2o=relations,
    )
