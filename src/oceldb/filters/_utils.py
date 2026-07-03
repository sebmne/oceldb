"""Shared utilities for filter implementations."""

from typing import Literal, cast

import polars as pl

from oceldb import schema as s
from oceldb.ocel import OCEL
from oceldb.pruning import prune_log

Mode = Literal["include", "exclude"]


def _scoped_match(
    match: pl.Expr,
    *,
    type_col: str,
    scope: list[str] | None,
    mode: Mode,
) -> pl.Expr:
    """Build the boolean *keep* expression shared by every predicate filter.

    Within *scope* (all types when *scope* is ``None``) a row is kept when
    *match* holds under ``"include"`` or fails under ``"exclude"``. Rows whose
    ``type_col`` value is outside *scope* are always kept, so a filter only ever
    touches the types it is scoped to.
    """
    decision = match if mode == "include" else ~match
    if scope is None:
        return decision
    return (~pl.col(type_col).is_in(scope)) | decision


def _sample_ids(
    ids: pl.Series, *, n: int | None, fraction: float | None, seed: int | None
) -> list[str]:
    """Sample identifier values by count or fraction, without replacement.

    Raises:
        ValueError: If neither or both of *n* and *fraction* are given, if
            *fraction* is outside ``[0, 1]``, or if *n* is negative.
    """
    if (n is None) == (fraction is None):
        raise ValueError("Pass exactly one of n or fraction.")
    total = ids.len()
    if fraction is not None:
        if not 0.0 <= fraction <= 1.0:
            raise ValueError(f"fraction must be in [0, 1], got {fraction}.")
        count = round(fraction * total)
    else:
        count = cast(int, n)
        if count < 0:
            raise ValueError(f"n must be non-negative, got {count}.")
    count = min(count, total)
    sampled = ids if count >= total else ids.sample(n=count, seed=seed)
    return [str(value) for value in sampled.to_list()]


def _filter_objects_direct(ocel: OCEL, predicate: pl.Expr) -> OCEL:
    """Filter the objects table by *predicate*, then prune the connected core."""
    satisfying = (
        ocel.objects()
        .filter(predicate)
        .select(pl.col(s.OCEL_ID).alias(s.OCEL_OBJECT_ID))
    )
    relations = ocel.event_object().join(satisfying, on=s.OCEL_OBJECT_ID, how="semi")
    kept_events = relations.select(s.OCEL_EVENT_ID).unique()
    kept_objects = relations.select(s.OCEL_OBJECT_ID).unique()
    return prune_log(
        ocel,
        events=ocel.events().join(
            kept_events, left_on=s.OCEL_ID, right_on=s.OCEL_EVENT_ID, how="semi"
        ),
        objects=ocel.objects().join(
            kept_objects, left_on=s.OCEL_ID, right_on=s.OCEL_OBJECT_ID, how="semi"
        ),
        e2o=relations,
    )
