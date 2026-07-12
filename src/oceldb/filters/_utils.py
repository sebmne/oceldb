"""Shared utilities for filter implementations."""

from collections.abc import Iterable
from typing import Literal, TypeAlias, cast

import polars as pl

from oceldb import schema as s
from oceldb.core.pruning import sublog_from_object_ids
from oceldb.ocel import OCEL
from oceldb.utils import to_list

Mode = Literal["include", "exclude"]
TypeScope: TypeAlias = str | Iterable[str] | None


def normalize_scope(scope: TypeScope) -> list[str] | None:
    """Normalize an optional scalar-or-iterable type scope."""
    return None if scope is None else to_list(scope)


def within_bounds(
    *, min_count: int | None, max_count: int | None, column: str = "_count"
) -> pl.Expr:
    """Build an inclusive count-range predicate."""
    predicate = pl.lit(True)
    if min_count is not None:
        predicate &= pl.col(column) >= min_count
    if max_count is not None:
        predicate &= pl.col(column) <= max_count
    return predicate


def scoped_match(
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
    return sublog_from_object_ids(ocel, satisfying)
