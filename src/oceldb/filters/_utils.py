"""Shared validation and expression helpers for filters."""

from collections.abc import Iterable
from datetime import date, datetime, timezone
from math import isfinite
from typing import Literal, TypeAlias, cast

import polars as pl

from oceldb import schema as s
from oceldb.core.pruning import sublog_from_object_ids
from oceldb.ocel import OCEL

Mode = Literal["include", "exclude"]
TypeScope: TypeAlias = str | Iterable[str] | None
TimeBound: TypeAlias = str | date | datetime


def normalize_scope(scope: object) -> list[str] | None:
    """Normalize an optional scalar-or-iterable type scope."""
    if scope is None:
        return None
    if isinstance(scope, str):
        values = [scope]
    elif isinstance(scope, Iterable):
        values = list(scope)
    else:
        raise TypeError("A type scope must be a string, iterable of strings, or None.")
    if not all(isinstance(value, str) for value in values):
        raise TypeError("Type scopes must contain only strings.")
    return values


def validate_mode(mode: str) -> Mode:
    """Validate an include/exclude mode at the public API boundary."""
    if mode not in {"include", "exclude"}:
        raise ValueError("mode must be 'include' or 'exclude'.")
    return cast(Mode, mode)


def within_bounds(
    *, min_count: int | None, max_count: int | None, column: str = "_count"
) -> pl.Expr:
    """Build a validated inclusive count-range predicate."""
    _validate_count("min_count", min_count)
    _validate_count("max_count", max_count)
    if min_count is None and max_count is None:
        raise ValueError("Pass at least one of min_count or max_count.")
    if min_count is not None and max_count is not None and min_count > max_count:
        raise ValueError("min_count must not exceed max_count.")

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
    """Build a non-null keep expression for a scoped predicate filter."""
    decision = match_decision(match, mode)
    if scope is None:
        return decision
    in_scope = pl.col(type_col).is_in(scope).fill_null(False)
    return (~in_scope) | decision


def match_decision(match: pl.Expr, mode: Mode) -> pl.Expr:
    """Interpret a null predicate result as no match.

    Nulls are dropped in include mode and retained in exclude mode.
    """
    validate_mode(mode)
    resolved = match.fill_null(False)
    return resolved if mode == "include" else ~resolved


def normalize_time_bound(value: object, *, name: str) -> datetime:
    """Return an ISO, date, or datetime scalar normalized to UTC."""
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, date):
        result = datetime.combine(value, datetime.min.time())
    elif isinstance(value, str):
        try:
            result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{name} must be a valid ISO 8601 timestamp.") from exc
    else:
        raise TypeError(f"{name} must be a string, date, or datetime.")

    if result.tzinfo is None:
        return result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def _sample_ids(
    ids: pl.Series, *, n: object, fraction: object, seed: object
) -> list[str]:
    """Sample identifier values by count or fraction, without replacement."""
    if (n is None) == (fraction is None):
        raise ValueError("Pass exactly one of n or fraction.")
    if seed is not None:
        if isinstance(seed, bool) or not isinstance(seed, int):
            raise TypeError("seed must be an integer or None.")
        if seed < 0:
            raise ValueError("seed must be non-negative.")
    total = ids.len()
    if fraction is not None:
        if isinstance(fraction, bool) or not isinstance(fraction, (int, float)):
            raise TypeError("fraction must be a number.")
        if not isfinite(fraction):
            raise ValueError("fraction must be finite.")
        if not 0.0 <= fraction <= 1.0:
            raise ValueError(f"fraction must be in [0, 1], got {fraction}.")
        count = round(fraction * total)
    else:
        if isinstance(n, bool) or not isinstance(n, int):
            raise TypeError("n must be an integer.")
        count = n
        if count < 0:
            raise ValueError(f"n must be non-negative, got {count}.")
    count = min(count, total)
    sampled = ids if count >= total else ids.sample(n=count, seed=seed)
    return [str(value) for value in sampled.to_list()]


def _filter_objects_direct(ocel: OCEL, predicate: pl.Expr) -> OCEL:
    """Filter object identities and prune around that authoritative selection."""
    objects = ocel.objects().filter(predicate)
    satisfying = objects.select(pl.col(s.OCEL_ID).alias(s.OCEL_OBJECT_ID))
    return sublog_from_object_ids(ocel, satisfying, objects=objects)


def _validate_count(name: str, value: object) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer or None.")
    if value < 0:
        raise ValueError(f"{name} must be non-negative.")
