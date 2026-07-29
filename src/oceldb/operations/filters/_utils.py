"""Shared validation and expression helpers for public filters."""

from datetime import date, datetime, timezone
from typing import Literal, cast

import polars as pl

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations.pruning import prune_log, sublog_from_relations

Mode = Literal["include", "exclude"]


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
    return scoped_match_many(match, scopes=((type_col, scope),), mode=mode)


def scoped_match_many(
    match: pl.Expr,
    *,
    scopes: tuple[tuple[str, list[str] | None], ...],
    mode: Mode,
) -> pl.Expr:
    """Apply a decision only where every supplied type scope matches."""
    decision = match_decision(match, mode)
    in_scope = pl.lit(True)
    scoped = False
    for column, values in scopes:
        if values is None:
            continue
        scoped = True
        in_scope &= pl.col(column).is_in(values).fill_null(False)
    return ((~in_scope) | decision) if scoped else decision


def match_decision(match: pl.Expr, mode: Mode) -> pl.Expr:
    """Interpret a null predicate result as no match.

    Nulls are dropped in include mode and retained in exclude mode.
    """
    validate_mode(mode)
    resolved = match.fill_null(False)
    return resolved if mode == "include" else ~resolved


def validate_predicate(predicate: object) -> pl.Expr:
    """Validate a Polars predicate at the public API boundary."""
    if not isinstance(predicate, pl.Expr):
        raise TypeError("predicate must be a Polars expression.")
    return predicate


def distinct_counts(
    relations: pl.LazyFrame,
    *,
    group: str,
    value: str,
    sorted_pairs: bool,
) -> pl.LazyFrame:
    """Count exact distinct endpoint pairs with a sorted streaming fast path."""
    pairs = relations.select(group, value)
    if not sorted_pairs:
        return pairs.group_by(group).agg(pl.col(value).n_unique().alias("_count"))
    new_pair = (
        (pl.col(group) != pl.col(group).shift(1))
        | (pl.col(value) != pl.col(value).shift(1))
    ).fill_null(True)
    return (
        pairs.filter(new_pair)
        .set_sorted(group)
        .group_by(group, maintain_order=True)
        .len(name="_count")
    )


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


def _filter_events_direct(
    ocel: OCEL,
    *,
    event_predicate: pl.Expr,
    relation_predicate: pl.Expr,
) -> OCEL:
    """Filter events and E2O through their corresponding denormalized fields."""
    events = ocel.events().filter(event_predicate)
    relations = ocel.e2o().filter(relation_predicate)
    return sublog_from_relations(ocel, relations, events=events)


def _filter_objects_direct(
    ocel: OCEL,
    *,
    object_predicate: pl.Expr,
    relation_predicate: pl.Expr,
) -> OCEL:
    """Filter objects and E2O through their corresponding denormalized fields."""
    objects = ocel.objects().filter(object_predicate)
    relations = ocel.e2o().filter(relation_predicate)
    kept_events = relations.select(s.OCEL_EVENT_ID).unique()
    events = ocel.events().join(
        kept_events,
        left_on=s.OCEL_ID,
        right_on=s.OCEL_EVENT_ID,
        how="semi",
    )
    return prune_log(ocel, events=events, objects=objects, e2o=relations)


def _validate_count(name: str, value: object) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer or None.")
    if value < 0:
        raise ValueError(f"{name} must be non-negative.")
