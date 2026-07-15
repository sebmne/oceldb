"""Object filters exposed through :mod:`oceldb.filters`."""

from typing import Literal

import polars as pl

from oceldb import schema as s
from oceldb.core.pruning import prune_log, sublog_from_object_ids
from oceldb.filters._utils import (
    Mode,
    TimeBound,
    TypeScope,
    _filter_objects_direct,
    _sample_ids,
    match_decision,
    normalize_scope,
    normalize_time_bound,
    scoped_match,
    validate_mode,
    within_bounds,
)
from oceldb.ocel import OCEL
from oceldb.core.step import step

Direction = Literal["in", "out", "both"]


@step
def filter_objects_by_attribute(
    ocel: OCEL,
    predicate: pl.Expr,
    *,
    object_types: TypeScope = None,
    when: Literal["sometimes", "always"] | TimeBound = "sometimes",
    mode: Mode = "include",
) -> OCEL:
    """Keep objects whose state history satisfies a predicate.

    ``when`` may be ``"sometimes"``, ``"always"``, or an ISO/date/datetime
    upper bound selecting the last state at or before that instant. Predicate
    nulls are treated as no match. Objects without states match ``"always"``
    vacuously, but not ``"sometimes"`` or a point-in-time query.
    """
    validate_mode(mode)
    scope = normalize_scope(object_types)
    states = ocel.object_states() if scope is None else ocel.object_states(*scope)
    scoped_objects = (
        ocel.objects()
        if scope is None
        else ocel.objects().filter(pl.col(s.OCEL_TYPE).is_in(scope))
    )
    resolved = predicate.fill_null(False)

    if when == "sometimes":
        matching = states.filter(resolved).select(s.OCEL_ID).unique()
    elif when == "always":
        failing = states.filter(~resolved).select(s.OCEL_ID).unique()
        matching = scoped_objects.select(s.OCEL_ID).join(
            failing, on=s.OCEL_ID, how="anti"
        )
    else:
        bound = normalize_time_bound(when, name="when")
        matching = (
            states.filter(
                pl.col(s.OCEL_TIME) <= pl.lit(bound, dtype=pl.Datetime("us", "UTC"))
            )
            .sort(s.OCEL_TYPE, s.OCEL_ID, s.OCEL_TIME)
            .unique(subset=[s.OCEL_ID], keep="last", maintain_order=True)
            .filter(resolved)
            .select(s.OCEL_ID)
        )

    scoped_ids = scoped_objects.select(s.OCEL_ID)
    kept_scoped = (
        matching
        if mode == "include"
        else scoped_ids.join(matching, on=s.OCEL_ID, how="anti")
    )
    if scope is None:
        kept = kept_scoped
    else:
        outside_scope = (
            ocel.objects()
            .filter(~pl.col(s.OCEL_TYPE).is_in(scope).fill_null(False))
            .select(s.OCEL_ID)
        )
        kept = pl.concat([outside_scope, kept_scoped]).unique()

    return sublog_from_object_ids(
        ocel,
        kept.select(pl.col(s.OCEL_ID).alias(s.OCEL_OBJECT_ID)),
    )


@step
def filter_objects_by_id(
    ocel: OCEL,
    *ids: str,
    object_types: TypeScope = None,
    mode: Mode = "include",
) -> OCEL:
    """Keep or remove objects whose ``ocel_id`` is in ``ids``."""
    keep = scoped_match(
        pl.col(s.OCEL_ID).is_in(list(ids)),
        type_col=s.OCEL_TYPE,
        scope=normalize_scope(object_types),
        mode=mode,
    )
    return _filter_objects_direct(ocel, keep)


@step
def filter_objects_by_type(
    ocel: OCEL,
    *types: str,
    mode: Mode = "include",
) -> OCEL:
    """Keep or remove objects whose ``ocel_type`` is in ``types``."""
    keep = match_decision(pl.col(s.OCEL_OBJECT_TYPE).is_in(list(types)), mode)
    objects = ocel.objects().filter(
        match_decision(pl.col(s.OCEL_TYPE).is_in(list(types)), mode)
    )
    relations = ocel.event_object().filter(keep)
    kept_events = relations.select(s.OCEL_EVENT_ID).unique()
    events = ocel.events().join(
        kept_events,
        left_on=s.OCEL_ID,
        right_on=s.OCEL_EVENT_ID,
        how="semi",
    )
    return prune_log(ocel, events=events, objects=objects, e2o=relations)


@step
def filter_objects_by_event_count(
    ocel: OCEL,
    *,
    min_count: int | None = None,
    max_count: int | None = None,
    event_types: TypeScope = None,
    object_types: TypeScope = None,
    mode: Mode = "include",
) -> OCEL:
    """Keep objects by their number of distinct related events."""
    in_bounds = within_bounds(min_count=min_count, max_count=max_count)
    e2o = ocel.event_object()
    if event_types is not None:
        event_scope = normalize_scope(event_types)
        assert event_scope is not None
        e2o = e2o.filter(pl.col(s.OCEL_EVENT_TYPE).is_in(event_scope))

    counts = e2o.group_by(s.OCEL_OBJECT_ID).agg(
        pl.col(s.OCEL_EVENT_ID).n_unique().alias("_count")
    )
    object_counts = (
        ocel.objects()
        .select(pl.col(s.OCEL_ID).alias(s.OCEL_OBJECT_ID), s.OCEL_TYPE)
        .join(counts, on=s.OCEL_OBJECT_ID, how="left")
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
        object_counts.filter(keep).select(s.OCEL_OBJECT_ID),
    )


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
    """Keep objects by incoming, outgoing, or total O2O relation count."""
    if direction not in {"in", "out", "both"}:
        raise ValueError("direction must be 'in', 'out', or 'both'.")
    in_bounds = within_bounds(min_count=min_count, max_count=max_count)
    related_scope = normalize_scope(related_types)
    o2o = ocel.object_object()

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


@step
def sample_objects(
    ocel: OCEL,
    n: int | None = None,
    *,
    fraction: float | None = None,
    seed: int | None = None,
) -> OCEL:
    """Keep a random object sample and prune around that selection."""
    ids = ocel.objects().select(s.OCEL_ID).collect().get_column(s.OCEL_ID).drop_nulls()
    keep = _sample_ids(ids, n=n, fraction=fraction, seed=seed)
    return filter_objects_by_id(ocel, *keep)
