"""Event filters exposed through :mod:`oceldb.filters`."""

import polars as pl

from oceldb import schema as s
from oceldb.core.pruning import sublog_from_event_ids, sublog_from_relations
from oceldb.filters._utils import (
    Mode,
    TimeBound,
    TypeScope,
    _sample_ids,
    match_decision,
    normalize_scope,
    normalize_time_bound,
    scoped_match,
    within_bounds,
)
from oceldb.ocel import OCEL
from oceldb.core.step import step


@step
def filter_events_by_attribute(
    ocel: OCEL,
    predicate: pl.Expr,
    *,
    event_types: TypeScope = None,
    mode: Mode = "include",
) -> OCEL:
    """Keep events matching a Polars predicate and prune the connected core.

    ``event_types`` limits which event types the predicate affects. Predicate
    nulls are treated as no match, so exclude mode retains them.
    """
    keep = scoped_match(
        predicate,
        type_col=s.OCEL_TYPE,
        scope=normalize_scope(event_types),
        mode=mode,
    )
    events = ocel.events().filter(keep)
    event_ids = events.select(pl.col(s.OCEL_ID).alias(s.OCEL_EVENT_ID))
    relations = ocel.event_object().join(event_ids, on=s.OCEL_EVENT_ID, how="semi")
    return sublog_from_relations(ocel, relations, events=events)


@step
def filter_events_by_id(
    ocel: OCEL,
    *ids: str,
    event_types: TypeScope = None,
    mode: Mode = "include",
) -> OCEL:
    """Keep or remove events whose ``ocel_id`` is in ``ids``."""
    return filter_events_by_attribute(
        ocel,
        pl.col(s.OCEL_ID).is_in(list(ids)),
        event_types=event_types,
        mode=mode,
    )


@step
def filter_events_by_type(
    ocel: OCEL,
    *types: str,
    mode: Mode = "include",
) -> OCEL:
    """Keep or remove events whose ``ocel_type`` is in ``types``."""
    keep = match_decision(pl.col(s.OCEL_EVENT_TYPE).is_in(list(types)), mode)
    events = ocel.events().filter(
        match_decision(pl.col(s.OCEL_TYPE).is_in(list(types)), mode)
    )
    relations = ocel.event_object().filter(keep)
    return sublog_from_relations(ocel, relations, events=events)


@step
def filter_events_by_time(
    ocel: OCEL,
    *,
    start: TimeBound | None = None,
    end: TimeBound | None = None,
    event_types: TypeScope = None,
    mode: Mode = "include",
) -> OCEL:
    """Keep events within an inclusive UTC-normalized time window.

    Naive dates and datetimes are interpreted as UTC. Offset-aware values are
    converted to UTC before comparison with canonical ``ocel_time`` values.
    """
    start_value = None if start is None else normalize_time_bound(start, name="start")
    end_value = None if end is None else normalize_time_bound(end, name="end")
    if start_value is None and end_value is None:
        raise ValueError("Pass at least one of start or end.")
    if start_value is not None and end_value is not None and start_value > end_value:
        raise ValueError("start must not be later than end.")

    predicate = pl.lit(True)
    timestamp_type = pl.Datetime("us", "UTC")
    if start_value is not None:
        predicate &= pl.col(s.OCEL_TIME) >= pl.lit(start_value, dtype=timestamp_type)
    if end_value is not None:
        predicate &= pl.col(s.OCEL_TIME) <= pl.lit(end_value, dtype=timestamp_type)
    return filter_events_by_attribute(
        ocel,
        predicate,
        event_types=event_types,
        mode=mode,
    )


@step
def filter_events_by_object_count(
    ocel: OCEL,
    *,
    min_count: int | None = None,
    max_count: int | None = None,
    object_types: TypeScope = None,
    event_types: TypeScope = None,
    mode: Mode = "include",
) -> OCEL:
    """Keep events by their number of distinct related objects."""
    in_bounds = within_bounds(min_count=min_count, max_count=max_count)
    e2o = ocel.event_object()
    if object_types is not None:
        object_scope = normalize_scope(object_types)
        assert object_scope is not None
        e2o = e2o.filter(pl.col(s.OCEL_OBJECT_TYPE).is_in(object_scope))

    counts = e2o.group_by(s.OCEL_EVENT_ID).agg(
        pl.col(s.OCEL_OBJECT_ID).n_unique().alias("_count")
    )
    event_counts = (
        ocel.events()
        .select(pl.col(s.OCEL_ID).alias(s.OCEL_EVENT_ID), s.OCEL_TYPE)
        .join(counts, on=s.OCEL_EVENT_ID, how="left")
        .with_columns(pl.col("_count").fill_null(0))
    )
    keep = scoped_match(
        in_bounds,
        type_col=s.OCEL_TYPE,
        scope=normalize_scope(event_types),
        mode=mode,
    )
    return sublog_from_event_ids(
        ocel,
        event_counts.filter(keep).select(s.OCEL_EVENT_ID),
    )


@step
def sample_events(
    ocel: OCEL,
    n: int | None = None,
    *,
    fraction: float | None = None,
    seed: int | None = None,
) -> OCEL:
    """Keep a random event sample and prune the connected core."""
    ids = ocel.events().select(s.OCEL_ID).collect().get_column(s.OCEL_ID).drop_nulls()
    keep = _sample_ids(ids, n=n, fraction=fraction, seed=seed)
    return filter_events_by_id(ocel, *keep)
