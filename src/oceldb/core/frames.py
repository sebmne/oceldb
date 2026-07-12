"""Lazy-frame selection and combination helpers used by :class:`OCEL`."""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

import polars as pl

from oceldb import schema as s


def select_types(
    frame: pl.LazyFrame, types: Sequence[str], core: tuple[str, ...]
) -> pl.LazyFrame:
    """Filter by OCEL type and omit attributes that are entirely null."""
    sub = frame.filter(pl.col(s.OCEL_TYPE).is_in(list(types)))
    kept = present_columns(sub, attribute_columns(frame, core))
    return sub.select(*core, *kept, s.OCEL_TYPE)


def attribute_columns(frame: pl.LazyFrame, core: tuple[str, ...]) -> list[str]:
    """Return non-core attribute column names from a logical OCEL frame."""
    return [
        name
        for name in frame.collect_schema().names()
        if name not in core and name != s.OCEL_TYPE
    ]


def present_columns(frame: pl.LazyFrame, candidates: list[str]) -> list[str]:
    """Return candidate columns containing at least one non-null value."""
    if not candidates:
        return []
    row = frame.select(
        pl.col(name).is_not_null().any().alias(name) for name in candidates
    ).collect()
    return [name for name in candidates if cast(bool, row.get_column(name).item())]


def concat_unique(
    frames: list[pl.LazyFrame], *, subset: list[str] | None
) -> pl.LazyFrame:
    """Union schema-aligned frames and remove duplicate rows or identifiers."""
    combined = pl.concat(frames, how="diagonal_relaxed")
    if subset is None:
        return combined.unique(maintain_order=True)
    return combined.unique(subset=subset, keep="first", maintain_order=True)


def reconstruct_object_states(
    object_changes: pl.LazyFrame,
    events: pl.LazyFrame,
    e2o: pl.LazyFrame,
    types: tuple[str, ...],
) -> pl.LazyFrame:
    """Forward-fill sparse changes and attach their deterministic causing event."""
    states, attrs = _forward_filled_states(object_changes, types)
    enriched = states.join(
        _causing_events(events, e2o, types),
        left_on=[s.OCEL_ID, s.OCEL_TIME],
        right_on=[s.OCEL_OBJECT_ID, s.OCEL_TIME],
        how="left",
    )
    return enriched.select(
        s.OCEL_ID,
        s.OCEL_TIME,
        *attrs,
        s.OCEL_EVENT_ID,
        s.OCEL_EVENT_TYPE,
        s.OCEL_TYPE,
    ).sort(s.OCEL_TYPE, s.OCEL_ID, s.OCEL_TIME)


def _forward_filled_states(
    object_changes: pl.LazyFrame, types: tuple[str, ...]
) -> tuple[pl.LazyFrame, list[str]]:
    frame = object_changes
    core = (s.OCEL_ID, s.OCEL_TIME, s.OCEL_CHANGED_FIELD)
    candidates = attribute_columns(frame, core)
    if types:
        frame = frame.filter(pl.col(s.OCEL_TYPE).is_in(list(types)))
        attrs = present_columns(frame, candidates)
    else:
        attrs = candidates
    keys = [s.OCEL_TYPE, s.OCEL_ID, s.OCEL_TIME]
    if not attrs:
        return frame.select(keys).unique(), attrs
    collapsed = frame.group_by(keys).agg(
        pl.col(attr).drop_nulls().last().alias(attr) for attr in attrs
    )
    states = collapsed.with_columns(
        pl.col(attr).forward_fill().over([s.OCEL_TYPE, s.OCEL_ID], order_by=s.OCEL_TIME)
        for attr in attrs
    )
    return states, attrs


def _causing_events(
    events: pl.LazyFrame, e2o: pl.LazyFrame, types: tuple[str, ...]
) -> pl.LazyFrame:
    relations = e2o
    if types:
        relations = relations.filter(pl.col(s.OCEL_OBJECT_TYPE).is_in(list(types)))
    return (
        relations.select(s.OCEL_OBJECT_ID, s.OCEL_EVENT_ID, s.OCEL_EVENT_TYPE)
        .join(
            events.select(s.OCEL_ID, s.OCEL_TIME),
            left_on=s.OCEL_EVENT_ID,
            right_on=s.OCEL_ID,
            how="inner",
        )
        .sort(s.OCEL_EVENT_ID)
        .unique(
            subset=[s.OCEL_OBJECT_ID, s.OCEL_TIME],
            keep="first",
            maintain_order=True,
        )
    )
