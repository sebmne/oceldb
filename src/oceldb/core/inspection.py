"""Materialized summaries and notebook representations for OCEL logs."""

from __future__ import annotations

import html
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import cast

import polars as pl

from oceldb import schema as s

_TYPE_PREVIEW_LIMIT = 6


@dataclass(frozen=True)
class OCELSummary:
    """Materialized row counts, type counts, and event time bounds."""

    events: int
    objects: int
    object_changes: int
    e2o: int
    o2o: int
    event_types: dict[str, int]
    object_types: dict[str, int]
    start_time: datetime | None
    end_time: datetime | None


@dataclass(frozen=True)
class _Overview:
    events: int
    objects: int
    object_changes: int
    e2o: int
    o2o: int
    event_types: list[str]
    object_types: list[str]


def describe_frames(
    events: pl.LazyFrame,
    objects: pl.LazyFrame,
    object_changes: pl.LazyFrame,
    e2o: pl.LazyFrame,
    o2o: pl.LazyFrame,
) -> OCELSummary:
    """Compute the programmatic summary for five logical OCEL frames."""
    return OCELSummary(
        events=_count(events),
        objects=_count(objects),
        object_changes=_count(object_changes),
        e2o=_count(e2o),
        o2o=_count(o2o),
        event_types=_type_counts(events),
        object_types=_type_counts(objects),
        start_time=_time_bound(events, descending=False),
        end_time=_time_bound(events, descending=True),
    )


def text_repr(
    events: pl.LazyFrame,
    objects: pl.LazyFrame,
    object_changes: pl.LazyFrame,
    e2o: pl.LazyFrame,
    o2o: pl.LazyFrame,
) -> str:
    """Render the compact plain-text OCEL overview."""
    overview = _overview(events, objects, object_changes, e2o, o2o)
    return (
        "OCEL\n"
        f"  events:         {_rows(overview.events)} | "
        f"{_types(overview.event_types)}\n"
        f"  objects:        {_rows(overview.objects)} | "
        f"{_types(overview.object_types)}\n"
        f"  object changes: {_rows(overview.object_changes)}\n"
        f"  relations:      E2O: {_rows(overview.e2o)} | "
        f"O2O: {_rows(overview.o2o)}"
    )


def html_repr(
    events: pl.LazyFrame,
    objects: pl.LazyFrame,
    object_changes: pl.LazyFrame,
    e2o: pl.LazyFrame,
    o2o: pl.LazyFrame,
) -> str:
    """Render the notebook HTML OCEL overview."""
    overview = _overview(events, objects, object_changes, e2o, o2o)
    counts = "".join(
        f"<tr><th style='text-align:left'>{label}</th>"
        f"<td style='text-align:right'>{value:,}</td></tr>"
        for label, value in (
            ("Events", overview.events),
            ("Objects", overview.objects),
            ("Object changes", overview.object_changes),
            ("E2O relations", overview.e2o),
            ("O2O relations", overview.o2o),
        )
    )
    return (
        "<div style='font-family:sans-serif'>"
        "<strong>OCEL</strong>"
        f"<table>{counts}</table>"
        f"<div><em>Event types:</em> {_chips(overview.event_types)}</div>"
        f"<div><em>Object types:</em> {_chips(overview.object_types)}</div>"
        "</div>"
    )


def _overview(
    events: pl.LazyFrame,
    objects: pl.LazyFrame,
    object_changes: pl.LazyFrame,
    e2o: pl.LazyFrame,
    o2o: pl.LazyFrame,
) -> _Overview:
    return _Overview(
        events=_count(events),
        objects=_count(objects),
        object_changes=_count(object_changes),
        e2o=_count(e2o),
        o2o=_count(o2o),
        event_types=_distinct_types(events),
        object_types=_distinct_types(objects),
    )


def _count(frame: pl.LazyFrame) -> int:
    return int(cast(int, frame.select(pl.len()).collect().item()))


def _distinct_types(frame: pl.LazyFrame) -> list[str]:
    values: Iterable[object] = (
        frame.select(s.OCEL_TYPE)
        .unique()
        .collect()
        .get_column(s.OCEL_TYPE)
        .drop_nulls()
        .sort()
        .to_list()
    )
    return [str(value) for value in values]


def _type_counts(frame: pl.LazyFrame) -> dict[str, int]:
    data = (
        frame.select(s.OCEL_TYPE)
        .drop_nulls()
        .group_by(s.OCEL_TYPE)
        .len()
        .sort(s.OCEL_TYPE)
        .collect()
    )
    return {
        str(name): int(cast(int, count))
        for name, count in zip(
            data.get_column(s.OCEL_TYPE).to_list(), data.get_column("len").to_list()
        )
    }


def _time_bound(frame: pl.LazyFrame, *, descending: bool) -> datetime | None:
    column = pl.col(s.OCEL_TIME)
    aggregation = column.max() if descending else column.min()
    value = frame.select(aggregation.alias("bound")).collect().item()
    return cast("datetime | None", value)


def _rows(count: int) -> str:
    return f"{count:,} row" if count == 1 else f"{count:,} rows"


def _types(values: list[str]) -> str:
    count = len(values)
    label = "type" if count == 1 else "types"
    return f"{count} {label} {_preview(values)}"


def _preview(values: Sequence[str]) -> str:
    if not values:
        return "[]"
    visible = [repr(value) for value in values[:_TYPE_PREVIEW_LIMIT]]
    remaining = len(values) - len(visible)
    if remaining:
        visible.append(f"... +{remaining} more")
    return "[" + ", ".join(visible) + "]"


def _chips(values: list[str]) -> str:
    if not values:
        return "<em>none</em>"
    return ", ".join(html.escape(value) for value in values)
