"""Materialized summaries and notebook representations for OCEL logs."""

import html
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
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
    event_types: Mapping[str, int]
    object_types: Mapping[str, int]
    start_time: datetime | None
    end_time: datetime | None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "event_types", MappingProxyType(dict(self.event_types))
        )
        object.__setattr__(
            self,
            "object_types",
            MappingProxyType(dict(self.object_types)),
        )


def describe_frames(
    events: pl.LazyFrame,
    objects: pl.LazyFrame,
    object_changes: pl.LazyFrame,
    e2o: pl.LazyFrame,
    o2o: pl.LazyFrame,
) -> OCELSummary:
    """Compute the programmatic summary for five logical OCEL frames."""
    time_dtype = events.collect_schema()[s.OCEL_TIME]
    data = pl.concat(
        [
            _typed_statistics(
                events,
                "events",
                time_dtype=time_dtype,
                include_time=True,
            ),
            _typed_statistics(
                objects,
                "objects",
                time_dtype=time_dtype,
                include_time=False,
            ),
            _row_count(object_changes, "object_changes", time_dtype=time_dtype),
            _row_count(e2o, "e2o", time_dtype=time_dtype),
            _row_count(o2o, "o2o", time_dtype=time_dtype),
        ],
        how="vertical",
    ).collect()
    event_data = data.filter(pl.col("_table") == "events")
    object_data = data.filter(pl.col("_table") == "objects")
    return OCELSummary(
        events=_sum_counts(event_data),
        objects=_sum_counts(object_data),
        object_changes=_named_count(data, "object_changes"),
        e2o=_named_count(data, "e2o"),
        o2o=_named_count(data, "o2o"),
        event_types=_counts_by_type(event_data),
        object_types=_counts_by_type(object_data),
        start_time=cast("datetime | None", event_data.get_column("_start").min()),
        end_time=cast("datetime | None", event_data.get_column("_end").max()),
    )


def text_repr(summary: OCELSummary) -> str:
    """Render the compact plain-text OCEL overview."""
    return (
        "OCEL\n"
        f"  events:         {_rows(summary.events)} | "
        f"{_types(list(summary.event_types))}\n"
        f"  objects:        {_rows(summary.objects)} | "
        f"{_types(list(summary.object_types))}\n"
        f"  object changes: {_rows(summary.object_changes)}\n"
        f"  relations:      E2O: {_rows(summary.e2o)} | "
        f"O2O: {_rows(summary.o2o)}"
    )


def html_repr(summary: OCELSummary) -> str:
    """Render the notebook HTML OCEL overview."""
    counts = "".join(
        f"<tr><th style='text-align:left'>{label}</th>"
        f"<td style='text-align:right'>{value:,}</td></tr>"
        for label, value in (
            ("Events", summary.events),
            ("Objects", summary.objects),
            ("Object changes", summary.object_changes),
            ("E2O relations", summary.e2o),
            ("O2O relations", summary.o2o),
        )
    )
    return (
        "<div style='font-family:sans-serif'>"
        "<strong>OCEL</strong>"
        f"<table>{counts}</table>"
        f"<div><em>Event types:</em> {_chips(list(summary.event_types))}</div>"
        f"<div><em>Object types:</em> {_chips(list(summary.object_types))}</div>"
        "</div>"
    )


def _typed_statistics(
    frame: pl.LazyFrame,
    table: str,
    *,
    time_dtype: pl.DataType,
    include_time: bool,
) -> pl.LazyFrame:
    start = (
        pl.col(s.OCEL_TIME).min() if include_time else pl.lit(None, dtype=time_dtype)
    )
    end = pl.col(s.OCEL_TIME).max() if include_time else pl.lit(None, dtype=time_dtype)
    return (
        frame.group_by(s.OCEL_TYPE)
        .agg(
            pl.len().alias("_count"),
            start.alias("_start"),
            end.alias("_end"),
        )
        .select(
            pl.lit(table).alias("_table"),
            s.OCEL_TYPE,
            "_count",
            "_start",
            "_end",
        )
    )


def _row_count(
    frame: pl.LazyFrame,
    table: str,
    *,
    time_dtype: pl.DataType,
) -> pl.LazyFrame:
    return frame.select(
        pl.lit(table).alias("_table"),
        pl.lit(None, dtype=pl.String).alias(s.OCEL_TYPE),
        pl.len().alias("_count"),
        pl.lit(None, dtype=time_dtype).alias("_start"),
        pl.lit(None, dtype=time_dtype).alias("_end"),
    )


def _sum_counts(data: pl.DataFrame) -> int:
    return int(cast(int, data.get_column("_count").sum() or 0))


def _named_count(data: pl.DataFrame, table: str) -> int:
    value = data.filter(pl.col("_table") == table).get_column("_count").item()
    return int(cast(int, value))


def _counts_by_type(data: pl.DataFrame) -> dict[str, int]:
    counts = {
        str(name): int(cast(int, count))
        for name, count in data.select(s.OCEL_TYPE, "_count").iter_rows()
        if name is not None
    }
    return dict(sorted(counts.items()))


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
