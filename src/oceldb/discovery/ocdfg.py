from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from statistics import mean, median
from typing import cast

from oceldb.core.ocel import OCEL
from oceldb.dsl import col, count, desc, sum_, when
from oceldb.inspect.types import object_types as all_object_types
from oceldb.query.types import EventOccurrenceRows


@dataclass(frozen=True)
class OCDFGNode:
    activity: str
    count: int
    start_count: int
    end_count: int


@dataclass(frozen=True)
class OCDFGEdge:
    source: str
    target: str
    count: int
    mean_duration_seconds: float
    median_duration_seconds: float
    min_duration_seconds: float
    max_duration_seconds: float


@dataclass(frozen=True)
class OCDFG:
    object_types: tuple[str, ...]
    nodes: tuple[OCDFGNode, ...]
    edges: tuple[OCDFGEdge, ...]

    def node(self, activity: str) -> OCDFGNode:
        for node in self.nodes:
            if node.activity == activity:
                return node
        raise KeyError(f"Unknown OC-DFG activity: {activity!r}")

    def edge(self, source: str, target: str) -> OCDFGEdge:
        for edge in self.edges:
            if edge.source == source and edge.target == target:
                return edge
        raise KeyError(f"Unknown OC-DFG edge: {source!r} -> {target!r}")


CountLike = int | float | str
NodeRow = tuple[str, CountLike, CountLike, CountLike]
EdgeRow = tuple[str, str, datetime, datetime]


def projected_dfg(ocel: OCEL, *object_types: str) -> OCDFG:
    selected_types = (
        _normalize_object_types(object_types)
        if object_types
        else tuple(all_object_types(ocel))
    )

    if not selected_types:
        return OCDFG(object_types=(), nodes=(), edges=())

    timeline = _timeline_query(ocel, selected_types)
    nodes = _load_nodes(timeline)
    edges = _load_edges(timeline)
    return OCDFG(
        object_types=selected_types,
        nodes=nodes,
        edges=edges,
    )


def ocdfg(ocel: OCEL, *object_types: str) -> OCDFG:
    return projected_dfg(ocel, *object_types)


def _normalize_object_types(values: tuple[str, ...]) -> tuple[str, ...]:
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return tuple(normalized)


def _timeline_query(ocel: OCEL, selected_types: tuple[str, ...]) -> EventOccurrenceRows:
    order_by = ("ocel_event_time", "ocel_event_id")
    return (
        ocel.query
        .event_occurrences(*selected_types)
        .with_columns(
            previous_event_type=col("ocel_event_type").lag().over(
                partition_by="ocel_object_id",
                order_by=order_by,
            ),
            next_event_type=col("ocel_event_type").lead().over(
                partition_by="ocel_object_id",
                order_by=order_by,
            ),
            next_event_time=col("ocel_event_time").lead().over(
                partition_by="ocel_object_id",
                order_by=order_by,
            ),
        )
    )


def _load_nodes(timeline: EventOccurrenceRows) -> tuple[OCDFGNode, ...]:
    rows = cast(
        list[NodeRow],
        timeline
        .group_by("ocel_event_type")
        .agg(
            count().alias("count"),
            sum_(
                when(col("previous_event_type").is_null())
                .then(1)
                .otherwise(0)
            ).alias("start_count"),
            sum_(
                when(col("next_event_type").is_null())
                .then(1)
                .otherwise(0)
            ).alias("end_count"),
        )
        .sort(desc("count"), "ocel_event_type")
        .collect()
        .fetchall(),
    )

    return tuple(
        OCDFGNode(
            activity=row[0],
            count=int(row[1]),
            start_count=int(row[2]),
            end_count=int(row[3]),
        )
        for row in rows
    )


def _load_edges(timeline: EventOccurrenceRows) -> tuple[OCDFGEdge, ...]:
    rows = cast(
        list[EdgeRow],
        timeline
        .where(col("next_event_type").not_null())
        .select(
            source=col("ocel_event_type"),
            target=col("next_event_type"),
            event_time=col("ocel_event_time"),
            next_event_time=col("next_event_time"),
        )
        .collect()
        .fetchall(),
    )

    durations_by_edge: dict[tuple[str, str], list[float]] = defaultdict(list)
    for source, target, event_time, next_event_time in rows:
        duration_seconds = (next_event_time - event_time).total_seconds()
        durations_by_edge[(source, target)].append(float(duration_seconds))

    edges = [
        OCDFGEdge(
            source=source,
            target=target,
            count=len(durations),
            mean_duration_seconds=float(mean(durations)),
            median_duration_seconds=float(median(durations)),
            min_duration_seconds=min(durations),
            max_duration_seconds=max(durations),
        )
        for (source, target), durations in durations_by_edge.items()
    ]
    edges.sort(key=lambda edge: (-edge.count, edge.source, edge.target))
    return tuple(edges)
