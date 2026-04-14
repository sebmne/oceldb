from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, cast

from oceldb.core.ocel import OCEL
from oceldb.dsl import count


@dataclass(frozen=True)
class EventObjectStats:
    avg_objects_per_event: Optional[float]
    min_objects_per_event: Optional[int]
    max_objects_per_event: Optional[int]
    avg_events_per_object: Optional[float]
    min_events_per_object: Optional[int]
    max_events_per_object: Optional[int]


@dataclass(frozen=True)
class ObjectObjectStats:
    edge_count: int
    source_object_count: int
    target_object_count: int
    linked_object_count: int
    avg_outgoing_links_per_source_object: Optional[float]
    min_outgoing_links_per_source_object: Optional[int]
    max_outgoing_links_per_source_object: Optional[int]
    avg_incoming_links_per_target_object: Optional[float]
    min_incoming_links_per_target_object: Optional[int]
    max_incoming_links_per_target_object: Optional[int]


def event_object_stats(ocel: OCEL) -> EventObjectStats:
    per_event = (
        ocel.query
        .event_objects()
        .group_by("ocel_event_id")
        .agg(count().alias("object_count"))
        .collect()
    )

    per_object = (
        ocel.query
        .event_objects()
        .group_by("ocel_object_id")
        .agg(count().alias("event_count"))
        .collect()
    )

    per_event_stats = per_event.aggregate(
        """
        AVG(object_count)::DOUBLE AS avg_objects_per_event,
        MIN(object_count) AS min_objects_per_event,
        MAX(object_count) AS max_objects_per_event
        """
    ).fetchone()

    per_object_stats = per_object.aggregate(
        """
        AVG(event_count)::DOUBLE AS avg_events_per_object,
        MIN(event_count) AS min_events_per_object,
        MAX(event_count) AS max_events_per_object
        """
    ).fetchone()

    if per_event_stats is None or per_object_stats is None:
        raise RuntimeError("event_object_stats aggregation returned no rows")

    return EventObjectStats(
        avg_objects_per_event=per_event_stats[0],
        min_objects_per_event=per_event_stats[1],
        max_objects_per_event=per_event_stats[2],
        avg_events_per_object=per_object_stats[0],
        min_events_per_object=per_object_stats[1],
        max_events_per_object=per_object_stats[2],
    )


def object_object_stats(ocel: OCEL) -> ObjectObjectStats:
    row = ocel.sql("""
        WITH per_source AS (
            SELECT
                "ocel_source_id",
                COUNT(*) AS "outgoing_link_count"
            FROM "object_object"
            GROUP BY "ocel_source_id"
        ),
        per_target AS (
            SELECT
                "ocel_target_id",
                COUNT(*) AS "incoming_link_count"
            FROM "object_object"
            GROUP BY "ocel_target_id"
        ),
        linked_objects AS (
            SELECT "ocel_source_id" AS "ocel_id" FROM "object_object"
            UNION
            SELECT "ocel_target_id" AS "ocel_id" FROM "object_object"
        )
        SELECT
            (SELECT COUNT(*) FROM "object_object") AS "edge_count",
            (SELECT COUNT(*) FROM per_source) AS "source_object_count",
            (SELECT COUNT(*) FROM per_target) AS "target_object_count",
            (SELECT COUNT(*) FROM linked_objects) AS "linked_object_count",
            (SELECT AVG("outgoing_link_count")::DOUBLE FROM per_source) AS "avg_outgoing_links_per_source_object",
            (SELECT MIN("outgoing_link_count") FROM per_source) AS "min_outgoing_links_per_source_object",
            (SELECT MAX("outgoing_link_count") FROM per_source) AS "max_outgoing_links_per_source_object",
            (SELECT AVG("incoming_link_count")::DOUBLE FROM per_target) AS "avg_incoming_links_per_target_object",
            (SELECT MIN("incoming_link_count") FROM per_target) AS "min_incoming_links_per_target_object",
            (SELECT MAX("incoming_link_count") FROM per_target) AS "max_incoming_links_per_target_object"
    """).fetchone()

    if row is None:
        raise RuntimeError("object_object_stats aggregation returned no rows")

    return ObjectObjectStats(
        edge_count=int(row[0]),
        source_object_count=int(row[1]),
        target_object_count=int(row[2]),
        linked_object_count=int(row[3]),
        avg_outgoing_links_per_source_object=_as_optional_float(row[4]),
        min_outgoing_links_per_source_object=_as_optional_int(row[5]),
        max_outgoing_links_per_source_object=_as_optional_int(row[6]),
        avg_incoming_links_per_target_object=_as_optional_float(row[7]),
        min_incoming_links_per_target_object=_as_optional_int(row[8]),
        max_incoming_links_per_target_object=_as_optional_int(row[9]),
    )


def _as_optional_int(value: object) -> Optional[int]:
    if value is None:
        return None
    return int(cast(int | float | str, value))


def _as_optional_float(value: object) -> Optional[float]:
    if value is None:
        return None
    return float(cast(int | float | str, value))
