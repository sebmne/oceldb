from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from oceldb.core.ocel import OCEL


@dataclass(frozen=True)
class EventObjectStats:
    """
    Structural statistics for the event-object relation.
    """

    avg_objects_per_event: Optional[float]
    min_objects_per_event: Optional[int]
    max_objects_per_event: Optional[int]
    avg_events_per_object: Optional[float]
    min_events_per_object: Optional[int]
    max_events_per_object: Optional[int]


def event_object_stats(ocel: OCEL) -> EventObjectStats:
    """
    Return structural statistics for the event-object relation.

    This currently remains SQL-based because the present analytical DSL does not
    yet model relation-rooted queries.
    """
    row = ocel.sql(f"""
        WITH per_event AS (
            SELECT
                ocel_event_id,
                COUNT(*) AS object_count
            FROM {ocel.schema}.event_object
            GROUP BY ocel_event_id
        ),
        per_object AS (
            SELECT
                ocel_object_id,
                COUNT(*) AS event_count
            FROM {ocel.schema}.event_object
            GROUP BY ocel_object_id
        )
        SELECT
            (SELECT AVG(object_count)::DOUBLE FROM per_event) AS avg_objects_per_event,
            (SELECT MIN(object_count) FROM per_event) AS min_objects_per_event,
            (SELECT MAX(object_count) FROM per_event) AS max_objects_per_event,
            (SELECT AVG(event_count)::DOUBLE FROM per_object) AS avg_events_per_object,
            (SELECT MIN(event_count) FROM per_object) AS min_events_per_object,
            (SELECT MAX(event_count) FROM per_object) AS max_events_per_object
    """).fetchone()

    if row is None:
        raise RuntimeError("event_object_stats query returned no rows")

    return EventObjectStats(
        avg_objects_per_event=row[0],
        min_objects_per_event=row[1],
        max_objects_per_event=row[2],
        avg_events_per_object=row[3],
        min_events_per_object=row[4],
        max_events_per_object=row[5],
    )
