from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

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


def event_object_stats(ocel: OCEL) -> EventObjectStats:
    per_event = (
        ocel.query()
        .event_objects()
        .group_by("ocel_event_id")
        .agg(count().alias("object_count"))
        .collect()
    )

    per_object = (
        ocel.query()
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
