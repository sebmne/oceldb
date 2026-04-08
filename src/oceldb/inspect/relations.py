from dataclasses import dataclass
from typing import Optional

from oceldb.core.ocel import OCEL
from oceldb.dsl import count, event_id, object_id


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

    The OCEL-aware first-stage aggregations are expressed through the `tables`
    API. The resulting DuckDB relations are then aggregated further using the
    DuckDB relation API, which is the intended boundary for general-purpose
    second-stage analysis.
    """
    per_event = (
        ocel.tables.event_objects()
        .select(event_id().as_("event_id"))
        .group_by(event_id())
        .agg(count().as_("object_count"))
        .relation()
    )

    per_object = (
        ocel.tables.event_objects()
        .select(object_id().as_("object_id"))
        .group_by(object_id())
        .agg(count().as_("event_count"))
        .relation()
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
