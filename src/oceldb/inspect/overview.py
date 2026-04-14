from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from oceldb.core.ocel import OCEL
from oceldb.inspect.profile import table_counts, time_range
from oceldb.inspect.types import event_types, object_types


@dataclass(frozen=True)
class OCELOverview:
    event_count: int
    object_count: int
    event_type_count: int
    object_type_count: int
    earliest_event_time: Optional[datetime]
    latest_event_time: Optional[datetime]


def overview(ocel: OCEL) -> OCELOverview:
    counts = table_counts(ocel)
    event_window = time_range(ocel)

    return OCELOverview(
        event_count=counts.event_count,
        object_count=counts.object_count,
        event_type_count=len(event_types(ocel)),
        object_type_count=len(object_types(ocel)),
        earliest_event_time=event_window.earliest_event_time,
        latest_event_time=event_window.latest_event_time,
    )
