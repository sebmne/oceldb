from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from oceldb.core.ocel import OCEL
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
    event_count = ocel.query.events().count()
    object_count = ocel.query.objects().count()

    time_window = ocel.query.events().select("ocel_time").collect()
    row = time_window.aggregate(
        "MIN(ocel_time) AS earliest, MAX(ocel_time) AS latest"
    ).fetchone()

    return OCELOverview(
        event_count=event_count,
        object_count=object_count,
        event_type_count=len(event_types(ocel)),
        object_type_count=len(object_types(ocel)),
        earliest_event_time=None if row is None else row[0],
        latest_event_time=None if row is None else row[1],
    )
