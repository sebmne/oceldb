from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from oceldb.analysis.api import analyze
from oceldb.core.ocel import OCEL
from oceldb.dsl import id_, max_, min_
from oceldb.dsl.fields import time_
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
    event_count = analyze(ocel).events().count_distinct(id_())
    object_count = analyze(ocel).objects().count_distinct(id_())

    event_type_count = len(event_types(ocel))
    object_type_count = len(object_types(ocel))

    earliest_event_time = analyze(ocel).events().agg(min_(time_())).scalar()
    latest_event_time = analyze(ocel).events().agg(max_(time_())).scalar()

    return OCELOverview(
        event_count=event_count,
        object_count=object_count,
        event_type_count=event_type_count,
        object_type_count=object_type_count,
        earliest_event_time=earliest_event_time,
        latest_event_time=latest_event_time,
    )
