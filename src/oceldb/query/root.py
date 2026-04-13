from __future__ import annotations

from dataclasses import dataclass

from oceldb.core.ocel import OCEL
from oceldb.query.plan import (
    EventObjectSource,
    EventSource,
    ObjectChangeSource,
    ObjectObjectSource,
    ObjectSource,
    ObjectStateSource,
    QueryPlan,
)
from oceldb.query.types import (
    EventObjectRows,
    EventRows,
    ObjectChangeRows,
    ObjectObjectRows,
    ObjectRows,
    ObjectStateSeed,
)


@dataclass(frozen=True)
class OCELQueryRoot:
    ocel: OCEL

    def events(self, *event_types: str) -> EventRows:
        return EventRows(
            QueryPlan.from_source(
                self.ocel,
                source=EventSource(selected_types=tuple(event_types)),
            )
        )

    def objects(self, *object_types: str) -> ObjectRows:
        return ObjectRows(
            QueryPlan.from_source(
                self.ocel,
                source=ObjectSource(selected_types=tuple(object_types)),
            )
        )

    def object_states(self, *object_types: str) -> ObjectStateSeed:
        return ObjectStateSeed(
            QueryPlan.from_source(
                self.ocel,
                source=ObjectStateSource(selected_types=tuple(object_types)),
            )
        )

    def object_changes(self, *object_types: str) -> ObjectChangeRows:
        return ObjectChangeRows(
            QueryPlan.from_source(
                self.ocel,
                source=ObjectChangeSource(selected_types=tuple(object_types)),
            )
        )

    def event_objects(self) -> EventObjectRows:
        return EventObjectRows(
            QueryPlan.from_source(self.ocel, source=EventObjectSource())
        )

    def object_objects(self) -> ObjectObjectRows:
        return ObjectObjectRows(
            QueryPlan.from_source(
                self.ocel,
                source=ObjectObjectSource(),
            )
        )
