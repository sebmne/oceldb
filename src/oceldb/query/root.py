from __future__ import annotations

from dataclasses import dataclass

from oceldb.core.ocel import OCEL
from oceldb.query.plan import (
    EventObjectSource,
    EventOccurrenceSource,
    EventSource,
    ObjectChangeSource,
    ObjectObjectSource,
    ObjectSource,
    ObjectStateSource,
    QueryPlan,
)
from oceldb.query.types import (
    EventObjectRows,
    EventOccurrenceRows,
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
        """Start a lazy event-row query, optionally filtered to event types."""
        return EventRows(
            QueryPlan.from_source(
                self.ocel,
                source=EventSource(selected_types=tuple(event_types)),
            )
        )

    def objects(self, *object_types: str) -> ObjectRows:
        """Start a lazy logical-object query with one row per object identity."""
        return ObjectRows(
            QueryPlan.from_source(
                self.ocel,
                source=ObjectSource(selected_types=tuple(object_types)),
            )
        )

    def object_states(self, *object_types: str) -> ObjectStateSeed:
        """
        Start a reconstructed object-state query.

        `object_states(...)` does not expose raw history rows. Call `.latest()`
        or `.as_of(...)` to choose the temporal projection and obtain one row
        per logical object with reconstructed state attributes.
        """
        return ObjectStateSeed(
            QueryPlan.from_source(
                self.ocel,
                source=ObjectStateSource(selected_types=tuple(object_types)),
            )
        )

    def object_changes(self, *object_types: str) -> ObjectChangeRows:
        """
        Start a raw object-history query.

        `object_changes(...)` exposes the sparse `object_change` rows exactly as
        stored on disk, including `ocel_changed_field` and nulls for attributes
        that were not updated on a given row. Use `object_states(...)` when you
        need reconstructed state instead of raw updates.
        """
        return ObjectChangeRows(
            QueryPlan.from_source(
                self.ocel,
                source=ObjectChangeSource(selected_types=tuple(object_types)),
            )
        )

    def event_occurrences(self, *object_types: str) -> EventOccurrenceRows:
        """
        Start a sequence-oriented query with one row per event occurrence on an object timeline.

        Each row represents one event-object incidence enriched with the event
        identity, event type, event time, object identity, and object type.
        This root is intended for sequence analysis such as directly-follows
        mining and custom window-based process projections.
        """
        return EventOccurrenceRows(
            QueryPlan.from_source(
                self.ocel,
                source=EventOccurrenceSource(
                    selected_object_types=tuple(object_types),
                ),
            )
        )

    def event_objects(self) -> EventObjectRows:
        """Start a lazy query over event-to-object incidence edges."""
        return EventObjectRows(
            QueryPlan.from_source(self.ocel, source=EventObjectSource())
        )

    def object_objects(self) -> ObjectObjectRows:
        """Start a lazy query over object-to-object relation edges."""
        return ObjectObjectRows(
            QueryPlan.from_source(
                self.ocel,
                source=ObjectObjectSource(),
            )
        )
