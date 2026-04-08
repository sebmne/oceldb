from __future__ import annotations

from oceldb.core.ocel import OCEL
from oceldb.inspect.attributes import attributes, event_attributes, object_attributes
from oceldb.inspect.overview import OCELOverview, overview
from oceldb.inspect.relations import EventObjectStats, event_object_stats
from oceldb.inspect.types import (
    event_type_counts,
    event_types,
    object_type_counts,
    object_types,
    types,
)


class OCELInspector:
    """
    Inspection and descriptive summary interface for an OCEL.

    This object exposes:
        - type discovery
        - attribute discovery
        - overview statistics
        - per-type counts
        - structural relation statistics
    """

    def __init__(self, ocel: OCEL) -> None:
        self._ocel = ocel

    # -------------------------------------------------------------------------
    # Type inspection
    # -------------------------------------------------------------------------

    def event_types(self):
        return event_types(self._ocel)

    def object_types(self):
        return object_types(self._ocel)

    def types(self):
        return types(self._ocel)

    def event_type_counts(self):
        return event_type_counts(self._ocel)

    def object_type_counts(self):
        return object_type_counts(self._ocel)

    # -------------------------------------------------------------------------
    # Attribute inspection
    # -------------------------------------------------------------------------

    def event_attributes(self, event_type_name: str):
        return event_attributes(self._ocel, event_type_name)

    def object_attributes(self, object_type_name: str):
        return object_attributes(self._ocel, object_type_name)

    def attributes(self):
        return attributes(self._ocel)

    # -------------------------------------------------------------------------
    # Overview
    # -------------------------------------------------------------------------

    def overview(self) -> OCELOverview:
        return overview(self._ocel)

    # -------------------------------------------------------------------------
    # Relation summaries
    # -------------------------------------------------------------------------

    def event_object_stats(self) -> EventObjectStats:
        return event_object_stats(self._ocel)
