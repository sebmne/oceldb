from __future__ import annotations

from oceldb.core.ocel import OCEL
from oceldb.inspect.attributes import attributes
from oceldb.inspect.overview import OCELOverview, overview
from oceldb.inspect.relations import event_object_stats
from oceldb.inspect.types import (
    event_type_counts,
    event_types,
    object_type_counts,
    object_types,
    types,
)


class OCELInspector:
    def __init__(self, ocel: OCEL) -> None:
        self._ocel = ocel

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

    def attributes(self):
        return attributes(self._ocel)

    def overview(self) -> OCELOverview:
        return overview(self._ocel)

    def event_object_stats(self):
        return event_object_stats(self._ocel)
