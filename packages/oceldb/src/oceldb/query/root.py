from __future__ import annotations

from dataclasses import dataclass

from oceldb.core.metadata import LogicalTableName
from oceldb.core.ocel import OCEL
from oceldb.query.lazy_query import LazyOCELQuery


@dataclass(frozen=True)
class OCELQueryRoot:
    ocel: OCEL

    def events(self, *event_types: str) -> LazyOCELQuery:
        return LazyOCELQuery.from_source(
            self.ocel,
            source_kind="event",
            selected_types=tuple(event_types),
        )

    def objects(self, *object_types: str) -> LazyOCELQuery:
        return LazyOCELQuery.from_source(
            self.ocel,
            source_kind="object",
            selected_types=tuple(object_types),
        )

    def event_objects(self) -> LazyOCELQuery:
        return self.relations("event_object")

    def object_objects(self) -> LazyOCELQuery:
        return self.relations("object_object")

    def relations(self, name: LogicalTableName) -> LazyOCELQuery:
        if name not in {"event_object", "object_object"}:
            raise ValueError(
                "relations(...) only accepts 'event_object' or 'object_object'"
            )
        return LazyOCELQuery.from_source(self.ocel, source_kind=name)
