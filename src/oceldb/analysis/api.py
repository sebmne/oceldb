from __future__ import annotations

from dataclasses import dataclass

from oceldb.analysis.query.table_query import TableQuery
from oceldb.core.ocel import OCEL


def analyze(ocel: OCEL) -> AnalysisContext:
    return AnalysisContext(ocel)


@dataclass(frozen=True)
class AnalysisContext:
    ocel: OCEL

    def events(self) -> TableQuery:
        return TableQuery.from_source(self.ocel, table_kind="event")

    def objects(self) -> TableQuery:
        return TableQuery.from_source(self.ocel, table_kind="object")

    def event_objects(self) -> TableQuery:
        return TableQuery.from_source(self.ocel, table_kind="event_object")

    def object_objects(self) -> TableQuery:
        return TableQuery.from_source(self.ocel, table_kind="object_object")
