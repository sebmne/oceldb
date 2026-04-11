from dataclasses import dataclass

from oceldb.core.ocel import OCEL
from oceldb.tables.query.table_query import TableQuery


@dataclass(frozen=True)
class OCELTables:
    ocel: OCEL

    def events(self) -> TableQuery:
        return TableQuery.from_source(self.ocel, table_kind="event")

    def objects(self) -> TableQuery:
        return TableQuery.from_source(self.ocel, table_kind="object")

    def event_objects(self) -> TableQuery:
        return TableQuery.from_source(self.ocel, table_kind="event_object")

    def object_objects(self) -> TableQuery:
        return TableQuery.from_source(self.ocel, table_kind="object_object")
