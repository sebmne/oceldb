from __future__ import annotations

from dataclasses import dataclass

from oceldb.expressions.base import Expr
from oceldb.expressions.context import Context
from oceldb.query.base import BaseQuery


@dataclass(frozen=True)
class EventQuery(BaseQuery):
    event_types: tuple[str, ...] = ()

    def _context(self) -> Context:
        return Context.for_events(schema=self.ocel.schema, alias="e")

    def _with_filters(self, filters: tuple[Expr, ...]) -> "EventQuery":
        return EventQuery(
            ocel=self.ocel,
            event_types=self.event_types,
            filters=filters,
        )

    def _root_alias(self) -> str:
        return "e"

    def _root_table_name(self) -> str:
        return "event"

    def _id_column(self) -> str:
        return "ocel_id"

    def _type_filter_sql(self) -> str | None:
        if not self.event_types:
            return None

        escaped_types = ", ".join(
            f"'{event_type.replace("'", "''")}'" for event_type in self.event_types
        )
        return f"e.ocel_type IN ({escaped_types})"

    def __repr__(self) -> str:
        return (
            f"EventQuery(event_types={self.event_types!r}, filters={len(self.filters)})"
        )
