from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from oceldb.expressions.base import Expr
from oceldb.expressions.context import ScopeKind
from oceldb.query.base import BaseQuery


@dataclass(frozen=True)
class EventQuery(BaseQuery):
    event_types: tuple[str, ...] = ()

    def _clone_with_filters(self, filters: tuple[Expr, ...]) -> "EventQuery":
        return EventQuery(
            ocel=self.ocel,
            event_types=self.event_types,
            filters=filters,
        )

    def _root_alias(self) -> str:
        return "e"

    def _root_kind(self) -> ScopeKind:
        return "event"

    def _root_table_name(self) -> str:
        return "event"

    def _type_filter_sql(self) -> Optional[str]:
        if not self.event_types:
            return None

        escaped_types = ", ".join(
            f"'{event_type.replace("'", "''")}'" for event_type in self.event_types
        )
        return f"e.ocel_type IN ({escaped_types})"

    def _materialize_sublog(self, target_schema: str) -> None:
        source_schema = self.ocel.schema
        con = self.ocel._con

        # Root events selected by this query.
        con.execute(f"""
            CREATE VIEW {target_schema}._root AS
            SELECT ocel_id
            FROM ({self.to_sql()}) q
        """)

        # Retained events are exactly the selected root events.
        con.execute(f"""
            CREATE VIEW {target_schema}.event AS
            SELECT DISTINCT e.*
            FROM {source_schema}.event e
            JOIN {target_schema}._root r
              ON e.ocel_id = r.ocel_id
        """)

        self._materialize_common_sublog_views(target_schema)

    def __repr__(self) -> str:
        return (
            f"EventQuery(event_types={self.event_types!r}, filters={len(self.filters)})"
        )
