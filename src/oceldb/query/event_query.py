from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Tuple

from oceldb.ast.base import BoolExpr
from oceldb.compiler.context import ScopeKind
from oceldb.dsl import type_
from oceldb.query.base import BaseQuery


@dataclass(frozen=True)
class EventQuery(BaseQuery):
    """
    Root query over OCEL events.
    """

    event_types: Tuple[str, ...] = field(default_factory=tuple)

    root_alias: str = field(init=False, default="e")
    root_kind: ScopeKind = field(init=False, default="event")
    root_table_name: str = field(init=False, default="event")
    id_column: str = field(init=False, default="ocel_id")

    def _clone_with_filters(self, filters: Tuple[BoolExpr, ...]):
        return EventQuery(
            ocel=self.ocel,
            filters=filters,
            event_types=self.event_types,
        )

    def _type_filter_expr(self) -> Optional[BoolExpr]:
        if not self.event_types:
            return None

        if len(self.event_types) == 1:
            return type_() == self.event_types[0]

        from oceldb.dsl.functions import in_

        return in_(type_(), self.event_types)

    def _materialize_sublog(self, target_schema: str) -> None:
        source_schema = self.ocel.schema
        con = self.ocel._con

        con.execute(f"""
            CREATE VIEW {target_schema}._root AS
            SELECT DISTINCT ocel_id
            FROM ({self.to_sql()}) q
        """)

        con.execute(f"""
            CREATE VIEW {target_schema}.event AS
            SELECT DISTINCT e.*
            FROM {source_schema}.event e
            JOIN {target_schema}._root r
              ON e.ocel_id = r.ocel_id
        """)
