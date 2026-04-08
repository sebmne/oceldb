from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Tuple

from oceldb.ast.base import BoolExpr
from oceldb.compiler.context import ScopeKind
from oceldb.dsl import type_
from oceldb.query.base import BaseQuery


@dataclass(frozen=True)
class ObjectQuery(BaseQuery):
    """
    Root query over logical OCEL objects.
    """

    object_types: Tuple[str, ...] = field(default_factory=tuple)

    root_alias: str = field(init=False, default="o")
    root_kind: ScopeKind = field(init=False, default="object")
    root_table_name: str = field(init=False, default="object")
    id_column: str = field(init=False, default="ocel_id")

    def _clone_with_filters(self, filters: Tuple[BoolExpr, ...]):
        return ObjectQuery(
            ocel=self.ocel,
            filters=filters,
            object_types=self.object_types,
        )

    def count(self) -> int:
        """
        Count distinct logical objects, not raw object-history rows.
        """
        row = self.ocel.sql(
            f"SELECT COUNT(DISTINCT ocel_id) FROM ({self.to_sql()}) q"
        ).fetchone()
        if row is None:
            raise RuntimeError("COUNT query returned no rows")
        return int(row[0])

    def _type_filter_expr(self) -> Optional[BoolExpr]:
        if not self.object_types:
            return None

        if len(self.object_types) == 1:
            return type_() == self.object_types[0]

        from oceldb.dsl.functions import in_

        return in_(type_(), self.object_types)

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
            JOIN {source_schema}.event_object eo
              ON e.ocel_id = eo.ocel_event_id
            JOIN {target_schema}._root r
              ON eo.ocel_object_id = r.ocel_id
        """)
