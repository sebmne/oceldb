from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from oceldb.expressions.base import Expr
from oceldb.expressions.context import ScopeKind
from oceldb.query.base import BaseQuery


@dataclass(frozen=True)
class ObjectQuery(BaseQuery):
    object_types: tuple[str, ...] = ()

    def _clone_with_filters(self, filters: tuple[Expr, ...]) -> "ObjectQuery":
        return ObjectQuery(
            ocel=self.ocel,
            object_types=self.object_types,
            filters=filters,
        )

    def _root_alias(self) -> str:
        return "o"

    def _root_kind(self) -> ScopeKind:
        return "object"

    def _root_table_name(self) -> str:
        return "object"

    def _type_filter_sql(self) -> Optional[str]:
        if not self.object_types:
            return None

        escaped_types = ", ".join(
            f"'{object_type.replace("'", "''")}'" for object_type in self.object_types
        )
        return f"o.ocel_type IN ({escaped_types})"

    def _materialize_sublog(self, target_schema: str) -> None:
        source_schema = self.ocel.schema
        con = self.ocel._con

        # Root objects selected by this query.
        con.execute(f"""
            CREATE VIEW {target_schema}._root AS
            SELECT ocel_id
            FROM ({self.to_sql()}) q
        """)

        # All events attached to the selected root objects.
        con.execute(f"""
            CREATE VIEW {target_schema}.event AS
            SELECT DISTINCT e.*
            FROM {source_schema}.event e
            JOIN {source_schema}.event_object eo
              ON e.ocel_id = eo.ocel_event_id
            JOIN {target_schema}._root r
              ON eo.ocel_object_id = r.ocel_id
        """)

        self._materialize_common_sublog_views(target_schema)

    def __repr__(self) -> str:
        return (
            f"ObjectQuery("
            f"object_types={self.object_types!r}, "
            f"filters={len(self.filters)}"
            f")"
        )
