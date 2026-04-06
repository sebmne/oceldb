from __future__ import annotations

from dataclasses import dataclass

from oceldb.expressions.base import Expr
from oceldb.expressions.context import Context
from oceldb.query.base import BaseQuery


@dataclass(frozen=True)
class ObjectQuery(BaseQuery):
    object_types: tuple[str, ...] = ()

    def _context(self) -> Context:
        return Context.for_objects(schema=self.ocel.schema, alias="o")

    def _with_filters(self, filters: tuple[Expr, ...]) -> "ObjectQuery":
        return ObjectQuery(
            ocel=self.ocel,
            object_types=self.object_types,
            filters=filters,
        )

    def _root_alias(self) -> str:
        return "o"

    def _root_table_name(self) -> str:
        return "object"

    def _id_column(self) -> str:
        return "ocel_id"

    def _type_filter_sql(self) -> str | None:
        if not self.object_types:
            return None

        escaped_types = ", ".join(
            f"'{object_type.replace("'", "''")}'" for object_type in self.object_types
        )
        return f"o.ocel_type IN ({escaped_types})"

    def __repr__(self) -> str:
        return (
            f"ObjectQuery("
            f"object_types={self.object_types!r}, "
            f"filters={len(self.filters)}"
            f")"
        )
