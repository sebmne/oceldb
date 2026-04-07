from __future__ import annotations

from dataclasses import dataclass

from oceldb.expressions.base import Expr
from oceldb.expressions.context import Context
from oceldb.expressions.scopes import Scope


def related(object_type: str) -> "Related":
    """
    Build a related-object scope.

    Semantics:
        objects of the given type that co-participate in at least one event with
        the current root object.

    Example:
        related("Item")
        related("Item").where(attr("price", cast="DOUBLE") > 100)
        related("Item").count() > 2
    """
    return Related(object_type=object_type)


@dataclass(frozen=True)
class Related(Scope):
    """
    Scope representing objects of a given type related to the current root
    object through shared events.
    """

    object_type: str = ""

    def _clone_with_filters(self, filters: tuple[Expr, ...]) -> "Related":
        return Related(object_type=self.object_type, filters=filters)

    def _scope_sql(self, ctx: Context) -> str:
        ctx.require_kind("object")
        child_ctx = ctx.child("oi", kind="object")

        escaped_type = self.object_type.replace("'", "''")

        sql = f"""
            FROM {ctx.table("event_object")} eo1
            JOIN {ctx.table("event_object")} eo2
              ON eo1.ocel_event_id = eo2.ocel_event_id
            JOIN {ctx.table("object")} oi
              ON eo2.ocel_object_id = oi.ocel_id
            WHERE eo1.ocel_object_id = {ctx.alias}.ocel_id
              AND oi.ocel_type = '{escaped_type}'
              AND oi.ocel_id != {ctx.alias}.ocel_id
        """

        for expr in self.filters:
            sql += f"\n  AND {expr.to_sql(child_ctx)}"

        return sql
