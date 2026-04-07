from __future__ import annotations

from dataclasses import dataclass

from oceldb.expressions.base import Expr
from oceldb.expressions.context import Context
from oceldb.expressions.scopes import Scope


def linked(object_type: str) -> Linked:
    """
    Build an object-object linked scope.

    Semantics:
        objects of the given type that are directly connected to the current
        root object through one object_object edge.

    The traversal is undirected by default.

    Examples:
        linked("Container")
        linked("Container").exists()
        linked("Container").where(attr("status") == "active").count() > 1
    """
    return Linked(object_type=object_type)


@dataclass(frozen=True)
class Linked(Scope):
    """
    Scope representing objects directly connected to the current root object
    through the object_object relation.
    """

    object_type: str = ""

    def _clone_with_filters(self, filters: tuple[Expr, ...]) -> Linked:
        return Linked(object_type=self.object_type, filters=filters)

    def _scope_sql(self, ctx: Context) -> str:
        ctx.require_kind("object")
        child_ctx = ctx.child("oi", kind="object")

        escaped_type = self.object_type.replace("'", "''")

        sql = f"""
            FROM {ctx.table("object_object")} oo
            JOIN {ctx.table("object")} oi
              ON (
                   (oo.ocel_source_id = {ctx.alias}.ocel_id AND oi.ocel_id = oo.ocel_target_id)
                OR (oo.ocel_target_id = {ctx.alias}.ocel_id AND oi.ocel_id = oo.ocel_source_id)
              )
            WHERE oi.ocel_type = '{escaped_type}'
              AND oi.ocel_id != {ctx.alias}.ocel_id
        """

        for expr in self.filters:
            sql += f"\n  AND {expr.to_sql(child_ctx)}"

        return sql
