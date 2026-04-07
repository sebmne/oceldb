from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from oceldb.expressions.base import Expr
from oceldb.expressions.context import Context
from oceldb.expressions.scopes import Scope


def has_event(event_type: Optional[str] = None) -> "HasEvent":
    """
    Build an event scope for events attached to the current root object.

    Example:
        has_event()
        has_event("Load")
        has_event("Load").where(attr("resource") == "System").exists()
        has_event("Load").count() > 1
    """
    return HasEvent(event_type=event_type)


@dataclass(frozen=True)
class HasEvent(Scope):
    """
    Scope representing events attached to the current root object.

    If `event_type` is given, the scope is restricted to that event type.
    """

    event_type: Optional[str] = None

    def _clone_with_filters(self, filters: tuple[Expr, ...]) -> "HasEvent":
        return HasEvent(event_type=self.event_type, filters=filters)

    def _scope_sql(self, ctx: Context) -> str:
        ctx.require_kind("object")
        child_ctx = ctx.child("e", kind="event")

        sql = f"""
            FROM {ctx.table("event_object")} eo
            JOIN {ctx.table("event")} e
              ON eo.ocel_event_id = e.ocel_id
            WHERE eo.ocel_object_id = {ctx.alias}.ocel_id
        """

        if self.event_type is not None:
            escaped_type = self.event_type.replace("'", "''")
            sql += f"\n  AND e.ocel_type = '{escaped_type}'"

        for expr in self.filters:
            sql += f"\n  AND {expr.to_sql(child_ctx)}"

        return sql
