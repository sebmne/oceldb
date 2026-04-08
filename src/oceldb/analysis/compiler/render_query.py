from __future__ import annotations

from oceldb.analysis.compiler.render_source import render_analysis_source
from oceldb.analysis.query.table_query import AnalysisTableKind, TableQuery
from oceldb.sql.context import CompileContext, ExprScopeKind
from oceldb.sql.render_expr import render_expr, render_order_expr


def render_table_query(query: TableQuery) -> str:
    """
    Compile an analytical table query into SQL.
    """
    rendered_source = render_analysis_source(query.ocel, query.table_kind)

    ctx = CompileContext(
        alias=rendered_source.alias,
        schema=query.ocel.schema,
        kind=_expr_scope_kind(query.table_kind),
    )

    select_parts: list[str] = []

    if query.selections:
        select_parts.extend(render_expr(expr, ctx) for expr in query.selections)

    if query.aggregations:
        select_parts.extend(render_expr(expr, ctx) for expr in query.aggregations)

    if not select_parts:
        if query.groupings:
            select_parts.extend(render_expr(expr, ctx) for expr in query.groupings)
        else:
            select_parts.append("*")

    distinct_sql = "DISTINCT " if query.is_distinct else ""
    select_sql = ", ".join(select_parts)

    sql = f"""
        SELECT {distinct_sql}{select_sql}
        FROM {rendered_source.from_sql}
    """

    if query.groupings:
        group_by_sql = ", ".join(render_expr(expr, ctx) for expr in query.groupings)
        sql += f"\nGROUP BY {group_by_sql}"

    if query.orderings:
        order_by_sql = ", ".join(
            render_order_expr(expr, ctx) for expr in query.orderings
        )
        sql += f"\nORDER BY {order_by_sql}"

    if query.limit_n is not None:
        sql += f"\nLIMIT {query.limit_n}"

    return sql


def _expr_scope_kind(table_kind: AnalysisTableKind) -> ExprScopeKind:
    match table_kind:
        case "event":
            return "event"
        case "object":
            return "object"
        case "event_object":
            return "event_object"
        case "object_object":
            return "object_object"
        case _:
            raise TypeError(f"Unsupported analysis table kind: {table_kind!r}")
