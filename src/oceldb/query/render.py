from __future__ import annotations

from typing import assert_never

from oceldb.query.plan import (
    DistinctPlan,
    ExtendPlan,
    FilterPlan,
    GroupPlan,
    HavingPlan,
    LimitPlan,
    ProjectPlan,
    QueryPlan,
    QueryPlanNode,
    SortPlan,
    SourcePlan,
    object_state_as_of,
    object_state_mode,
    root_source,
    selected_types,
    source_kind,
    SourceSpec,
)
from oceldb.query.schema import analyze_node
from oceldb.query.validate import validate_query
from oceldb.sql.context import CompileContext, ExprScopeKind, render_object_state_source
from oceldb.sql.render_expr import render_compare_value, render_expr, render_order_expr


def compile_query(query: QueryPlan) -> str:
    validate_query(query)
    return _render_node(query.node, query)


def _render_node(node: QueryPlanNode, query: QueryPlan) -> str:
    match node:
        case SourcePlan(source=source):
            return _render_source(source, query)

        case FilterPlan(input=inner, predicates=predicates):
            child_sql = _render_node(inner, query)
            child_analysis = analyze_node(inner, query, has_parent=True)
            alias = _alias_for(node)
            ctx = _compile_context(query, alias=alias, kind=child_analysis.current_kind)
            where_sql = " AND ".join(render_expr(expr, ctx) for expr in predicates)
            return f"SELECT * FROM ({child_sql}) {alias} WHERE {where_sql}"

        case HavingPlan(input=inner, predicates=predicates):
            child_sql = _render_node(inner, query)
            child_analysis = analyze_node(inner, query, has_parent=True)
            alias = _alias_for(node)
            ctx = _compile_context(query, alias=alias, kind=child_analysis.current_kind)
            where_sql = " AND ".join(render_expr(expr, ctx) for expr in predicates)
            return f"SELECT * FROM ({child_sql}) {alias} WHERE {where_sql}"

        case ExtendPlan(input=inner, assignments=assignments):
            child_sql = _render_node(inner, query)
            child_analysis = analyze_node(inner, query, has_parent=True)
            alias = _alias_for(node)
            ctx = _compile_context(query, alias=alias, kind=child_analysis.current_kind)
            additions = ", ".join(render_expr(expr, ctx) for expr in assignments)
            return f"SELECT {alias}.*, {additions} FROM ({child_sql}) {alias}"

        case ProjectPlan(input=inner, projections=projections):
            child_sql = _render_node(inner, query)
            child_analysis = analyze_node(inner, query, has_parent=True)
            alias = _alias_for(node)
            ctx = _compile_context(query, alias=alias, kind=child_analysis.current_kind)
            select_sql = ", ".join(render_expr(expr, ctx) for expr in projections)
            return f"SELECT {select_sql} FROM ({child_sql}) {alias}"

        case GroupPlan(input=inner, keys=keys, aggregations=aggregations):
            child_sql = _render_node(inner, query)
            child_analysis = analyze_node(inner, query, has_parent=True)
            alias = _alias_for(node)
            ctx = _compile_context(query, alias=alias, kind=child_analysis.current_kind)
            select_parts = [
                *(render_expr(expr, ctx) for expr in keys),
                *(render_expr(expr, ctx) for expr in aggregations),
            ]
            group_sql = ", ".join(render_expr(expr, ctx) for expr in keys)
            return (
                f"SELECT {', '.join(select_parts)} "
                f"FROM ({child_sql}) {alias} "
                f"GROUP BY {group_sql}"
            )

        case SortPlan(input=inner, orderings=orderings):
            child_sql = _render_node(inner, query)
            child_analysis = analyze_node(inner, query, has_parent=True)
            alias = _alias_for(node)
            ctx = _compile_context(query, alias=alias, kind=child_analysis.current_kind)
            order_sql = ", ".join(render_order_expr(expr, ctx) for expr in orderings)
            return f"SELECT * FROM ({child_sql}) {alias} ORDER BY {order_sql}"

        case DistinctPlan(input=inner):
            child_sql = _render_node(inner, query)
            alias = _alias_for(node)
            return f"SELECT DISTINCT * FROM ({child_sql}) {alias}"

        case LimitPlan(input=inner, n=n):
            child_sql = _render_node(inner, query)
            alias = _alias_for(node)
            return f"SELECT * FROM ({child_sql}) {alias} LIMIT {n}"
    assert_never(node)


def _render_source(source: SourceSpec, query: QueryPlan) -> str:
    kind = source_kind(source)

    if kind == "object":
        source_sql = f'SELECT * FROM {query.ocel._table_sql("object")}'
    elif kind == "object_state":
        mode = object_state_mode(source)
        if mode is None:
            raise ValueError(
                "object_states(...) queries require an explicit temporal projection; "
                "call .latest() or .as_of(timestamp)"
            )
        source_sql = f"""
            SELECT *
            FROM (
                {render_object_state_source(
                    query.ocel._table_refs,
                    tuple(query.ocel._available_columns("object_change")),
                    mode=mode,
                    as_of=object_state_as_of(source),
                )}
            ) object_state
        """
    elif kind == "object_change":
        source_sql = f'SELECT * FROM {query.ocel._table_sql("object_change")}'
    else:
        source_sql = f'SELECT * FROM {query.ocel._table_sql(kind)}'

    types = selected_types(source)
    if not types:
        return source_sql

    values_sql = ", ".join(
        render_compare_value(
            value,
            _compile_context(query, alias="root", kind=kind),
        )
        for value in types
    )
    return f"{source_sql} WHERE \"ocel_type\" IN ({values_sql})"


def _alias_for(node: QueryPlanNode) -> str:
    from oceldb.query.plan import plan_depth

    return f"q{plan_depth(node) - 1}"


def _compile_context(
    query: QueryPlan,
    *,
    alias: str,
    kind: ExprScopeKind,
    event_alias: str | None = None,
) -> CompileContext:
    source = root_source(query.node)
    return CompileContext(
        alias=alias,
        kind=kind,
        table_refs=query.ocel._table_refs,
        object_change_columns=tuple(query.ocel._available_columns("object_change")),
        object_state_mode=object_state_mode(source),
        object_state_as_of=object_state_as_of(source),
        event_alias=event_alias,
    )
