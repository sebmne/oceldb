"""SQL emission for plan nodes.

``compile_query`` takes a plan tree plus the dataset's manifest and produces a
self-contained SQL string. The compiler nests each operator as an outer
``SELECT ... FROM (...) qN`` around its child, with the alias depth matching
the plan depth so every level has a stable name to reach into.
"""

from __future__ import annotations

from oceldb.compile.context import CompileContext, quote_ident
from oceldb.compile.expr import render_expr, render_order_expr
from oceldb.compile.schema import analyze_node
from oceldb.compile.sources import render_source
from oceldb.compile.validate import validate_query
from oceldb.core.manifest import OCELManifest
from oceldb.plan.nodes import (
    DistinctPlan,
    ExtendPlan,
    FilterPlan,
    GroupPlan,
    HavingPlan,
    LimitPlan,
    PlanNode,
    ProjectPlan,
    RenamePlan,
    SortPlan,
    SourcePlan,
    plan_depth,
    root_source,
)
from oceldb.plan.scope import ScopeKind
from oceldb.plan.sources import ObjectStateSource


def compile_query(node: PlanNode, manifest: OCELManifest) -> str:
    validate_query(node, manifest)
    return _render_node(node, manifest)


def _render_node(node: PlanNode, manifest: OCELManifest) -> str:
    if isinstance(node, SourcePlan):
        return _render_source_select(node, manifest)

    child_sql = _render_node(node.input, manifest)  # type: ignore[attr-defined]
    child_analysis = analyze_node(node.input, manifest, has_parent=True)  # type: ignore[attr-defined]
    alias = _alias_for(node)
    ctx = _compile_context(
        node,
        manifest,
        alias=alias,
        kind=child_analysis.current_kind,
    )

    if isinstance(node, FilterPlan):
        where_sql = " AND ".join(render_expr(p, ctx) for p in node.predicates)
        return f"SELECT * FROM ({child_sql}) {alias} WHERE {where_sql}"

    if isinstance(node, HavingPlan):
        where_sql = " AND ".join(render_expr(p, ctx) for p in node.predicates)
        return f"SELECT * FROM ({child_sql}) {alias} WHERE {where_sql}"

    if isinstance(node, ExtendPlan):
        additions = ", ".join(render_expr(expr, ctx) for expr in node.assignments)
        return f"SELECT {alias}.*, {additions} FROM ({child_sql}) {alias}"

    if isinstance(node, ProjectPlan):
        select_sql = ", ".join(render_expr(expr, ctx) for expr in node.projections)
        return f"SELECT {select_sql} FROM ({child_sql}) {alias}"

    if isinstance(node, RenamePlan):
        rename_map = dict(node.renames)
        select_sql = ", ".join(
            (
                f"{alias}.{quote_ident(name)} AS {quote_ident(rename_map[name])}"
                if name in rename_map
                else f"{alias}.{quote_ident(name)}"
            )
            for name in child_analysis.columns
        )
        return f"SELECT {select_sql} FROM ({child_sql}) {alias}"

    if isinstance(node, GroupPlan):
        select_parts = [
            *(render_expr(expr, ctx) for expr in node.keys),
            *(render_expr(expr, ctx) for expr in node.aggregations),
        ]
        group_sql = ", ".join(render_expr(expr, ctx) for expr in node.keys)
        return (
            f"SELECT {', '.join(select_parts)} "
            f"FROM ({child_sql}) {alias} "
            f"GROUP BY {group_sql}"
        )

    if isinstance(node, SortPlan):
        order_sql = ", ".join(render_order_expr(o, ctx) for o in node.orderings)
        return f"SELECT * FROM ({child_sql}) {alias} ORDER BY {order_sql}"

    if isinstance(node, DistinctPlan):
        return f"SELECT DISTINCT * FROM ({child_sql}) {alias}"

    if isinstance(node, LimitPlan):
        return f"SELECT * FROM ({child_sql}) {alias} LIMIT {node.n}"

    raise TypeError(f"Unsupported plan node: {type(node).__name__}")


def _render_source_select(plan: SourcePlan, manifest: OCELManifest) -> str:
    source = plan.source
    alias = "root"
    ctx = _compile_context(plan, manifest, alias=alias, kind=source.scope())
    from_sql = render_source(source, alias, ctx)
    return f"SELECT * FROM {from_sql}"


def _alias_for(node: PlanNode) -> str:
    return f"q{plan_depth(node) - 1}"


def _compile_context(
    node: PlanNode,
    manifest: OCELManifest,
    *,
    alias: str,
    kind: ScopeKind,
    event_alias: str | None = None,
) -> CompileContext:
    source = root_source(node)
    object_state_mode = (
        source.mode if isinstance(source, ObjectStateSource) else None
    )
    object_change_columns = tuple(manifest.table("object_change").columns)
    return CompileContext(
        alias=alias,
        kind=kind,
        object_change_columns=object_change_columns,
        object_state_mode=object_state_mode,
        event_alias=event_alias,
    )


__all__ = ["compile_query"]
