from __future__ import annotations

from dataclasses import dataclass
from typing import assert_never

from oceldb.ast.base import AliasExpr, Expr
from oceldb.query.names import output_name
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
    object_state_mode,
    selected_types,
    source_kind,
)
from oceldb.sql.context import ExprScopeKind

type ScopeKind = ExprScopeKind


@dataclass(frozen=True)
class NodeAnalysis:
    columns: dict[str, str]
    current_kind: ScopeKind


def query_output_columns(query: QueryPlan) -> dict[str, str]:
    return analyze_node(query.node, query).columns


def analyze_node(
    node: QueryPlanNode,
    query: QueryPlan,
    *,
    has_parent: bool = False,
) -> NodeAnalysis:
    match node:
        case SourcePlan(source=source):
            kind = source_kind(source)
            if kind == "object_state" and object_state_mode(source) is None:
                raise ValueError(
                    "object_states(...) queries require an explicit temporal projection; "
                    "call .latest() or .as_of(timestamp)"
                )

            columns = dict(query.ocel._available_columns(kind))
            types = selected_types(source)
            if types and "ocel_type" not in columns:
                raise ValueError(f"{kind!r} does not support type filtering")
            return NodeAnalysis(columns=columns, current_kind=kind)

        case FilterPlan(input=inner) | SortPlan(input=inner) | DistinctPlan(input=inner) | LimitPlan(input=inner):
            return analyze_node(inner, query, has_parent=True)

        case HavingPlan(input=inner):
            child = analyze_node(inner, query, has_parent=True)
            if child.current_kind != "grouped":
                raise ValueError("having(...) is only valid after group_by(...).agg(...)")
            return child

        case ExtendPlan(input=inner, assignments=assignments):
            child = analyze_node(inner, query, has_parent=True)
            columns = dict(child.columns)
            for expr in assignments:
                if not isinstance(expr, AliasExpr):
                    raise ValueError("with_columns(...) assignments must be aliased")
                columns[expr.name] = "UNKNOWN"
            return NodeAnalysis(columns=columns, current_kind=child.current_kind)

        case ProjectPlan(input=inner, projections=projections):
            child = analyze_node(inner, query, has_parent=True)
            return NodeAnalysis(
                columns=derive_output_columns(projections, has_following_ops=has_parent),
                current_kind=child.current_kind,
            )

        case GroupPlan(input=inner, keys=keys, aggregations=aggregations):
            analyze_node(inner, query, has_parent=True)
            if not keys:
                raise ValueError("group_by(...).agg(...) requires at least one grouping")
            return NodeAnalysis(
                columns=derive_output_columns(
                    (*keys, *aggregations),
                    has_following_ops=has_parent,
                ),
                current_kind="grouped",
            )

    assert_never(node)


def derive_output_columns(
    exprs: tuple[Expr, ...],
    *,
    has_following_ops: bool,
) -> dict[str, str]:
    columns: dict[str, str] = {}
    for expr in exprs:
        name = output_name(expr)
        if name is None:
            if has_following_ops:
                raise ValueError(
                    "Expressions used before later query operations must have a stable output name; "
                    "add .alias('name')."
                )
            continue
        columns[name] = "UNKNOWN"
    return columns
