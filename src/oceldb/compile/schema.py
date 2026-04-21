"""Per-node schema analysis — the bridge between plan IR and SQL emission.

``analyze_node`` walks a plan tree and returns, for the given node, the set of
visible output columns and the current ``ScopeKind``. The compiler uses it to
build the right ``CompileContext`` for each level; validation uses it to check
column references.

``output_name`` is the column-name derivation rule for projections — e.g. the
alias of an ``AliasExpr``, the column name of a ``ColumnExpr``, or ``None``
when no stable name exists (in which case downstream operators must see the
expression aliased).
"""

from __future__ import annotations

from dataclasses import dataclass

from oceldb.core.manifest import OCELManifest
from oceldb.expr.nodes import (
    AliasExpr,
    AvgAgg,
    BinaryOpExpr,
    CaseExpr,
    CastExpr,
    ColumnExpr,
    CountAgg,
    Expr,
    FunctionExpr,
    MaxAgg,
    MinAgg,
    RelationCountExpr,
    SumAgg,
    WindowFunctionExpr,
)
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
)
from oceldb.plan.scope import ScopeKind
from oceldb.plan.sources import ObjectStateSource, source_available_columns


@dataclass(frozen=True)
class NodeAnalysis:
    columns: dict[str, str]
    current_kind: ScopeKind


def query_output_columns(node: PlanNode, manifest: OCELManifest) -> dict[str, str]:
    return analyze_node(node, manifest).columns


def analyze_node(
    node: PlanNode,
    manifest: OCELManifest,
    *,
    has_parent: bool = False,
) -> NodeAnalysis:
    if isinstance(node, SourcePlan):
        source = node.source
        if isinstance(source, ObjectStateSource) and source.mode is None:
            raise ValueError(
                "object_states(...) queries require an explicit temporal projection; "
                "call .latest() or .as_of(timestamp)"
            )
        columns = dict(source_available_columns(source, manifest))
        return NodeAnalysis(columns=columns, current_kind=source.scope())

    if isinstance(node, (FilterPlan, SortPlan, DistinctPlan, LimitPlan)):
        return analyze_node(node.input, manifest, has_parent=True)

    if isinstance(node, HavingPlan):
        child = analyze_node(node.input, manifest, has_parent=True)
        if child.current_kind != "grouped":
            raise ValueError("having(...) is only valid after group_by(...).agg(...)")
        return child

    if isinstance(node, ExtendPlan):
        child = analyze_node(node.input, manifest, has_parent=True)
        columns = dict(child.columns)
        for expr in node.assignments:
            if not isinstance(expr, AliasExpr):
                raise ValueError("with_columns(...) assignments must be aliased")
            columns[expr.name] = "UNKNOWN"
        return NodeAnalysis(columns=columns, current_kind=child.current_kind)

    if isinstance(node, ProjectPlan):
        child = analyze_node(node.input, manifest, has_parent=True)
        return NodeAnalysis(
            columns=derive_output_columns(node.projections, has_following_ops=has_parent),
            current_kind=child.current_kind,
        )

    if isinstance(node, RenamePlan):
        child = analyze_node(node.input, manifest, has_parent=True)
        rename_map = dict(node.renames)
        columns = {
            rename_map.get(name, name): sql_type
            for name, sql_type in child.columns.items()
        }
        return NodeAnalysis(columns=columns, current_kind=child.current_kind)

    if isinstance(node, GroupPlan):
        analyze_node(node.input, manifest, has_parent=True)
        if not node.keys:
            raise ValueError("group_by(...).agg(...) requires at least one grouping")
        return NodeAnalysis(
            columns=derive_output_columns(
                (*node.keys, *node.aggregations),
                has_following_ops=has_parent,
            ),
            current_kind="grouped",
        )

    raise TypeError(f"Unsupported plan node: {type(node).__name__}")


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
                    "Expressions used before later query operations must have a stable "
                    "output name; add .alias('name')."
                )
            continue
        columns[name] = "UNKNOWN"
    return columns


def output_name(expr: Expr) -> str | None:
    if isinstance(expr, AliasExpr):
        return expr.name
    if isinstance(expr, ColumnExpr):
        return expr.name
    if isinstance(expr, CastExpr):
        return output_name(expr.expr)
    if isinstance(expr, FunctionExpr):
        return _func_output_name(expr.name, expr.args)
    if isinstance(expr, WindowFunctionExpr):
        return _func_output_name(expr.name, expr.args)
    if isinstance(expr, CountAgg):
        if expr.expr is None:
            return "count"
        inner = output_name(expr.expr)
        prefix = "count_distinct" if expr.distinct else "count"
        return prefix if inner is None else f"{prefix}_{inner}"
    if isinstance(expr, SumAgg):
        inner = output_name(expr.expr)
        return None if inner is None else f"sum_{inner}"
    if isinstance(expr, AvgAgg):
        inner = output_name(expr.expr)
        return None if inner is None else f"avg_{inner}"
    if isinstance(expr, MinAgg):
        inner = output_name(expr.expr)
        return None if inner is None else f"min_{inner}"
    if isinstance(expr, MaxAgg):
        inner = output_name(expr.expr)
        return None if inner is None else f"max_{inner}"
    if isinstance(expr, RelationCountExpr):
        return f"{expr.target.kind}_count"
    if isinstance(expr, (BinaryOpExpr, CaseExpr)):
        return None
    return None


def _func_output_name(name: str, args: tuple[Expr, ...]) -> str | None:
    lower_name = name.lower()
    if not args:
        return lower_name
    inner = args[0]
    inner_name = output_name(inner)
    return lower_name if inner_name is None else f"{lower_name}_{inner_name}"


__all__ = [
    "NodeAnalysis",
    "analyze_node",
    "derive_output_columns",
    "output_name",
    "query_output_columns",
]
