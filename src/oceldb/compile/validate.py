"""Plan and expression validation.

``validate_query`` walks the plan tree and validates each operator against the
schema produced by ``analyze_node``. Column references, scope compatibility of
relation predicates, and window/aggregate placement rules are all enforced
here so compilation can assume a well-formed tree.
"""

from __future__ import annotations

from oceldb.compile.schema import analyze_node
from oceldb.core.manifest import OCELManifest
from oceldb.expr.nodes import (
    AggregateExpr,
    AliasExpr,
    AvgAgg,
    BinaryOpExpr,
    BoolOpExpr,
    CaseExpr,
    CastExpr,
    ColumnExpr,
    CompareExpr,
    CountAgg,
    Expr,
    ExprVisitor,
    FunctionExpr,
    InExpr,
    LiteralExpr,
    MaxAgg,
    MinAgg,
    NotExpr,
    PredicateFunctionExpr,
    RelationAllExpr,
    RelationCountExpr,
    RelationExistsExpr,
    RelationKind,
    RelationTarget,
    SortExpr,
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
from oceldb.plan.sources import source_available_columns

# Re-export via import for relation target columns
from oceldb.plan.sources import (
    EventSource,
    ObjectSource,
    ObjectStateSource,
)


def validate_query(node: PlanNode, manifest: OCELManifest) -> None:
    _validate_node(node, manifest, has_parent=False)


def _validate_node(
    node: PlanNode,
    manifest: OCELManifest,
    *,
    has_parent: bool,
) -> None:
    analyze_node(node, manifest, has_parent=has_parent)

    if isinstance(node, SourcePlan):
        return

    child = analyze_node(node.input, manifest, has_parent=True)  # type: ignore[attr-defined]

    if isinstance(node, (FilterPlan, HavingPlan)):
        _validate_node(node.input, manifest, has_parent=True)
        for expr in node.predicates:
            validate_expr(expr, child.columns, child.current_kind, manifest)
        return

    if isinstance(node, ExtendPlan):
        _validate_node(node.input, manifest, has_parent=True)
        for expr in node.assignments:
            if not isinstance(expr, AliasExpr):
                raise ValueError("with_columns(...) assignments must be aliased")
            validate_expr(expr.expr, child.columns, child.current_kind, manifest)
        return

    if isinstance(node, ProjectPlan):
        _validate_node(node.input, manifest, has_parent=True)
        for expr in node.projections:
            validate_expr(expr, child.columns, child.current_kind, manifest)
        return

    if isinstance(node, RenamePlan):
        _validate_node(node.input, manifest, has_parent=True)
        return

    if isinstance(node, GroupPlan):
        _validate_node(node.input, manifest, has_parent=True)
        for expr in node.keys:
            validate_expr(expr, child.columns, child.current_kind, manifest)
        for expr in node.aggregations:
            validate_expr(expr, child.columns, child.current_kind, manifest)
        return

    if isinstance(node, SortPlan):
        _validate_node(node.input, manifest, has_parent=True)
        for ordering in node.orderings:
            validate_sort_expr(ordering, child.columns, child.current_kind, manifest)
        return

    if isinstance(node, (DistinctPlan, LimitPlan)):
        _validate_node(node.input, manifest, has_parent=True)
        return

    raise TypeError(f"Unsupported plan node: {type(node).__name__}")


def validate_sort_expr(
    sort: SortExpr,
    columns: dict[str, str],
    current_kind: ScopeKind,
    manifest: OCELManifest,
) -> None:
    inner = sort.expr
    if isinstance(inner, ColumnExpr):
        if inner.name not in columns:
            raise ValueError(f"Unknown sort column {inner.name!r}")
        return
    validate_expr(inner, columns, current_kind, manifest)


def validate_expr(
    expr: Expr,
    columns: dict[str, str],
    current_kind: ScopeKind,
    manifest: OCELManifest,
) -> None:
    ValidationVisitor(columns, current_kind, manifest).visit(expr)


def _relation_target_kind(kind: RelationKind, current_kind: ScopeKind) -> ScopeKind:
    if kind in {"cooccurs_with", "linked"}:
        return "object_state" if current_kind == "object_state" else "object"
    if kind == "has_event":
        return "event"
    if kind == "has_object":
        return "object_state_at_event"
    raise TypeError(f"Unsupported relation kind: {kind!r}")


def _relation_target_columns(
    target: RelationTarget,
    current_kind: ScopeKind,
    manifest: OCELManifest,
) -> dict[str, str]:
    kind = _relation_target_kind(target.kind, current_kind)
    if kind == "event":
        return dict(source_available_columns(
            EventSource(selected_types=(target.type_name,)),
            manifest,
        ))
    if kind == "object":
        return dict(source_available_columns(
            ObjectSource(selected_types=(target.type_name,)),
            manifest,
        ))
    if kind == "object_state":
        return dict(source_available_columns(
            ObjectStateSource(
                selected_types=(target.type_name,),
                mode=("latest", None),
            ),
            manifest,
        ))
    if kind == "object_state_at_event":
        # Same column shape as object_state.
        return dict(source_available_columns(
            ObjectStateSource(
                selected_types=(target.type_name,),
                mode=("latest", None),
            ),
            manifest,
        ))
    raise TypeError(f"Unsupported relation target kind: {kind!r}")


class ValidationVisitor(ExprVisitor[None]):
    def __init__(
        self,
        columns: dict[str, str],
        current_kind: ScopeKind,
        manifest: OCELManifest,
    ) -> None:
        self.columns: dict[str, str] = columns
        self.current_kind: ScopeKind = current_kind
        self.manifest: OCELManifest = manifest

    def visit_ColumnExpr(self, expr: ColumnExpr) -> None:
        if expr.name not in self.columns:
            raise ValueError(
                f"Unknown column {expr.name!r} in {self.current_kind!r} scope"
            )

    def visit_LiteralExpr(self, expr: LiteralExpr) -> None:
        return None

    def visit_AliasExpr(self, expr: AliasExpr) -> None:
        self.visit(expr.expr)

    def visit_CastExpr(self, expr: CastExpr) -> None:
        self.visit(expr.expr)

    def visit_BinaryOpExpr(self, expr: BinaryOpExpr) -> None:
        self.visit(expr.left)
        self.visit(expr.right)

    def visit_CompareExpr(self, expr: CompareExpr) -> None:
        self.visit(expr.left)
        self.visit(expr.right)

    def visit_BoolOpExpr(self, expr: BoolOpExpr) -> None:
        for operand in expr.operands:
            self.visit(operand)

    def visit_NotExpr(self, expr: NotExpr) -> None:
        self.visit(expr.operand)

    def visit_InExpr(self, expr: InExpr) -> None:
        self.visit(expr.expr)

    def visit_FunctionExpr(self, expr: FunctionExpr) -> None:
        for arg in expr.args:
            self.visit(arg)

    def visit_PredicateFunctionExpr(self, expr: PredicateFunctionExpr) -> None:
        for arg in expr.args:
            self.visit(arg)

    def visit_CaseExpr(self, expr: CaseExpr) -> None:
        for condition, value in expr.branches:
            self.visit(condition)
            self.visit(value)
        if expr.default is not None:
            self.visit(expr.default)

    def visit_WindowFunctionExpr(self, expr: WindowFunctionExpr) -> None:
        for arg in expr.args:
            self.visit(arg)
        for partition in expr.partition_by:
            self.visit(partition)
        for ordering in expr.order_by:
            validate_sort_expr(ordering, self.columns, self.current_kind, self.manifest)

    # Aggregates — validate the inner expressions.

    def visit_CountAgg(self, expr: CountAgg) -> None:
        if expr.expr is not None:
            self.visit(expr.expr)

    def visit_SumAgg(self, expr: SumAgg) -> None:
        self.visit(expr.expr)

    def visit_AvgAgg(self, expr: AvgAgg) -> None:
        self.visit(expr.expr)

    def visit_MinAgg(self, expr: MinAgg) -> None:
        self.visit(expr.expr)

    def visit_MaxAgg(self, expr: MaxAgg) -> None:
        self.visit(expr.expr)

    # Relations.

    def visit_RelationExistsExpr(self, expr: RelationExistsExpr) -> None:
        self._validate_relation(expr.target)
        if expr.predicate is not None:
            self._validate_relation_predicate(expr.target, expr.predicate)

    def visit_RelationCountExpr(self, expr: RelationCountExpr) -> None:
        self._validate_relation(expr.target)
        if expr.predicate is not None:
            self._validate_relation_predicate(expr.target, expr.predicate)

    def visit_RelationAllExpr(self, expr: RelationAllExpr) -> None:
        self._validate_relation(expr.target)
        self._validate_relation_predicate(expr.target, expr.predicate)

    def _validate_relation(self, target: RelationTarget) -> None:
        if target.kind == "has_object":
            if self.current_kind != "event":
                raise ValueError("has_object(...) is only valid in event-rooted scope")
        else:
            if self.current_kind not in {"object", "object_state"}:
                raise ValueError(
                    f"{target.kind}(...) is only valid in object-rooted scope"
                )
        if "ocel_id" not in self.columns:
            raise ValueError(
                f"{target.kind}(...) requires the current query scope to contain 'ocel_id'"
            )

    def _validate_relation_predicate(
        self,
        target: RelationTarget,
        predicate: Expr,
    ) -> None:
        target_kind = _relation_target_kind(target.kind, self.current_kind)
        target_columns = _relation_target_columns(target, self.current_kind, self.manifest)
        nested = ValidationVisitor(target_columns, target_kind, self.manifest)
        nested.visit(predicate)


# ---------------------------------------------------------------------------
# Aggregate / window containment predicates — used by the api/ layer.
# ---------------------------------------------------------------------------


def contains_aggregate(expr: Expr) -> bool:
    if isinstance(expr, AggregateExpr):
        return True
    for child in expr.children():
        if contains_aggregate(child):
            return True
    return False


def contains_window(expr: Expr) -> bool:
    if isinstance(expr, WindowFunctionExpr):
        return True
    for child in expr.children():
        if contains_window(child):
            return True
    return False


__all__ = [
    "ValidationVisitor",
    "contains_aggregate",
    "contains_window",
    "validate_expr",
    "validate_query",
    "validate_sort_expr",
]
