from __future__ import annotations

from dataclasses import dataclass
from typing import assert_never

from oceldb.ast.aggregation import AvgAgg, CountAgg, CountDistinctAgg, MaxAgg, MinAgg, SumAgg
from oceldb.ast.base import (
    AliasExpr,
    AndExpr,
    CastExpr,
    CompareExpr,
    Expr,
    ExprVisitor,
    InExpr,
    LiteralExpr,
    NotExpr,
    OrExpr,
    SortExpr,
    UnaryPredicate,
)
from oceldb.ast.field import ColumnExpr
from oceldb.ast.relation import RelationAllExpr, RelationCountExpr, RelationExistsExpr, RelationKind, RelationSpec
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
)
from oceldb.query.schema import analyze_node
from oceldb.sql.context import ExprScopeKind


def validate_query(query: QueryPlan) -> None:
    _validate_node(query.node, query, has_parent=False)


def _validate_node(
    node: QueryPlanNode,
    query: QueryPlan,
    *,
    has_parent: bool,
) -> None:
    analyze_node(node, query, has_parent=has_parent)

    match node:
        case SourcePlan():
            return

        case FilterPlan(input=inner, predicates=predicates):
            _validate_node(inner, query, has_parent=True)
            child = analyze_node(inner, query, has_parent=True)
            for expr in predicates:
                validate_expr(expr, child.columns, child.current_kind, query)
            return

        case HavingPlan(input=inner, predicates=predicates):
            _validate_node(inner, query, has_parent=True)
            child = analyze_node(inner, query, has_parent=True)
            for expr in predicates:
                validate_expr(expr, child.columns, child.current_kind, query)
            return

        case ExtendPlan(input=inner, assignments=assignments):
            _validate_node(inner, query, has_parent=True)
            child = analyze_node(inner, query, has_parent=True)
            for expr in assignments:
                if not isinstance(expr, AliasExpr):
                    raise ValueError("with_columns(...) assignments must be aliased")
                validate_expr(expr.expr, child.columns, child.current_kind, query)
            return

        case ProjectPlan(input=inner, projections=projections):
            _validate_node(inner, query, has_parent=True)
            child = analyze_node(inner, query, has_parent=True)
            for expr in projections:
                validate_expr(expr, child.columns, child.current_kind, query)
            return

        case GroupPlan(input=inner, keys=keys, aggregations=aggregations):
            _validate_node(inner, query, has_parent=True)
            child = analyze_node(inner, query, has_parent=True)
            for expr in keys:
                validate_expr(expr, child.columns, child.current_kind, query)
            for expr in aggregations:
                validate_expr(expr, child.columns, child.current_kind, query)
            return

        case SortPlan(input=inner, orderings=orderings):
            _validate_node(inner, query, has_parent=True)
            child = analyze_node(inner, query, has_parent=True)
            for ordering in orderings:
                validate_sort_expr(ordering, child.columns, child.current_kind, query)
            return

        case DistinctPlan(input=inner) | LimitPlan(input=inner):
            _validate_node(inner, query, has_parent=True)
            return

    assert_never(node)


def validate_sort_expr(
    expr: SortExpr,
    columns: dict[str, str],
    current_kind: ExprScopeKind,
    query: QueryPlan,
) -> None:
    match expr.expr:
        case str() as name:
            if name not in columns:
                raise ValueError(f"Unknown sort column {name!r}")
        case _:
            validate_expr(expr.expr, columns, current_kind, query)


def validate_expr(
    expr: Expr,
    columns: dict[str, str],
    current_kind: ExprScopeKind,
    query: QueryPlan,
) -> None:
    ValidationVisitor(columns, current_kind, query).visit(expr)


def _relation_target_kind(kind: RelationKind, current_kind: ExprScopeKind) -> ExprScopeKind:
    match kind:
        case "related" | "linked":
            return "object_state" if current_kind == "object_state" else "object"
        case "has_event":
            return "event"
        case "has_object":
            return "object_state_at_event"
    assert_never(kind)


@dataclass
class ValidationVisitor(ExprVisitor[None]):
    columns: dict[str, str]
    current_kind: ExprScopeKind
    query: QueryPlan

    def visit(self, expr: Expr) -> None:
        expr.accept(self)

    def visit_column(self, expr: ColumnExpr) -> None:
        if expr.name not in self.columns:
            raise ValueError(f"Unknown column {expr.name!r} in {self.current_kind!r} scope")

    def visit_alias(self, expr: AliasExpr) -> None:
        self.visit(expr.expr)

    def visit_literal(self, expr: LiteralExpr) -> None:
        return None

    def visit_cast(self, expr: CastExpr) -> None:
        self.visit(expr.expr)

    def visit_compare(self, expr: CompareExpr) -> None:
        self.visit(expr.left)
        if isinstance(expr.right, Expr):
            self.visit(expr.right)

    def visit_unary_predicate(self, expr: UnaryPredicate) -> None:
        self.visit(expr.expr)

    def visit_and(self, expr: AndExpr) -> None:
        self.visit(expr.left)
        self.visit(expr.right)

    def visit_or(self, expr: OrExpr) -> None:
        self.visit(expr.left)
        self.visit(expr.right)

    def visit_not(self, expr: NotExpr) -> None:
        self.visit(expr.expr)

    def visit_in(self, expr: InExpr) -> None:
        self.visit(expr.expr)
        for value in expr.values:
            if isinstance(value, Expr):
                self.visit(value)

    def visit_count(self, expr: CountAgg) -> None:
        return None

    def visit_count_distinct(self, expr: CountDistinctAgg) -> None:
        self.visit(expr.expr)

    def visit_min(self, expr: MinAgg) -> None:
        self.visit(expr.expr)

    def visit_max(self, expr: MaxAgg) -> None:
        self.visit(expr.expr)

    def visit_sum(self, expr: SumAgg) -> None:
        self.visit(expr.expr)

    def visit_avg(self, expr: AvgAgg) -> None:
        self.visit(expr.expr)

    def visit_relation_exists(self, expr: RelationExistsExpr) -> None:
        self._validate_relation(expr.spec)

    def visit_relation_count(self, expr: RelationCountExpr) -> None:
        self._validate_relation(expr.spec)

    def visit_relation_all(self, expr: RelationAllExpr) -> None:
        self._validate_relation(expr.spec)
        target_kind = _relation_target_kind(expr.spec.kind, self.current_kind)
        target_columns = self.query.ocel._available_columns(target_kind)
        nested = ValidationVisitor(target_columns, target_kind, self.query)
        nested.visit(expr.condition)

    def _validate_relation(self, spec: RelationSpec) -> None:
        if spec.kind == "has_object":
            if self.current_kind != "event":
                raise ValueError("has_object(...) is only valid in event-rooted scope")
        else:
            if self.current_kind not in {"object", "object_state"}:
                raise ValueError(f"{spec.kind}(...) is only valid in object-rooted scope")

        if "ocel_id" not in self.columns:
            raise ValueError(
                f"{spec.kind}(...) requires the current query scope to contain 'ocel_id'"
            )

        target_kind = _relation_target_kind(spec.kind, self.current_kind)
        target_columns = self.query.ocel._available_columns(target_kind)
        nested = ValidationVisitor(target_columns, target_kind, self.query)
        for expr in spec.filters:
            nested.visit(expr)
