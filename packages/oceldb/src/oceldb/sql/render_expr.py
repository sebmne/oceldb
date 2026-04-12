from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from oceldb.ast.aggregation import AvgAgg, CountAgg, CountDistinctAgg, MaxAgg, MinAgg, SumAgg
from oceldb.ast.base import (
    AliasExpr,
    AndExpr,
    CastExpr,
    CompareExpr,
    CompareValue,
    Expr,
    ExprVisitor,
    InExpr,
    LiteralExpr,
    NotExpr,
    SortExpr,
    UnaryPredicate,
    OrExpr,
)
from oceldb.ast.field import ColumnExpr
from oceldb.ast.relation import RelationAllExpr, RelationCountExpr, RelationExistsExpr, RelationSpec
from oceldb.sql.context import CompileContext


def render_expr(expr: Expr, ctx: CompileContext) -> str:
    return expr.accept(SQLRenderVisitor(ctx))


def render_order_expr(expr: SortExpr, ctx: CompileContext) -> str:
    direction = "DESC" if expr.descending else "ASC"
    match expr.expr:
        case str() as name:
            return f"{quote_ident(name)} {direction}"
        case _:
            return f"{render_expr(expr.expr, ctx)} {direction}"


def render_compare_value(value: CompareValue, ctx: CompileContext) -> str:
    if isinstance(value, Expr):
        return render_expr(value, ctx)
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    if isinstance(value, datetime):
        return f"'{value.isoformat(sep=' ')}'"
    if isinstance(value, date):
        return f"'{value.isoformat()}'"
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    raise TypeError(f"Unsupported comparison value: {value!r}")


class SQLRenderVisitor(ExprVisitor[str]):
    def __init__(self, ctx: CompileContext) -> None:
        self.ctx = ctx

    def visit_column(self, expr: ColumnExpr) -> str:
        return f"{self.ctx.alias}.{quote_ident(expr.name)}"

    def visit_alias(self, expr: AliasExpr) -> str:
        return f"{render_expr(expr.expr, self.ctx)} AS {quote_ident(expr.alias)}"

    def visit_literal(self, expr: LiteralExpr) -> str:
        return render_compare_value(expr.value, self.ctx)

    def visit_cast(self, expr: CastExpr) -> str:
        return f"TRY_CAST({render_expr(expr.expr, self.ctx)} AS {expr.sql_type})"

    def visit_compare(self, expr: CompareExpr) -> str:
        left = render_expr(expr.left, self.ctx)
        right = render_compare_value(expr.right, self.ctx)

        if expr.right is None and expr.op in {"=", "!="}:
            op = "IS NULL" if expr.op == "=" else "IS NOT NULL"
            return f"({left} {op})"

        return f"({left} {expr.op} {right})"

    def visit_unary_predicate(self, expr: UnaryPredicate) -> str:
        return f"({render_expr(expr.expr, self.ctx)} {expr.op})"

    def visit_and(self, expr: AndExpr) -> str:
        return f"({render_expr(expr.left, self.ctx)} AND {render_expr(expr.right, self.ctx)})"

    def visit_or(self, expr: OrExpr) -> str:
        return f"({render_expr(expr.left, self.ctx)} OR {render_expr(expr.right, self.ctx)})"

    def visit_not(self, expr: NotExpr) -> str:
        return f"(NOT {render_expr(expr.expr, self.ctx)})"

    def visit_in(self, expr: InExpr) -> str:
        values_sql = ", ".join(
            render_compare_value(value, self.ctx)
            for value in expr.values
        )
        return f"({render_expr(expr.expr, self.ctx)} IN ({values_sql}))"

    def visit_count(self, expr: CountAgg) -> str:
        return "COUNT(*)"

    def visit_count_distinct(self, expr: CountDistinctAgg) -> str:
        return f"COUNT(DISTINCT {render_expr(expr.expr, self.ctx)})"

    def visit_min(self, expr: MinAgg) -> str:
        return f"MIN({render_expr(expr.expr, self.ctx)})"

    def visit_max(self, expr: MaxAgg) -> str:
        return f"MAX({render_expr(expr.expr, self.ctx)})"

    def visit_sum(self, expr: SumAgg) -> str:
        return f"SUM({render_expr(expr.expr, self.ctx)})"

    def visit_avg(self, expr: AvgAgg) -> str:
        return f"AVG({render_expr(expr.expr, self.ctx)})"

    def visit_relation_exists(self, expr: RelationExistsExpr) -> str:
        return f"EXISTS ({render_relation_subquery(expr.spec, self.ctx, select_sql='1')})"

    def visit_relation_count(self, expr: RelationCountExpr) -> str:
        return f"({render_relation_subquery(expr.spec, self.ctx, select_sql='COUNT(*)')})"

    def visit_relation_all(self, expr: RelationAllExpr) -> str:
        candidate_alias = relation_candidate_alias(expr.spec.kind)
        candidate_kind = relation_candidate_kind(expr.spec.kind)
        candidate_ctx = self.ctx.with_alias(candidate_alias, kind=candidate_kind)

        subquery = render_relation_subquery(
            expr.spec,
            self.ctx,
            select_sql="1",
            extra_predicates=[
                f"NOT ({render_expr(expr.condition, candidate_ctx)})",
            ],
        )
        return f"NOT EXISTS ({subquery})"


def render_relation_subquery(
    spec: RelationSpec,
    ctx: CompileContext,
    *,
    select_sql: str,
    extra_predicates: list[str] | None = None,
) -> str:
    match spec.kind:
        case "related":
            return _render_related_subquery(
                spec,
                ctx,
                select_sql=select_sql,
                extra_predicates=extra_predicates or [],
            )
        case "linked":
            return _render_linked_subquery(
                spec,
                ctx,
                select_sql=select_sql,
                extra_predicates=extra_predicates or [],
            )
        case "has_event":
            return _render_has_event_subquery(
                spec,
                ctx,
                select_sql=select_sql,
                extra_predicates=extra_predicates or [],
            )
        case _:
            raise TypeError(f"Unsupported relation kind: {spec.kind!r}")


def _render_related_subquery(
    spec: RelationSpec,
    ctx: CompileContext,
    *,
    select_sql: str,
    extra_predicates: list[str],
) -> str:
    if ctx.kind != "object":
        raise ValueError("related(...) is only valid in object-rooted scope")

    candidate_alias = "ro"
    candidate_ctx = ctx.with_alias(candidate_alias, kind="object")
    predicates = [
        f'eo1."ocel_object_id" = {ctx.alias}."ocel_id"',
        'eo1."ocel_event_id" = eo2."ocel_event_id"',
        f'eo2."ocel_object_id" = {candidate_alias}."ocel_id"',
        f'{candidate_alias}."ocel_type" = {render_compare_value(spec.target_type, ctx)}',
    ]

    predicates.extend(render_expr(expr, candidate_ctx) for expr in spec.filters)
    predicates.extend(extra_predicates)

    return f"""
        SELECT {select_sql}
        FROM {ctx.table("event_object")} eo1
        JOIN {ctx.table("event_object")} eo2
          ON eo1."ocel_event_id" = eo2."ocel_event_id"
        JOIN {ctx.table("object")} {candidate_alias}
          ON eo2."ocel_object_id" = {candidate_alias}."ocel_id"
        WHERE {" AND ".join(predicates)}
    """


def _render_linked_subquery(
    spec: RelationSpec,
    ctx: CompileContext,
    *,
    select_sql: str,
    extra_predicates: list[str],
) -> str:
    if ctx.kind != "object":
        raise ValueError("linked(...) is only valid in object-rooted scope")

    candidate_alias = "lo"
    candidate_ctx = ctx.with_alias(candidate_alias, kind="object")
    predicates = [
        f'{candidate_alias}."ocel_type" = {render_compare_value(spec.target_type, ctx)}',
    ]
    predicates.extend(render_expr(expr, candidate_ctx) for expr in spec.filters)
    predicates.extend(extra_predicates)

    return f"""
        SELECT {select_sql}
        FROM {ctx.table("object_object")} oo
        JOIN {ctx.table("object")} {candidate_alias}
          ON (
               (oo."ocel_source_id" = {ctx.alias}."ocel_id" AND oo."ocel_target_id" = {candidate_alias}."ocel_id")
            OR (oo."ocel_target_id" = {ctx.alias}."ocel_id" AND oo."ocel_source_id" = {candidate_alias}."ocel_id")
          )
        WHERE {" AND ".join(predicates)}
    """


def _render_has_event_subquery(
    spec: RelationSpec,
    ctx: CompileContext,
    *,
    select_sql: str,
    extra_predicates: list[str],
) -> str:
    if ctx.kind != "object":
        raise ValueError("has_event(...) is only valid in object-rooted scope")

    candidate_alias = "he"
    candidate_ctx = ctx.with_alias(candidate_alias, kind="event")
    predicates = [
        f'eo."ocel_object_id" = {ctx.alias}."ocel_id"',
        f'{candidate_alias}."ocel_type" = {render_compare_value(spec.target_type, ctx)}',
    ]
    predicates.extend(render_expr(expr, candidate_ctx) for expr in spec.filters)
    predicates.extend(extra_predicates)

    return f"""
        SELECT {select_sql}
        FROM {ctx.table("event_object")} eo
        JOIN {ctx.table("event")} {candidate_alias}
          ON eo."ocel_event_id" = {candidate_alias}."ocel_id"
        WHERE {" AND ".join(predicates)}
    """


def relation_candidate_kind(kind: str) -> str:
    match kind:
        case "related" | "linked":
            return "object"
        case "has_event":
            return "event"
        case _:
            raise TypeError(f"Unsupported relation kind: {kind!r}")


def relation_candidate_alias(kind: str) -> str:
    match kind:
        case "related":
            return "ro"
        case "linked":
            return "lo"
        case "has_event":
            return "he"
        case _:
            raise TypeError(f"Unsupported relation kind: {kind!r}")


def quote_ident(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'
