from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import assert_never

from oceldb.ast.aggregation import AvgAgg, CountAgg, CountDistinctAgg, MaxAgg, MinAgg, SumAgg
from oceldb.ast.base import (
    AliasExpr,
    AndExpr,
    BinaryOpExpr,
    CaseExpr,
    CastExpr,
    CompareExpr,
    CompareValue,
    Expr,
    ExprVisitor,
    FunctionExpr,
    InExpr,
    LiteralExpr,
    NotExpr,
    PredicateFunctionExpr,
    SortExpr,
    UnaryPredicate,
    WindowFunctionExpr,
    OrExpr,
)
from oceldb.ast.field import ColumnExpr
from oceldb.ast.relation import (
    LinkedDirection,
    RelationAllExpr,
    RelationCountExpr,
    RelationExistsExpr,
    RelationKind,
    RelationSpec,
)
from oceldb.sql.context import (
    CompileContext,
    ExprScopeKind,
    render_object_state_source,
    render_object_state_source_at_event,
)


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
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


class SQLRenderVisitor(ExprVisitor[str]):
    def __init__(self, ctx: CompileContext) -> None:
        self.ctx = ctx

    def visit_column(self, expr: ColumnExpr) -> str:
        return f"{self.ctx.alias}.{quote_ident(expr.name)}"

    def visit_alias(self, expr: AliasExpr) -> str:
        return f"{render_expr(expr.expr, self.ctx)} AS {quote_ident(expr.name)}"

    def visit_literal(self, expr: LiteralExpr) -> str:
        return render_compare_value(expr.value, self.ctx)

    def visit_cast(self, expr: CastExpr) -> str:
        return f"TRY_CAST({render_expr(expr.expr, self.ctx)} AS {expr.sql_type})"

    def visit_binary_op(self, expr: BinaryOpExpr) -> str:
        left = _render_scalar_value(expr.left, self.ctx)
        right = _render_scalar_value(expr.right, self.ctx)
        return f"({left} {expr.op} {right})"

    def visit_scalar_function(self, expr: FunctionExpr) -> str:
        return _render_scalar_function(expr, self.ctx)

    def visit_predicate_function(self, expr: PredicateFunctionExpr) -> str:
        return _render_predicate_function(expr, self.ctx)

    def visit_case(self, expr: CaseExpr) -> str:
        when_sql = " ".join(
            f"WHEN {render_expr(condition, self.ctx)} THEN {_render_scalar_value(value, self.ctx)}"
            for condition, value in expr.branches
        )
        else_sql = _render_scalar_value(expr.default, self.ctx)
        return f"(CASE {when_sql} ELSE {else_sql} END)"

    def visit_window_function(self, expr: WindowFunctionExpr) -> str:
        if expr.window is None:
            raise ValueError(
                f"Window function {expr.name}(...) requires .over(...) before it can be rendered"
            )

        partition_sql = ""
        if expr.window.partition_by:
            partition_sql = "PARTITION BY " + ", ".join(
                render_expr(value, self.ctx)
                for value in expr.window.partition_by
            )

        order_sql = "ORDER BY " + ", ".join(
            render_order_expr(value, self.ctx)
            for value in expr.window.order_by
        )

        clauses = [clause for clause in (partition_sql, order_sql) if clause]
        return f"{_render_window_function_call(expr, self.ctx)} OVER ({' '.join(clauses)})"

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
        candidate_kind = relation_candidate_kind(expr.spec.kind, self.ctx.kind)
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
        case "cooccurs_with":
            return _render_cooccurs_with_subquery(
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
        case "has_object":
            return _render_has_object_subquery(
                spec,
                ctx,
                select_sql=select_sql,
                extra_predicates=extra_predicates or [],
            )
    assert_never(spec.kind)


def _render_cooccurs_with_subquery(
    spec: RelationSpec,
    ctx: CompileContext,
    *,
    select_sql: str,
    extra_predicates: list[str],
) -> str:
    if ctx.kind not in {"object", "object_state"}:
        raise ValueError("cooccurs_with(...) is only valid in object-rooted scope")

    candidate_alias = "ro"
    candidate_kind = relation_candidate_kind(spec.kind, ctx.kind)
    candidate_ctx = ctx.with_alias(candidate_alias, kind=candidate_kind)
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
        JOIN {render_scope_source(candidate_ctx)}
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
    if ctx.kind not in {"object", "object_state"}:
        raise ValueError("linked(...) is only valid in object-rooted scope")

    candidate_alias = "lo"
    candidate_kind = relation_candidate_kind(spec.kind, ctx.kind)
    candidate_ctx = ctx.with_alias(candidate_alias, kind=candidate_kind)
    predicates = [
        f'{candidate_alias}."ocel_type" = {render_compare_value(spec.target_type, ctx)}',
    ]
    predicates.extend(render_expr(expr, candidate_ctx) for expr in spec.filters)
    predicates.extend(extra_predicates)

    if spec.linked_max_hops is None:
        return _render_unbounded_linked_subquery(
            spec,
            ctx,
            candidate_alias=candidate_alias,
            candidate_ctx=candidate_ctx,
            select_sql=select_sql,
            predicates=predicates,
        )

    anchor_node = f'{ctx.alias}."ocel_id"'
    anchor_next = _linked_neighbor_sql(spec.linked_direction, anchor_node)
    anchor_match = _linked_match_sql(spec.linked_direction, anchor_node)

    recursive_node = 'lp."ocel_id"'
    recursive_next = _linked_neighbor_sql(spec.linked_direction, recursive_node)
    recursive_match = _linked_match_sql(spec.linked_direction, recursive_node)
    hop_limit_sql = f'AND lp."depth" < {spec.linked_max_hops}'

    return f"""
        WITH RECURSIVE linked_paths("ocel_id", "depth", "path") AS (
            SELECT
                {anchor_next} AS "ocel_id",
                1 AS "depth",
                ',' || {anchor_node} || ',' || {anchor_next} || ',' AS "path"
            FROM {ctx.table("object_object")} oo
            WHERE {anchor_match}
              AND {anchor_next} <> {anchor_node}

            UNION ALL

            SELECT
                {recursive_next} AS "ocel_id",
                lp."depth" + 1 AS "depth",
                lp."path" || {recursive_next} || ',' AS "path"
            FROM linked_paths lp
            JOIN {ctx.table("object_object")} oo
              ON {recursive_match}
            WHERE POSITION(',' || {recursive_next} || ',' IN lp."path") = 0
              {hop_limit_sql}
        )
        SELECT {select_sql}
        FROM (
            SELECT DISTINCT lp."ocel_id"
            FROM linked_paths lp
        ) linked_ids
        JOIN {render_scope_source(candidate_ctx)}
          ON linked_ids."ocel_id" = {candidate_alias}."ocel_id"
        WHERE {" AND ".join(predicates)}
    """


def _render_unbounded_linked_subquery(
    spec: RelationSpec,
    ctx: CompileContext,
    *,
    candidate_alias: str,
    candidate_ctx: CompileContext,
    select_sql: str,
    predicates: list[str],
) -> str:
    anchor_node = f'{ctx.alias}."ocel_id"'
    linked_edges_sql = _linked_edges_sql(spec.linked_direction, ctx.table("object_object"))

    return f"""
        WITH RECURSIVE
        linked_edges("source_id", "target_id") AS (
            {linked_edges_sql}
        ),
        linked_nodes("ocel_id") AS (
            SELECT le."target_id"
            FROM linked_edges le
            WHERE le."source_id" = {anchor_node}
              AND le."target_id" <> {anchor_node}

            UNION

            SELECT le."target_id"
            FROM linked_nodes ln
            JOIN linked_edges le
              ON le."source_id" = ln."ocel_id"
            WHERE le."target_id" <> {anchor_node}
        )
        SELECT {select_sql}
        FROM linked_nodes linked_ids
        JOIN {render_scope_source(candidate_ctx)}
          ON linked_ids."ocel_id" = {candidate_alias}."ocel_id"
        WHERE {" AND ".join(predicates)}
    """


def _render_has_event_subquery(
    spec: RelationSpec,
    ctx: CompileContext,
    *,
    select_sql: str,
    extra_predicates: list[str],
) -> str:
    if ctx.kind not in {"object", "object_state"}:
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


def _render_has_object_subquery(
    spec: RelationSpec,
    ctx: CompileContext,
    *,
    select_sql: str,
    extra_predicates: list[str],
) -> str:
    if ctx.kind != "event":
        raise ValueError("has_object(...) is only valid in event-rooted scope")

    candidate_alias = "ho"
    candidate_ctx = ctx.with_alias(
        candidate_alias,
        kind="object_state_at_event",
        event_alias=ctx.alias,
    )
    predicates = [
        f'eo."ocel_event_id" = {ctx.alias}."ocel_id"',
        f'{candidate_alias}."ocel_type" = {render_compare_value(spec.target_type, ctx)}',
    ]
    predicates.extend(render_expr(expr, candidate_ctx) for expr in spec.filters)
    predicates.extend(extra_predicates)

    return f"""
        SELECT {select_sql}
        FROM {ctx.table("event_object")} eo
        JOIN {render_scope_source(candidate_ctx)} ON eo."ocel_object_id" = {candidate_alias}."ocel_id"
        WHERE {" AND ".join(predicates)}
    """


def relation_candidate_kind(kind: RelationKind, current_kind: ExprScopeKind) -> ExprScopeKind:
    match kind:
        case "cooccurs_with" | "linked":
            return "object_state" if current_kind == "object_state" else "object"
        case "has_event":
            return "event"
        case "has_object":
            return "object_state_at_event"
    assert_never(kind)


def relation_candidate_alias(kind: RelationKind) -> str:
    match kind:
        case "cooccurs_with":
            return "ro"
        case "linked":
            return "lo"
        case "has_event":
            return "he"
        case "has_object":
            return "ho"
    assert_never(kind)


def _linked_match_sql(direction: LinkedDirection, node_sql: str) -> str:
    match direction:
        case "bidirectional":
            return f'(oo."ocel_source_id" = {node_sql} OR oo."ocel_target_id" = {node_sql})'
        case "outgoing":
            return f'oo."ocel_source_id" = {node_sql}'
        case "incoming":
            return f'oo."ocel_target_id" = {node_sql}'
    assert_never(direction)


def _linked_neighbor_sql(direction: LinkedDirection, node_sql: str) -> str:
    match direction:
        case "bidirectional":
            return (
                f'CASE WHEN oo."ocel_source_id" = {node_sql} '
                f'THEN oo."ocel_target_id" ELSE oo."ocel_source_id" END'
            )
        case "outgoing":
            return 'oo."ocel_target_id"'
        case "incoming":
            return 'oo."ocel_source_id"'
    assert_never(direction)


def _linked_edges_sql(direction: LinkedDirection, object_object_sql: str) -> str:
    match direction:
        case "bidirectional":
            return f"""
                SELECT
                    oo."ocel_source_id" AS "source_id",
                    oo."ocel_target_id" AS "target_id"
                FROM {object_object_sql} oo
                UNION
                SELECT
                    oo."ocel_target_id" AS "source_id",
                    oo."ocel_source_id" AS "target_id"
                FROM {object_object_sql} oo
            """
        case "outgoing":
            return f"""
                SELECT
                    oo."ocel_source_id" AS "source_id",
                    oo."ocel_target_id" AS "target_id"
                FROM {object_object_sql} oo
            """
        case "incoming":
            return f"""
                SELECT
                    oo."ocel_target_id" AS "source_id",
                    oo."ocel_source_id" AS "target_id"
                FROM {object_object_sql} oo
            """
    assert_never(direction)


def render_scope_source(ctx: CompileContext) -> str:
    match ctx.kind:
        case "event":
            return f'{ctx.table("event")} {ctx.alias}'
        case "object":
            return f'{ctx.table("object")} {ctx.alias}'
        case "object_state":
            return (
                f'({render_object_state_source(
                    ctx.object_change_columns,
                    mode=ctx.object_state_mode or "latest",
                    as_of=ctx.object_state_as_of,
                )}) {ctx.alias}'
            )
        case "object_state_at_event":
            if ctx.event_alias is None:
                raise ValueError("object_state_at_event scope requires an event alias")
            return (
                f'LATERAL ({render_object_state_source_at_event(
                    ctx.object_change_columns,
                    event_alias=ctx.event_alias,
                )}) {ctx.alias}'
            )
        case "object_change":
            return f'{ctx.table("object_change")} {ctx.alias}'
        case _:
            raise TypeError(f"Unsupported scope kind: {ctx.kind!r}")


def quote_ident(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _render_scalar_value(value: Expr | CompareValue, ctx: CompileContext) -> str:
    if isinstance(value, Expr):
        return render_expr(value, ctx)
    return render_compare_value(value, ctx)


def _render_scalar_function(expr: FunctionExpr, ctx: CompileContext) -> str:
    rendered_args = [
        _render_scalar_value(value, ctx)
        for value in expr.args
    ]

    match expr.name:
        case "coalesce":
            return f"COALESCE({', '.join(rendered_args)})"
        case "lower":
            return f"LOWER({rendered_args[0]})"
        case "upper":
            return f"UPPER({rendered_args[0]})"
        case "abs":
            return f"ABS({rendered_args[0]})"
        case "round":
            return f"ROUND({rendered_args[0]}, {rendered_args[1]})"
        case "year":
            return f"EXTRACT(YEAR FROM {rendered_args[0]})"
        case "month":
            return f"EXTRACT(MONTH FROM {rendered_args[0]})"
        case "day":
            return f"EXTRACT(DAY FROM {rendered_args[0]})"
        case "date":
            return f"CAST({rendered_args[0]} AS DATE)"
        case _:
            raise TypeError(f"Unsupported scalar function: {expr.name!r}")


def _render_window_function_call(
    expr: WindowFunctionExpr,
    ctx: CompileContext,
) -> str:
    rendered_args = [
        _render_scalar_value(value, ctx)
        for value in expr.args
    ]

    match expr.name:
        case "lag" | "lead":
            if not rendered_args:
                raise ValueError(f"{expr.name}(...) requires an expression argument")
            pieces = [rendered_args[0]]
            if expr.offset is not None:
                pieces.append(str(expr.offset))
            if expr.default is not None:
                pieces.append(_render_scalar_value(expr.default, ctx))
            return f"{expr.name.upper()}({', '.join(pieces)})"
        case "row_number":
            return "ROW_NUMBER()"
        case _:
            raise TypeError(f"Unsupported window function: {expr.name!r}")


def _render_predicate_function(
    expr: PredicateFunctionExpr,
    ctx: CompileContext,
) -> str:
    rendered_args = [
        _render_scalar_value(value, ctx)
        for value in expr.args
    ]

    match expr.name:
        case "contains":
            value_sql, needle_sql = rendered_args
            return f"(POSITION({needle_sql} IN {value_sql}) > 0)"
        case "starts_with":
            value_sql, prefix_sql = rendered_args
            return f"(LEFT({value_sql}, LENGTH({prefix_sql})) = {prefix_sql})"
        case "ends_with":
            value_sql, suffix_sql = rendered_args
            return f"(RIGHT({value_sql}, LENGTH({suffix_sql})) = {suffix_sql})"
        case _:
            raise TypeError(f"Unsupported predicate function: {expr.name!r}")
