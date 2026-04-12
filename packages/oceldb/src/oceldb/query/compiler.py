from __future__ import annotations

from dataclasses import dataclass

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
    SortExpr,
    UnaryPredicate,
    OrExpr,
)
from oceldb.ast.field import ColumnExpr
from oceldb.ast.relation import RelationAllExpr, RelationCountExpr, RelationExistsExpr, RelationSpec
from oceldb.query.lazy_query import (
    FilterOp,
    GroupByAggOp,
    LazyOCELQuery,
    LimitOp,
    SelectOp,
    SortOp,
    UniqueOp,
    WithColumnsOp,
)
from oceldb.sql.context import CompileContext
from oceldb.sql.render_expr import render_compare_value, render_expr, render_order_expr


def compile_query(query: LazyOCELQuery) -> str:
    _validate_query(query)

    sql = _render_source(query)
    current_kind = query.source_kind

    for index, op in enumerate(query.ops):
        alias = f"q{index}"
        ctx = CompileContext(alias=alias, schema=query.ocel.schema, kind=current_kind)

        match op:
            case FilterOp(predicates=predicates):
                where_sql = " AND ".join(render_expr(expr, ctx) for expr in predicates)
                sql = f"SELECT * FROM ({sql}) {alias} WHERE {where_sql}"

            case WithColumnsOp(exprs=exprs):
                additions = ", ".join(render_expr(expr, ctx) for expr in exprs)
                sql = f"SELECT {alias}.*, {additions} FROM ({sql}) {alias}"

            case SelectOp(exprs=exprs):
                select_sql = ", ".join(render_expr(expr, ctx) for expr in exprs)
                sql = f"SELECT {select_sql} FROM ({sql}) {alias}"

            case GroupByAggOp(groupings=groupings, aggregations=aggregations):
                select_parts = [
                    *(render_expr(expr, ctx) for expr in groupings),
                    *(render_expr(expr, ctx) for expr in aggregations),
                ]
                group_sql = ", ".join(render_expr(expr, ctx) for expr in groupings)
                sql = (
                    f"SELECT {', '.join(select_parts)} "
                    f"FROM ({sql}) {alias} "
                    f"GROUP BY {group_sql}"
                )

            case SortOp(orderings=orderings):
                order_sql = ", ".join(render_order_expr(expr, ctx) for expr in orderings)
                sql = f"SELECT * FROM ({sql}) {alias} ORDER BY {order_sql}"

            case UniqueOp():
                sql = f"SELECT DISTINCT * FROM ({sql}) {alias}"

            case LimitOp(n=n):
                sql = f"SELECT * FROM ({sql}) {alias} LIMIT {n}"

            case _:
                raise TypeError(f"Unsupported query operation: {type(op)!r}")

    return sql


def query_output_columns(query: LazyOCELQuery) -> dict[str, str]:
    _validate_query(query)

    columns = dict(query.ocel.available_columns(query.source_kind))

    if query.selected_types and "ocel_type" not in columns:
        raise ValueError(f"{query.source_kind!r} does not support type filtering")

    for index, op in enumerate(query.ops):
        has_following_ops = index < len(query.ops) - 1

        match op:
            case FilterOp():
                continue

            case WithColumnsOp(exprs=exprs):
                for expr in exprs:
                    columns[expr.alias] = "UNKNOWN"

            case SelectOp(exprs=exprs):
                columns = _derive_output_columns(
                    exprs,
                    has_following_ops=has_following_ops,
                )

            case GroupByAggOp(groupings=groupings, aggregations=aggregations):
                columns = _derive_output_columns(
                    (*groupings, *aggregations),
                    has_following_ops=has_following_ops,
                )

            case SortOp() | UniqueOp() | LimitOp():
                continue

            case _:
                raise TypeError(f"Unsupported query operation: {type(op)!r}")

    return columns


def _render_source(query: LazyOCELQuery) -> str:
    source_sql = f'SELECT * FROM "{query.ocel.schema}"."{query.source_kind}"'
    if not query.selected_types:
        return source_sql

    values_sql = ", ".join(
        render_compare_value(value, CompileContext(alias="root", schema=query.ocel.schema, kind=query.source_kind))
        for value in query.selected_types
    )
    return f"{source_sql} WHERE \"ocel_type\" IN ({values_sql})"


def _validate_query(query: LazyOCELQuery) -> None:
    columns = dict(query.ocel.available_columns(query.source_kind))
    current_kind = query.source_kind

    if query.selected_types and "ocel_type" not in columns:
        raise ValueError(f"{query.source_kind!r} does not support type filtering")

    for index, op in enumerate(query.ops):
        has_following_ops = index < len(query.ops) - 1

        match op:
            case FilterOp(predicates=predicates):
                for expr in predicates:
                    _validate_expr(expr, columns, current_kind, query)

            case WithColumnsOp(exprs=exprs):
                for expr in exprs:
                    _validate_expr(expr.expr, columns, current_kind, query)
                    columns[expr.alias] = "UNKNOWN"

            case SelectOp(exprs=exprs):
                for expr in exprs:
                    _validate_expr(expr, columns, current_kind, query)
                columns = _derive_output_columns(exprs, has_following_ops=has_following_ops)

            case GroupByAggOp(groupings=groupings, aggregations=aggregations):
                if not groupings:
                    raise ValueError("group_by(...).agg(...) requires at least one grouping")
                for expr in groupings:
                    _validate_expr(expr, columns, current_kind, query)
                for expr in aggregations:
                    _validate_expr(expr, columns, current_kind, query)
                columns = _derive_output_columns(
                    (*groupings, *aggregations),
                    has_following_ops=has_following_ops,
                )

            case SortOp(orderings=orderings):
                for ordering in orderings:
                    _validate_sort_expr(ordering, columns, current_kind, query)

            case UniqueOp() | LimitOp():
                continue

            case _:
                raise TypeError(f"Unsupported query operation: {type(op)!r}")


def _derive_output_columns(
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


def _validate_sort_expr(
    expr: SortExpr,
    columns: dict[str, str],
    current_kind: str,
    query: LazyOCELQuery,
) -> None:
    match expr.expr:
        case str() as name:
            if name not in columns:
                raise ValueError(f"Unknown sort column {name!r}")
        case _:
            _validate_expr(expr.expr, columns, current_kind, query)


def _validate_expr(
    expr: Expr,
    columns: dict[str, str],
    current_kind: str,
    query: LazyOCELQuery,
) -> None:
    ValidationVisitor(columns, current_kind, query).visit(expr)


def output_name(expr: Expr) -> str | None:
    match expr:
        case AliasExpr(alias=alias):
            return alias
        case ColumnExpr(name=name):
            return name
        case CastExpr(expr=inner):
            return output_name(inner)
        case CountAgg():
            return "count"
        case CountDistinctAgg(expr=inner):
            inner_name = output_name(inner)
            return "count_distinct" if inner_name is None else f"count_distinct_{inner_name}"
        case MinAgg(expr=inner):
            inner_name = output_name(inner)
            return None if inner_name is None else f"min_{inner_name}"
        case MaxAgg(expr=inner):
            inner_name = output_name(inner)
            return None if inner_name is None else f"max_{inner_name}"
        case SumAgg(expr=inner):
            inner_name = output_name(inner)
            return None if inner_name is None else f"sum_{inner_name}"
        case AvgAgg(expr=inner):
            inner_name = output_name(inner)
            return None if inner_name is None else f"avg_{inner_name}"
        case RelationCountExpr(spec=spec):
            return f"{spec.kind}_count"
        case _:
            return None


def _relation_target_kind(kind: str) -> str:
    match kind:
        case "related" | "linked":
            return "object"
        case "has_event":
            return "event"
        case _:
            raise TypeError(f"Unsupported relation kind: {kind!r}")


@dataclass
class ValidationVisitor(ExprVisitor[None]):
    columns: dict[str, str]
    current_kind: str
    query: LazyOCELQuery

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
        target_columns = self.query.ocel.available_columns(_relation_target_kind(expr.spec.kind))
        nested = ValidationVisitor(target_columns, _relation_target_kind(expr.spec.kind), self.query)
        nested.visit(expr.condition)

    def _validate_relation(self, spec: RelationSpec) -> None:
        if self.current_kind != "object":
            raise ValueError(f"{spec.kind}(...) is only valid in object-rooted scope")
        if "ocel_id" not in self.columns:
            raise ValueError(
                f"{spec.kind}(...) requires the current query scope to contain 'ocel_id'"
            )
        target_kind = _relation_target_kind(spec.kind)
        target_columns = self.query.ocel.available_columns(target_kind)
        nested = ValidationVisitor(target_columns, target_kind, self.query)
        for expr in spec.filters:
            nested.visit(expr)
