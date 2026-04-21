"""Expression-to-SQL rendering.

The visitor translates an ``Expr`` tree into a SQL fragment against the active
``CompileContext``. Relation predicates (``has_event``/``has_object``/
``cooccurs_with``/``linked``) delegate to :mod:`oceldb.compile.relations`; this
module builds the candidate context, renders the inner predicate against it,
and hands the rendered SQL back so the subquery can be assembled.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from oceldb.compile.context import CompileContext, quote_ident
from oceldb.compile.relations import candidate_ctx_for, render_relation_subquery
from oceldb.expr.nodes import (
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
    Literal_,
    MaxAgg,
    MinAgg,
    NotExpr,
    PredicateFunctionExpr,
    RelationAllExpr,
    RelationCountExpr,
    RelationExistsExpr,
    SortExpr,
    SumAgg,
    WindowFunctionExpr,
)


def render_expr(expr: Expr, ctx: CompileContext) -> str:
    return SQLRenderVisitor(ctx).visit(expr)


def render_order_expr(sort: SortExpr, ctx: CompileContext) -> str:
    direction = "DESC" if sort.descending else "ASC"
    inner = sort.expr
    if isinstance(inner, ColumnExpr):
        return f"{quote_ident(inner.name)} {direction}"
    return f"{render_expr(inner, ctx)} {direction}"


def render_compare_value(value: Any, ctx: CompileContext) -> str:
    if isinstance(value, Expr):
        return render_expr(value, ctx)
    return _render_literal(value)


class SQLRenderVisitor(ExprVisitor[str]):
    def __init__(self, ctx: CompileContext) -> None:
        self.ctx = ctx

    # --- leaves -----------------------------------------------------------

    def visit_ColumnExpr(self, expr: ColumnExpr) -> str:
        return f"{self.ctx.alias}.{quote_ident(expr.name)}"

    def visit_LiteralExpr(self, expr: LiteralExpr) -> str:
        return _render_literal(expr.value)

    # --- decorators -------------------------------------------------------

    def visit_AliasExpr(self, expr: AliasExpr) -> str:
        return f"{self.visit(expr.expr)} AS {quote_ident(expr.name)}"

    def visit_CastExpr(self, expr: CastExpr) -> str:
        return f"TRY_CAST({self.visit(expr.expr)} AS {expr.sql_type})"

    # --- scalar operators -------------------------------------------------

    def visit_BinaryOpExpr(self, expr: BinaryOpExpr) -> str:
        left = self.visit(expr.left)
        right = self.visit(expr.right)
        return f"({left} {expr.op} {right})"

    def visit_CompareExpr(self, expr: CompareExpr) -> str:
        left = self.visit(expr.left)
        right_expr = expr.right
        if (
            isinstance(right_expr, LiteralExpr)
            and right_expr.value is None
            and expr.op in {"=", "!="}
        ):
            op = "IS NULL" if expr.op == "=" else "IS NOT NULL"
            return f"({left} {op})"
        right = self.visit(right_expr)
        return f"({left} {expr.op} {right})"

    def visit_BoolOpExpr(self, expr: BoolOpExpr) -> str:
        joiner = f" {expr.op} "
        rendered = joiner.join(self.visit(operand) for operand in expr.operands)
        return f"({rendered})"

    def visit_NotExpr(self, expr: NotExpr) -> str:
        return f"(NOT {self.visit(expr.operand)})"

    def visit_InExpr(self, expr: InExpr) -> str:
        values_sql = ", ".join(_render_literal(v) for v in expr.values)
        return f"({self.visit(expr.expr)} IN ({values_sql}))"

    # --- functions --------------------------------------------------------

    def visit_FunctionExpr(self, expr: FunctionExpr) -> str:
        return _render_scalar_function(expr, self)

    def visit_PredicateFunctionExpr(self, expr: PredicateFunctionExpr) -> str:
        return _render_predicate_function(expr, self)

    def visit_CaseExpr(self, expr: CaseExpr) -> str:
        if not expr.branches:
            raise ValueError("CASE expression must have at least one WHEN branch")
        branches_sql = " ".join(
            f"WHEN {self.visit(condition)} THEN {self.visit(value)}"
            for condition, value in expr.branches
        )
        else_sql = self.visit(expr.default) if expr.default is not None else "NULL"
        return f"(CASE {branches_sql} ELSE {else_sql} END)"

    # --- aggregates -------------------------------------------------------

    def visit_CountAgg(self, expr: CountAgg) -> str:
        if expr.expr is None:
            return "COUNT(*)"
        inner = self.visit(expr.expr)
        if expr.distinct:
            return f"COUNT(DISTINCT {inner})"
        return f"COUNT({inner})"

    def visit_SumAgg(self, expr: SumAgg) -> str:
        return f"SUM({self.visit(expr.expr)})"

    def visit_AvgAgg(self, expr: AvgAgg) -> str:
        return f"AVG({self.visit(expr.expr)})"

    def visit_MinAgg(self, expr: MinAgg) -> str:
        return f"MIN({self.visit(expr.expr)})"

    def visit_MaxAgg(self, expr: MaxAgg) -> str:
        return f"MAX({self.visit(expr.expr)})"

    # --- windows ----------------------------------------------------------

    def visit_WindowFunctionExpr(self, expr: WindowFunctionExpr) -> str:
        call_sql = _render_window_function_call(expr, self)
        clauses: list[str] = []
        if expr.partition_by:
            partition_sql = ", ".join(self.visit(p) for p in expr.partition_by)
            clauses.append(f"PARTITION BY {partition_sql}")
        if expr.order_by:
            order_sql = ", ".join(
                f"{self.visit(o.expr)} {'DESC' if o.descending else 'ASC'}"
                for o in expr.order_by
            )
            clauses.append(f"ORDER BY {order_sql}")
        if not clauses:
            return f"{call_sql} OVER ()"
        return f"{call_sql} OVER ({' '.join(clauses)})"

    # --- relations --------------------------------------------------------

    def visit_RelationExistsExpr(self, expr: RelationExistsExpr) -> str:
        candidate_ctx = candidate_ctx_for(expr.target, self.ctx)
        predicate_sql = self._render_relation_predicate(expr.predicate, candidate_ctx)
        subquery = render_relation_subquery(
            expr.target,
            self.ctx,
            candidate_ctx,
            select_sql="1",
            candidate_predicate_sql=predicate_sql,
        )
        return f"EXISTS ({subquery})"

    def visit_RelationCountExpr(self, expr: RelationCountExpr) -> str:
        candidate_ctx = candidate_ctx_for(expr.target, self.ctx)
        predicate_sql = self._render_relation_predicate(expr.predicate, candidate_ctx)
        subquery = render_relation_subquery(
            expr.target,
            self.ctx,
            candidate_ctx,
            select_sql="COUNT(*)",
            candidate_predicate_sql=predicate_sql,
        )
        return f"({subquery})"

    def visit_RelationAllExpr(self, expr: RelationAllExpr) -> str:
        candidate_ctx = candidate_ctx_for(expr.target, self.ctx)
        predicate_sql = render_expr(expr.predicate, candidate_ctx)
        subquery = render_relation_subquery(
            expr.target,
            self.ctx,
            candidate_ctx,
            select_sql="1",
            candidate_predicate_sql=predicate_sql,
            negate_candidate_predicate=True,
        )
        return f"NOT EXISTS ({subquery})"

    def _render_relation_predicate(
        self,
        predicate: Expr | None,
        candidate_ctx: CompileContext,
    ) -> str | None:
        if predicate is None:
            return None
        return render_expr(predicate, candidate_ctx)

    # --- sort (shouldn't be visited directly, but safe fallback) ----------

    def visit_SortExpr(self, expr: SortExpr) -> str:
        return render_order_expr(expr, self.ctx)


# ---------------------------------------------------------------------------
# Scalar / predicate / window function dispatch
# ---------------------------------------------------------------------------


def _render_scalar_function(expr: FunctionExpr, visitor: SQLRenderVisitor) -> str:
    rendered = [visitor.visit(arg) for arg in expr.args]
    name = expr.name.upper()
    if name == "COALESCE":
        return f"COALESCE({', '.join(rendered)})"
    if name == "ABS":
        return f"ABS({rendered[0]})"
    if name == "ROUND":
        return f"ROUND({rendered[0]}, {rendered[1]})"
    if name == "LOWER":
        return f"LOWER({rendered[0]})"
    if name == "UPPER":
        return f"UPPER({rendered[0]})"
    if name == "EXTRACT":
        if not expr.extra:
            raise ValueError("EXTRACT(...) requires an extract-field extra")
        field = expr.extra[0]
        return f"EXTRACT({field} FROM {rendered[0]})"
    raise TypeError(f"Unsupported scalar function: {expr.name!r}")


def _render_predicate_function(
    expr: PredicateFunctionExpr,
    visitor: SQLRenderVisitor,
) -> str:
    rendered = [visitor.visit(arg) for arg in expr.args]
    name = expr.name.upper()
    if name == "IS_NULL":
        return f"({rendered[0]} IS NULL)"
    if name == "IS_NOT_NULL":
        return f"({rendered[0]} IS NOT NULL)"
    if name == "STR_CONTAINS":
        value_sql, needle_sql = rendered
        return f"(POSITION({needle_sql} IN {value_sql}) > 0)"
    if name == "STARTS_WITH":
        value_sql, prefix_sql = rendered
        return f"(LEFT({value_sql}, LENGTH({prefix_sql})) = {prefix_sql})"
    if name == "ENDS_WITH":
        value_sql, suffix_sql = rendered
        return f"(RIGHT({value_sql}, LENGTH({suffix_sql})) = {suffix_sql})"
    raise TypeError(f"Unsupported predicate function: {expr.name!r}")


def _render_window_function_call(
    expr: WindowFunctionExpr,
    visitor: SQLRenderVisitor,
) -> str:
    rendered = [visitor.visit(arg) for arg in expr.args]
    name = expr.name.upper()
    if name in {"LEAD", "LAG"}:
        if not rendered:
            raise ValueError(f"{name}(...) requires at least one argument")
        return f"{name}({', '.join(rendered)})"
    if name == "ROW_NUMBER":
        return "ROW_NUMBER()"
    if name in {"RANK", "DENSE_RANK"}:
        return f"{name}()"
    raise TypeError(f"Unsupported window function: {expr.name!r}")


# ---------------------------------------------------------------------------
# Literal rendering
# ---------------------------------------------------------------------------


def _render_literal(value: Literal_ | Any) -> str:
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
    raise TypeError(f"Cannot render value as SQL literal: {value!r}")


__all__ = [
    "SQLRenderVisitor",
    "render_compare_value",
    "render_expr",
    "render_order_expr",
]
