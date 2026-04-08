from __future__ import annotations

from oceldb.ast.aggregation import (
    AvgAgg,
    CountAgg,
    CountDistinctAgg,
    MaxAgg,
    MinAgg,
    SumAgg,
)
from oceldb.ast.attribute import AttrExpr
from oceldb.ast.base import (
    AliasedExpr,
    AndExpr,
    BoolExpr,
    CompareExpr,
    CompareValue,
    Expr,
    NotExpr,
    OrderExpr,
    OrExpr,
    ScalarExpr,
    UnaryPredicate,
)
from oceldb.ast.field import FieldExpr
from oceldb.ast.function import InExpr
from oceldb.ast.relation import (
    RelationAllExpr,
    RelationCountExpr,
    RelationExistsExpr,
    RelationSpec,
)
from oceldb.compiler.context import CompileContext


def render_expr(expr: Expr, ctx: CompileContext) -> str:
    """
    Render a generic DSL expression into SQL.
    """
    match expr:
        case AliasedExpr(expr=inner, alias=alias):
            return f"{render_expr(inner, ctx)} AS {quote_ident(alias)}"

        case FieldExpr():
            return render_field_expr(expr, ctx)

        case AttrExpr():
            return render_attr_expr(expr, ctx)

        case CompareExpr():
            return render_compare_expr(expr, ctx)

        case UnaryPredicate(expr=inner, op=op):
            return f"({render_scalar_expr(inner, ctx)} {op})"

        case AndExpr(left=left, right=right):
            return f"({render_bool_expr(left, ctx)} AND {render_bool_expr(right, ctx)})"

        case OrExpr(left=left, right=right):
            return f"({render_bool_expr(left, ctx)} OR {render_bool_expr(right, ctx)})"

        case NotExpr(expr=inner):
            return f"(NOT {render_bool_expr(inner, ctx)})"

        case CountAgg():
            return "COUNT(*)"

        case CountDistinctAgg(expr=inner):
            return f"COUNT(DISTINCT {render_expr(inner, ctx)})"

        case MinAgg(expr=inner):
            return f"MIN({render_expr(inner, ctx)})"

        case MaxAgg(expr=inner):
            return f"MAX({render_expr(inner, ctx)})"

        case SumAgg(expr=inner):
            return f"SUM({render_expr(inner, ctx)})"

        case AvgAgg(expr=inner):
            return f"AVG({render_expr(inner, ctx)})"

        case InExpr(expr=inner, values=values):
            rendered_values = ", ".join(
                render_compare_value(value, ctx) for value in values
            )
            return f"({render_scalar_expr(inner, ctx)} IN ({rendered_values}))"

        case RelationExistsExpr(spec=spec):
            return render_relation_exists(spec, ctx)

        case RelationCountExpr(spec=spec):
            return render_relation_count(spec, ctx)

        case RelationAllExpr(spec=spec, condition=condition):
            return render_relation_all(spec, condition, ctx)

        case _:
            raise TypeError(f"Unsupported expression type: {type(expr)!r}")


def render_scalar_expr(expr: ScalarExpr, ctx: CompileContext) -> str:
    """
    Render a scalar expression into SQL.
    """
    return render_expr(expr, ctx)


def render_bool_expr(expr: BoolExpr, ctx: CompileContext) -> str:
    """
    Render a boolean expression into SQL.
    """
    return render_expr(expr, ctx)


def render_order_expr(expr: OrderExpr, ctx: CompileContext) -> str:
    """
    Render an ordering specification into SQL.
    """
    match expr.expr:
        case str() as name:
            return f"{quote_ident(name)} {expr.direction}"
        case _:
            return f"{render_expr(expr.expr, ctx)} {expr.direction}"


def render_field_expr(expr: FieldExpr, ctx: CompileContext) -> str:
    """
    Render a fixed field access into SQL.
    """
    base = f"{ctx.alias}.{quote_ident(expr.name)}"
    if expr.cast is not None:
        return f"TRY_CAST({base} AS {expr.cast})"
    return base


def render_attr_expr(expr: AttrExpr, ctx: CompileContext) -> str:
    """
    Render a dynamic JSON attribute access into SQL.
    """
    escaped = expr.name.replace("'", "''")
    base = f"{ctx.alias}.attributes->>'{escaped}'"
    if expr.cast is not None:
        return f"TRY_CAST({base} AS {expr.cast})"
    return base


def render_compare_expr(expr: CompareExpr, ctx: CompileContext) -> str:
    """
    Render a scalar comparison into SQL.
    """
    left = render_scalar_expr(expr.left, ctx)
    right = render_compare_value(expr.right, ctx)
    return f"({left} {expr.op} {right})"


def render_compare_value(value: CompareValue, ctx: CompileContext) -> str:
    """
    Render a comparison RHS value into SQL.
    """
    match value:
        case Expr():
            return render_expr(value, ctx)

        case None:
            return "NULL"

        case bool() as b:
            return "TRUE" if b else "FALSE"

        case int() | float():
            return str(value)

        case str() as s:
            escaped = s.replace("'", "''")
            return f"'{escaped}'"

        case _:
            raise TypeError(f"Unsupported comparison value: {value!r}")


def render_relation_exists(spec: RelationSpec, ctx: CompileContext) -> str:
    subquery = render_relation_subquery(spec, ctx, select_sql="1")
    return f"EXISTS ({subquery})"


def render_relation_count(spec: RelationSpec, ctx: CompileContext) -> str:
    subquery = render_relation_subquery(spec, ctx, select_sql="COUNT(*)")
    return f"({subquery})"


def render_relation_all(
    spec: RelationSpec, condition: BoolExpr, ctx: CompileContext
) -> str:
    candidate_alias = relation_candidate_alias(spec.kind)

    base_subquery = render_relation_subquery(
        spec,
        ctx,
        select_sql="1",
        extra_predicates=[
            f"NOT ({render_bool_expr(condition, ctx.with_alias(candidate_alias, kind=relation_candidate_kind(spec.kind)))})"  # type: ignore
        ],
    )
    return f"NOT EXISTS ({base_subquery})"


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


def render_relation_subquery(
    spec: RelationSpec,
    ctx: CompileContext,
    *,
    select_sql: str,
    extra_predicates: list[str] | None = None,
) -> str:
    match spec.kind:
        case "related":
            return render_related_subquery(
                spec,
                ctx,
                select_sql=select_sql,
                extra_predicates=extra_predicates or [],
            )
        case "linked":
            return render_linked_subquery(
                spec,
                ctx,
                select_sql=select_sql,
                extra_predicates=extra_predicates or [],
            )
        case "has_event":
            return render_has_event_subquery(
                spec,
                ctx,
                select_sql=select_sql,
                extra_predicates=extra_predicates or [],
            )
        case _:
            raise TypeError(f"Unsupported relation kind: {spec.kind!r}")


def render_related_subquery(
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
        f"eo1.ocel_object_id = {ctx.alias}.ocel_id",
        "eo1.ocel_event_id = eo2.ocel_event_id",
        "eo2.ocel_object_id = ro.ocel_id",
        f"{candidate_alias}.ocel_type = {render_compare_value(spec.target_type, ctx)}",
    ]

    for expr in spec.filters:
        predicates.append(render_bool_expr(expr, candidate_ctx))

    predicates.extend(extra_predicates)

    where_sql = " AND ".join(predicates)

    return f"""
        SELECT {select_sql}
        FROM {ctx.table("event_object")} eo1
        JOIN {ctx.table("event_object")} eo2
          ON eo1.ocel_event_id = eo2.ocel_event_id
        JOIN {ctx.table("object")} {candidate_alias}
          ON eo2.ocel_object_id = {candidate_alias}.ocel_id
        WHERE {where_sql}
    """


def render_linked_subquery(
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
        f"{candidate_alias}.ocel_type = {render_compare_value(spec.target_type, ctx)}",
    ]

    for expr in spec.filters:
        predicates.append(render_bool_expr(expr, candidate_ctx))

    predicates.extend(extra_predicates)

    where_sql = " AND ".join(predicates)

    return f"""
        SELECT {select_sql}
        FROM {ctx.table("object_object")} oo
        JOIN {ctx.table("object")} {candidate_alias}
          ON (
               (oo.ocel_source_id = {ctx.alias}.ocel_id AND oo.ocel_target_id = {candidate_alias}.ocel_id)
            OR (oo.ocel_target_id = {ctx.alias}.ocel_id AND oo.ocel_source_id = {candidate_alias}.ocel_id)
          )
        WHERE {where_sql}
    """


def render_has_event_subquery(
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
        f"eo.ocel_object_id = {ctx.alias}.ocel_id",
        f"{candidate_alias}.ocel_type = {render_compare_value(spec.target_type, ctx)}",
    ]

    for expr in spec.filters:
        predicates.append(render_bool_expr(expr, candidate_ctx))

    predicates.extend(extra_predicates)

    where_sql = " AND ".join(predicates)

    return f"""
        SELECT {select_sql}
        FROM {ctx.table("event_object")} eo
        JOIN {ctx.table("event")} {candidate_alias}
          ON eo.ocel_event_id = {candidate_alias}.ocel_id
        WHERE {where_sql}
    """


def quote_ident(name: str) -> str:
    """
    Quote a SQL identifier safely for DuckDB.
    """
    escaped = name.replace('"', '""')
    return f'"{escaped}"'
