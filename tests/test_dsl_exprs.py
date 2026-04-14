from __future__ import annotations

import pytest

from oceldb.ast.aggregation import CountAgg
from oceldb.ast.base import (
    AliasExpr,
    BinaryOpExpr,
    CaseExpr,
    CastExpr,
    FunctionExpr,
    LiteralExpr,
    PredicateFunctionExpr,
    SortExpr,
    WindowFunctionExpr,
)
from oceldb.ast.field import ColumnExpr
from oceldb.ast.relation import RelationAllExpr, RelationCountExpr, RelationExistsExpr
from oceldb.dsl import (
    abs_,
    coalesce,
    cooccurs_with,
    col,
    count,
    desc,
    has_event,
    has_object,
    linked,
    lit,
    row_number,
    round_,
    when,
)
from oceldb.sql.render_expr import render_compare_value, render_expr, render_order_expr


def test_col_builder():
    expr = col("ocel_id")
    assert isinstance(expr, ColumnExpr)
    assert expr.name == "ocel_id"


def test_literal_builder():
    expr = lit(42)
    assert isinstance(expr, LiteralExpr)
    assert expr.value == 42


def test_alias_and_cast():
    expr = col("total_price").cast("DOUBLE").alias("price")
    assert isinstance(expr, AliasExpr)
    assert isinstance(expr.expr, CastExpr)


def test_arithmetic_builder():
    expr = col("total_price") * 2
    assert isinstance(expr, BinaryOpExpr)
    assert expr.op == "*"


def test_function_builders():
    assert isinstance(coalesce(col("name"), "unknown"), FunctionExpr)
    assert isinstance(col("name").fill_null("unknown"), FunctionExpr)
    assert isinstance(col("total_price").abs(), FunctionExpr)
    assert isinstance(col("total_price").round(2), FunctionExpr)
    assert isinstance(abs_(col("total_price")), FunctionExpr)
    assert isinstance(round_(col("total_price"), 1), FunctionExpr)
    assert isinstance(col("name").str.lower(), FunctionExpr)
    assert isinstance(col("ocel_time").dt.year(), FunctionExpr)
    assert isinstance(col("name").str.contains("Ali"), PredicateFunctionExpr)
    assert isinstance(
        col("ocel_event_type").lead().over(
            partition_by="ocel_object_id",
            order_by=("ocel_event_time", "ocel_event_id"),
        ),
        WindowFunctionExpr,
    )
    assert isinstance(
        row_number().over(
            partition_by="ocel_object_id",
            order_by=("ocel_event_time", "ocel_event_id"),
        ),
        WindowFunctionExpr,
    )
    assert isinstance(
        when(col("status") == "open").then("OPEN").otherwise("OTHER"),
        CaseExpr,
    )


def test_count_builder():
    expr = count()
    assert isinstance(expr, CountAgg)


def test_in_helper():
    expr = col("ocel_type").is_in(["A", "B"])
    assert expr.values == ("A", "B")
    assert "A" in expr.values


def test_expr_bool_is_rejected():
    with pytest.raises(TypeError, match="cannot be used as Python booleans"):
        bool(col("status") == "open")


def test_expr_if_is_rejected():
    with pytest.raises(TypeError, match="cannot be used as Python booleans"):
        if col("status") == "open":
            pytest.fail("expression should not be truthy")


def test_python_and_on_exprs_is_rejected():
    with pytest.raises(TypeError, match="cannot be used as Python booleans"):
        _ = (col("status") == "open") and (col("priority") == "high")


def test_relation_builders(ocel):
    assert isinstance(cooccurs_with("customer").exists(), RelationExistsExpr)
    assert isinstance(cooccurs_with("customer").count(), RelationCountExpr)
    assert isinstance(has_event("Pay Order").all(col("method").not_null()), RelationAllExpr)
    assert isinstance(has_object("order").exists(), RelationExistsExpr)
    assert isinstance(linked("customer").outgoing().exists(), RelationExistsExpr)
    assert isinstance(linked("customer").max_hops(3).count(), RelationCountExpr)
    assert isinstance(linked("customer").max_hops(None).exists(), RelationExistsExpr)


def test_render_column_expr(ctx):
    assert render_expr(col("ocel_id"), ctx) == 'o."ocel_id"'


def test_render_compare_value_datetime(ctx):
    rendered = render_compare_value("hello", ctx)
    assert rendered == "'hello'"


def test_render_relation_exists(ocel, ctx):
    rendered = render_expr(cooccurs_with("customer").exists(), ctx)
    assert "EXISTS" in rendered
    assert "event_object" in rendered


def test_render_arithmetic_expr(ctx):
    rendered = render_expr(col("total_price") * 2, ctx)
    assert rendered == '(o."total_price" * 2)'


def test_render_coalesce_expr(ctx):
    rendered = render_expr(coalesce(col("name"), "unknown"), ctx)
    assert rendered == 'COALESCE(o."name", \'unknown\')'


def test_render_string_predicate_expr(ctx):
    rendered = render_expr(col("name").str.contains("Ali"), ctx)
    assert "POSITION" in rendered
    assert "'Ali'" in rendered


def test_render_datetime_expr(ctx):
    rendered = render_expr(col("ocel_time").dt.year(), ctx)
    assert rendered == 'EXTRACT(YEAR FROM o."ocel_time")'


def test_render_window_expr(ctx):
    rendered = render_expr(
        col("ocel_event_type").lead().over(
            partition_by="ocel_object_id",
            order_by=("ocel_event_time", "ocel_event_id"),
        ),
        ctx,
    )
    assert (
        rendered
        == 'LEAD(o."ocel_event_type", 1) OVER (PARTITION BY o."ocel_object_id" ORDER BY o."ocel_event_time" ASC, o."ocel_event_id" ASC)'
    )


def test_render_numeric_function_expr(ctx):
    assert render_expr(col("total_price").abs(), ctx) == 'ABS(o."total_price")'
    assert render_expr(round_(col("total_price"), 1), ctx) == 'ROUND(o."total_price", 1)'


def test_render_case_expr(ctx):
    rendered = render_expr(
        when(col("status") == "open").then("OPEN").otherwise("OTHER"),
        ctx,
    )
    assert rendered == '(CASE WHEN (o."status" = \'open\') THEN \'OPEN\' ELSE \'OTHER\' END)'


def test_render_has_object_exists(event_ctx):
    rendered = render_expr(has_object("order").exists(), event_ctx)
    assert "EXISTS" in rendered
    assert "event_object" in rendered


def test_render_order_expr(ctx):
    order = desc("n")
    assert isinstance(order, SortExpr)
    assert render_order_expr(order, ctx) == '"n" DESC'
