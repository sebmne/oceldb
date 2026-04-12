from __future__ import annotations

from oceldb.ast.aggregation import CountAgg
from oceldb.ast.base import AliasExpr, CastExpr, CompareExpr, LiteralExpr, SortExpr
from oceldb.ast.field import ColumnExpr
from oceldb.ast.relation import RelationAllExpr, RelationCountExpr, RelationExistsExpr
from oceldb.dsl import col, count, desc, has_event, lit, related
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


def test_count_builder():
    expr = count()
    assert isinstance(expr, CountAgg)


def test_in_helper():
    expr = col("ocel_type").is_in(["A", "B"])
    assert expr.values == ("A", "B")
    assert "A" in expr.values


def test_relation_builders():
    assert isinstance(related("customer").exists(), RelationExistsExpr)
    assert isinstance(related("customer").count(), RelationCountExpr)
    assert isinstance(has_event("Pay Order").all(col("method").not_null()), RelationAllExpr)


def test_render_column_expr(ctx):
    assert render_expr(col("ocel_id"), ctx) == 'o."ocel_id"'


def test_render_compare_value_datetime(ctx):
    rendered = render_compare_value("hello", ctx)
    assert rendered == "'hello'"


def test_render_relation_exists(ctx):
    rendered = render_expr(related("customer").exists(), ctx)
    assert "EXISTS" in rendered
    assert "event_object" in rendered


def test_render_order_expr(ctx):
    order = desc("n")
    assert isinstance(order, SortExpr)
    assert render_order_expr(order, ctx) == '"n" DESC'
