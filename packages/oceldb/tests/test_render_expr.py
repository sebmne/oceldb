"""Tests for SQL rendering of individual expressions."""

from __future__ import annotations

import pytest

from oceldb.ast.attribute import AttrExpr
from oceldb.ast.base import AliasedExpr, CompareExpr, OrderExpr
from oceldb.ast.field import FieldExpr
from oceldb.dsl import (
    attr,
    avg,
    count,
    count_distinct,
    field,
    has_event,
    id_,
    in_,
    linked,
    max_,
    min_,
    related,
    sum_,
    time_,
    type_,
)
from oceldb.sql.context import CompileContext
from oceldb.sql.render_expr import (
    render_bool_expr,
    render_compare_value,
    render_expr,
    render_order_expr,
    quote_ident,
)


# ── Helpers ──────────────────────────────────────────────────────────────


@pytest.fixture()
def ctx():
    return CompileContext(alias="o", schema="s", kind="object")


@pytest.fixture()
def ev_ctx():
    return CompileContext(alias="e", schema="s", kind="event")


# ── quote_ident ──────────────────────────────────────────────────────────


class TestQuoteIdent:
    def test_simple(self):
        assert quote_ident("ocel_id") == '"ocel_id"'

    def test_with_double_quote(self):
        assert quote_ident('col"name') == '"col""name"'


# ── Field rendering ─────────────────────────────────────────────────────


class TestRenderFieldExpr:
    def test_basic(self, ctx):
        assert render_expr(FieldExpr("ocel_id"), ctx) == 'o."ocel_id"'

    def test_with_cast(self, ctx):
        expr = FieldExpr("ocel_time", cast="TIMESTAMP")
        assert render_expr(expr, ctx) == 'TRY_CAST(o."ocel_time" AS TIMESTAMP)'

    def test_different_alias(self):
        ctx = CompileContext(alias="ev", schema="s", kind="event")
        assert render_expr(FieldExpr("ocel_id"), ctx) == 'ev."ocel_id"'


# ── Attribute rendering ─────────────────────────────────────────────────


class TestRenderAttrExpr:
    def test_basic(self, ctx):
        result = render_expr(AttrExpr("price"), ctx)
        assert result == "o.attributes->>'price'"

    def test_with_cast(self, ctx):
        result = render_expr(AttrExpr("price", cast="DOUBLE"), ctx)
        assert result == "TRY_CAST(o.attributes->>'price' AS DOUBLE)"

    def test_escapes_single_quotes(self, ctx):
        result = render_expr(AttrExpr("it's"), ctx)
        assert result == "o.attributes->>'it''s'"


# ── Compare values ───────────────────────────────────────────────────────


class TestRenderCompareValue:
    def test_none(self, ctx):
        assert render_compare_value(None, ctx) == "NULL"

    def test_true(self, ctx):
        assert render_compare_value(True, ctx) == "TRUE"

    def test_false(self, ctx):
        assert render_compare_value(False, ctx) == "FALSE"

    def test_int(self, ctx):
        assert render_compare_value(42, ctx) == "42"

    def test_negative_int(self, ctx):
        assert render_compare_value(-7, ctx) == "-7"

    def test_float(self, ctx):
        assert render_compare_value(3.14, ctx) == "3.14"

    def test_string(self, ctx):
        assert render_compare_value("hello", ctx) == "'hello'"

    def test_string_escapes_quotes(self, ctx):
        assert render_compare_value("it's", ctx) == "'it''s'"

    def test_expr(self, ctx):
        inner = FieldExpr("ocel_type")
        result = render_compare_value(inner, ctx)
        assert result == 'o."ocel_type"'

    def test_unsupported_raises(self, ctx):
        with pytest.raises(TypeError, match="Unsupported comparison value"):
            render_compare_value([1, 2, 3], ctx)  # type: ignore[arg-type]


# ── Comparison expressions ───────────────────────────────────────────────


class TestRenderCompareExpr:
    def test_eq_string(self, ctx):
        expr = type_() == "Order"
        assert render_expr(expr, ctx) == "(o.\"ocel_type\" = 'Order')"

    def test_ne(self, ctx):
        expr = type_() != "Order"
        assert render_expr(expr, ctx) == "(o.\"ocel_type\" != 'Order')"

    def test_gt_int(self, ctx):
        expr = attr("price", cast=int) > 100
        result = render_expr(expr, ctx)
        assert result == "(TRY_CAST(o.attributes->>'price' AS BIGINT) > 100)"

    def test_eq_none(self, ctx):
        expr = id_() == None  # noqa: E711
        assert render_expr(expr, ctx) == '(o."ocel_id" = NULL)'

    def test_expr_to_expr(self, ctx):
        expr = FieldExpr("ocel_id") == FieldExpr("ocel_type")
        result = render_expr(expr, ctx)
        assert result == '(o."ocel_id" = o."ocel_type")'


# ── Unary predicates ─────────────────────────────────────────────────────


class TestRenderUnaryPredicate:
    def test_is_null(self, ctx):
        expr = field("ocel_changed_field").is_null()
        assert render_expr(expr, ctx) == '(o."ocel_changed_field" IS NULL)'

    def test_not_null(self, ctx):
        expr = field("ocel_changed_field").not_null()
        assert render_expr(expr, ctx) == '(o."ocel_changed_field" IS NOT NULL)'


# ── Boolean operators ────────────────────────────────────────────────────


class TestRenderBooleanOps:
    def test_and(self, ctx):
        expr = (type_() == "Order") & (id_() == "o1")
        result = render_expr(expr, ctx)
        assert result == "((o.\"ocel_type\" = 'Order') AND (o.\"ocel_id\" = 'o1'))"

    def test_or(self, ctx):
        expr = (type_() == "Order") | (type_() == "Invoice")
        result = render_expr(expr, ctx)
        assert result == "((o.\"ocel_type\" = 'Order') OR (o.\"ocel_type\" = 'Invoice'))"

    def test_not(self, ctx):
        expr = ~(type_() == "Order")
        result = render_expr(expr, ctx)
        assert result == "(NOT (o.\"ocel_type\" = 'Order'))"

    def test_nested(self, ctx):
        expr = (type_() == "Order") & ((id_() == "o1") | (id_() == "o2"))
        result = render_expr(expr, ctx)
        assert "AND" in result
        assert "OR" in result


# ── Aggregates ───────────────────────────────────────────────────────────


class TestRenderAggregates:
    def test_count(self, ctx):
        assert render_expr(count(), ctx) == "COUNT(*)"

    def test_count_distinct(self, ctx):
        result = render_expr(count_distinct(type_()), ctx)
        assert result == 'COUNT(DISTINCT o."ocel_type")'

    def test_min(self, ctx):
        result = render_expr(min_(time_()), ctx)
        assert result == 'MIN(o."ocel_time")'

    def test_max(self, ctx):
        result = render_expr(max_(time_()), ctx)
        assert result == 'MAX(o."ocel_time")'

    def test_sum(self, ctx):
        result = render_expr(sum_(attr("price", cast=float)), ctx)
        assert result == "SUM(TRY_CAST(o.attributes->>'price' AS DOUBLE))"

    def test_avg(self, ctx):
        result = render_expr(avg(attr("price", cast=float)), ctx)
        assert result == "AVG(TRY_CAST(o.attributes->>'price' AS DOUBLE))"


# ── IN predicate ─────────────────────────────────────────────────────────


class TestRenderIn:
    def test_basic(self, ctx):
        expr = in_(type_(), ["Order", "Invoice"])
        result = render_expr(expr, ctx)
        assert result == "(o.\"ocel_type\" IN ('Order', 'Invoice'))"

    def test_single_value(self, ctx):
        expr = in_(type_(), ["Order"])
        result = render_expr(expr, ctx)
        assert result == "(o.\"ocel_type\" IN ('Order'))"

    def test_int_values(self, ctx):
        expr = in_(attr("count", cast=int), [1, 2, 3])
        result = render_expr(expr, ctx)
        assert "IN (1, 2, 3)" in result


# ── Aliased expression ───────────────────────────────────────────────────


class TestRenderAliased:
    def test_basic(self, ctx):
        expr = type_().as_("my_type")
        result = render_expr(expr, ctx)
        assert result == 'o."ocel_type" AS "my_type"'

    def test_aggregate_aliased(self, ctx):
        expr = count().as_("total")
        result = render_expr(expr, ctx)
        assert result == 'COUNT(*) AS "total"'


# ── Order expression ────────────────────────────────────────────────────


class TestRenderOrderExpr:
    def test_asc(self, ctx):
        expr = OrderExpr(expr=FieldExpr("ocel_time"), direction="ASC")
        result = render_order_expr(expr, ctx)
        assert result == 'o."ocel_time" ASC'

    def test_desc_string(self, ctx):
        expr = OrderExpr(expr="my_alias", direction="DESC")
        result = render_order_expr(expr, ctx)
        assert result == '"my_alias" DESC'


# ── Relation subqueries ─────────────────────────────────────────────────


class TestRenderRelations:
    def test_related_exists(self, ctx):
        expr = related("customer").exists()
        result = render_expr(expr, ctx)
        assert result.startswith("EXISTS (")
        assert "event_object" in result
        assert "ro.ocel_type = 'customer'" in result

    def test_related_count(self, ctx):
        expr = related("customer").count()
        result = render_expr(expr, ctx)
        assert "COUNT(*)" in result
        assert "event_object" in result

    def test_related_with_filter(self, ctx):
        expr = related("customer").where(attr("name") == "Alice").exists()
        result = render_expr(expr, ctx)
        assert "Alice" in result

    def test_linked_exists(self, ctx):
        expr = linked("customer").exists()
        result = render_expr(expr, ctx)
        assert "EXISTS" in result
        assert "object_object" in result
        assert "lo.ocel_type = 'customer'" in result

    def test_linked_bidirectional(self, ctx):
        expr = linked("customer").exists()
        result = render_expr(expr, ctx)
        assert "ocel_source_id" in result
        assert "ocel_target_id" in result
        assert "OR" in result

    def test_has_event_exists(self, ctx):
        expr = has_event("Pay Order").exists()
        result = render_expr(expr, ctx)
        assert "EXISTS" in result
        assert "he.ocel_type = 'Pay Order'" in result

    def test_has_event_count(self, ctx):
        expr = has_event("Pay Order").count()
        result = render_expr(expr, ctx)
        assert "COUNT(*)" in result

    def test_relation_all(self, ctx):
        expr = related("customer").all(attr("name").not_null())
        result = render_expr(expr, ctx)
        assert "NOT EXISTS" in result
        assert "IS NOT NULL" in result

    def test_related_invalid_scope(self):
        ctx = CompileContext(alias="e", schema="s", kind="event")
        expr = related("customer").exists()
        with pytest.raises(ValueError, match="object-rooted scope"):
            render_expr(expr, ctx)

    def test_linked_invalid_scope(self):
        ctx = CompileContext(alias="e", schema="s", kind="event")
        expr = linked("customer").exists()
        with pytest.raises(ValueError, match="object-rooted scope"):
            render_expr(expr, ctx)

    def test_has_event_invalid_scope(self):
        ctx = CompileContext(alias="e", schema="s", kind="event")
        expr = has_event("Pay Order").exists()
        with pytest.raises(ValueError, match="object-rooted scope"):
            render_expr(expr, ctx)


# ── Unsupported expression type ──────────────────────────────────────────


class TestUnsupportedExpr:
    def test_raises(self, ctx):
        with pytest.raises(TypeError, match="Unsupported expression type"):
            render_expr("not an expr", ctx)  # type: ignore[arg-type]
