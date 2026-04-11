"""Unit tests for AST node construction and operator overloading."""

from __future__ import annotations

import pytest

from oceldb.ast.base import (
    AliasedExpr,
    AndExpr,
    CompareExpr,
    NotExpr,
    OrderExpr,
    OrExpr,
    UnaryPredicate,
)
from oceldb.ast.field import FieldExpr
from oceldb.ast.attribute import AttrExpr
from oceldb.ast.aggregation import (
    AvgAgg,
    CountAgg,
    CountDistinctAgg,
    MaxAgg,
    MinAgg,
    SumAgg,
)
from oceldb.ast.function import InExpr
from oceldb.ast.relation import (
    RelationAllExpr,
    RelationCountExpr,
    RelationExistsExpr,
    RelationSpec,
)


class TestFieldExpr:
    def test_basic(self):
        expr = FieldExpr(name="ocel_id")
        assert expr.name == "ocel_id"
        assert expr.cast is None

    def test_with_cast(self):
        expr = FieldExpr(name="ocel_time", cast="TIMESTAMP")
        assert expr.cast == "TIMESTAMP"

    def test_frozen(self):
        expr = FieldExpr(name="ocel_id")
        with pytest.raises(AttributeError):
            expr.name = "other"  # type: ignore[misc]


class TestAttrExpr:
    def test_basic(self):
        expr = AttrExpr(name="price")
        assert expr.name == "price"
        assert expr.cast is None

    def test_with_cast(self):
        expr = AttrExpr(name="price", cast="DOUBLE")
        assert expr.cast == "DOUBLE"


class TestComparisonOperators:
    def test_eq(self):
        expr = FieldExpr("ocel_type") == "Order"
        assert isinstance(expr, CompareExpr)
        assert expr.op == "="
        assert expr.right == "Order"

    def test_ne(self):
        expr = FieldExpr("ocel_type") != "Order"
        assert isinstance(expr, CompareExpr)
        assert expr.op == "!="

    def test_gt(self):
        expr = FieldExpr("ocel_time") > "2022-01-01"
        assert expr.op == ">"

    def test_ge(self):
        expr = FieldExpr("ocel_time") >= "2022-01-01"
        assert expr.op == ">="

    def test_lt(self):
        expr = FieldExpr("ocel_time") < "2022-01-01"
        assert expr.op == "<"

    def test_le(self):
        expr = FieldExpr("ocel_time") <= "2022-01-01"
        assert expr.op == "<="

    def test_compare_with_none(self):
        expr = FieldExpr("ocel_id") == None  # noqa: E711
        assert expr.right is None

    def test_compare_with_int(self):
        expr = AttrExpr("price", cast="BIGINT") > 100
        assert expr.right == 100

    def test_compare_with_float(self):
        expr = AttrExpr("price", cast="DOUBLE") > 99.9
        assert expr.right == 99.9

    def test_compare_with_bool(self):
        expr = AttrExpr("active", cast="BOOLEAN") == True  # noqa: E712
        assert expr.right is True

    def test_compare_expr_to_expr(self):
        left = FieldExpr("ocel_id")
        right = FieldExpr("ocel_type")
        expr = left == right
        assert isinstance(expr.right, FieldExpr)


class TestNullChecks:
    def test_is_null(self):
        expr = FieldExpr("ocel_changed_field").is_null()
        assert isinstance(expr, UnaryPredicate)
        assert expr.op == "IS NULL"

    def test_not_null(self):
        expr = FieldExpr("ocel_changed_field").not_null()
        assert isinstance(expr, UnaryPredicate)
        assert expr.op == "IS NOT NULL"


class TestBooleanOperators:
    def test_and(self):
        a = FieldExpr("ocel_type") == "Order"
        b = FieldExpr("ocel_id") == "o1"
        result = a & b
        assert isinstance(result, AndExpr)
        assert result.left is a
        assert result.right is b

    def test_or(self):
        a = FieldExpr("ocel_type") == "Order"
        b = FieldExpr("ocel_type") == "Invoice"
        result = a | b
        assert isinstance(result, OrExpr)

    def test_not(self):
        a = FieldExpr("ocel_type") == "Order"
        result = ~a
        assert isinstance(result, NotExpr)
        assert result.expr is a

    def test_complex_boolean(self):
        a = FieldExpr("ocel_type") == "Order"
        b = FieldExpr("ocel_id") == "o1"
        c = FieldExpr("ocel_id") == "o2"
        result = a & (b | c)
        assert isinstance(result, AndExpr)
        assert isinstance(result.right, OrExpr)


class TestAliasing:
    def test_as_(self):
        expr = FieldExpr("ocel_type").as_("my_type")
        assert isinstance(expr, AliasedExpr)
        assert expr.alias == "my_type"
        assert isinstance(expr.expr, FieldExpr)

    def test_empty_alias_raises(self):
        with pytest.raises(ValueError, match="must not be empty"):
            FieldExpr("ocel_type").as_("")


class TestOrderExpr:
    def test_creation(self):
        expr = OrderExpr(expr=FieldExpr("ocel_time"), direction="ASC")
        assert expr.direction == "ASC"

    def test_string_expr(self):
        expr = OrderExpr(expr="my_alias", direction="DESC")
        assert expr.expr == "my_alias"


class TestAggregateNodes:
    def test_count(self):
        assert isinstance(CountAgg(), CountAgg)

    def test_count_distinct(self):
        f = FieldExpr("ocel_type")
        agg = CountDistinctAgg(f)
        assert agg.expr is f

    def test_min(self):
        agg = MinAgg(FieldExpr("ocel_time"))
        assert isinstance(agg.expr, FieldExpr)

    def test_max(self):
        agg = MaxAgg(FieldExpr("ocel_time"))
        assert isinstance(agg.expr, FieldExpr)

    def test_sum(self):
        agg = SumAgg(AttrExpr("price", cast="DOUBLE"))
        assert isinstance(agg.expr, AttrExpr)

    def test_avg(self):
        agg = AvgAgg(AttrExpr("price", cast="DOUBLE"))
        assert isinstance(agg.expr, AttrExpr)


class TestInExpr:
    def test_basic(self):
        expr = InExpr(expr=FieldExpr("ocel_type"), values=("Order", "Invoice"))
        assert expr.values == ("Order", "Invoice")

    def test_single_value(self):
        expr = InExpr(expr=FieldExpr("ocel_type"), values=("Order",))
        assert len(expr.values) == 1


class TestRelationSpec:
    def test_related(self):
        spec = RelationSpec(kind="related", target_type="customer")
        assert spec.kind == "related"
        assert spec.filters == ()

    def test_linked(self):
        spec = RelationSpec(kind="linked", target_type="order")
        assert spec.kind == "linked"

    def test_has_event(self):
        spec = RelationSpec(kind="has_event", target_type="Pay Order")
        assert spec.kind == "has_event"

    def test_with_filters(self):
        f = FieldExpr("ocel_type") == "x"
        spec = RelationSpec(kind="related", target_type="t", filters=(f,))
        assert len(spec.filters) == 1

    def test_relation_exists(self):
        spec = RelationSpec(kind="related", target_type="customer")
        expr = RelationExistsExpr(spec)
        assert isinstance(expr, RelationExistsExpr)

    def test_relation_count(self):
        spec = RelationSpec(kind="related", target_type="customer")
        expr = RelationCountExpr(spec)
        assert isinstance(expr, RelationCountExpr)

    def test_relation_all(self):
        spec = RelationSpec(kind="related", target_type="customer")
        cond = FieldExpr("ocel_type") == "customer"
        expr = RelationAllExpr(spec, cond)
        assert isinstance(expr, RelationAllExpr)
        assert expr.condition is cond
