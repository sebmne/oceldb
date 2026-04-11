"""Tests for the DSL factory functions."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest

from oceldb.dsl import (
    asc,
    attr,
    avg,
    changed_field,
    count,
    count_distinct,
    desc,
    event_id,
    field,
    has_event,
    id_,
    in_,
    linked,
    max_,
    min_,
    object_id,
    related,
    source_id,
    sum_,
    target_id,
    time_,
    type_,
)
from oceldb.ast.aggregation import (
    AvgAgg,
    CountAgg,
    CountDistinctAgg,
    MaxAgg,
    MinAgg,
    SumAgg,
)
from oceldb.ast.attribute import AttrExpr
from oceldb.ast.base import OrderExpr
from oceldb.ast.field import FieldExpr
from oceldb.ast.function import InExpr
from oceldb.ast.relation import (
    RelationAllExpr,
    RelationCountExpr,
    RelationExistsExpr,
)
from oceldb.dsl._utils import python_type_to_sql_type
from oceldb.dsl.relations import RelationBuilder


# ── Field factories ──────────────────────────────────────────────────────


class TestFieldFactories:
    def test_field_no_cast(self):
        f = field("ocel_id")
        assert isinstance(f, FieldExpr)
        assert f.name == "ocel_id"
        assert f.cast is None

    def test_field_with_cast(self):
        f = field("ocel_time", cast=datetime)
        assert f.cast == "TIMESTAMP"

    def test_id_(self):
        assert id_().name == "ocel_id"

    def test_type_(self):
        assert type_().name == "ocel_type"

    def test_time_(self):
        assert time_().name == "ocel_time"

    def test_changed_field(self):
        assert changed_field().name == "ocel_changed_field"

    def test_event_id(self):
        assert event_id().name == "ocel_event_id"

    def test_object_id(self):
        assert object_id().name == "ocel_object_id"

    def test_source_id(self):
        assert source_id().name == "ocel_source_id"

    def test_target_id(self):
        assert target_id().name == "ocel_target_id"


# ── Attribute factories ──────────────────────────────────────────────────


class TestAttrFactory:
    def test_basic(self):
        a = attr("price")
        assert isinstance(a, AttrExpr)
        assert a.name == "price"
        assert a.cast is None

    def test_with_cast(self):
        a = attr("price", cast=float)
        assert a.cast == "DOUBLE"

    def test_empty_name_raises(self):
        with pytest.raises(ValueError, match="must not be empty"):
            attr("")


# ── Type conversion ──────────────────────────────────────────────────────


class TestTypeConversion:
    @pytest.mark.parametrize(
        "py_type, sql_type",
        [
            (int, "BIGINT"),
            (float, "DOUBLE"),
            (str, "VARCHAR"),
            (bool, "BOOLEAN"),
            (datetime, "TIMESTAMP"),
            (date, "DATE"),
            (Decimal, "DOUBLE"),
        ],
    )
    def test_known_types(self, py_type, sql_type):
        assert python_type_to_sql_type(py_type) == sql_type

    def test_none_returns_none(self):
        assert python_type_to_sql_type(None) is None

    def test_unsupported_type_raises(self):
        with pytest.raises(TypeError, match="Unsupported cast type"):
            python_type_to_sql_type(list)  # type: ignore[arg-type]


# ── Aggregate factories ─────────────────────────────────────────────────


class TestAggregateFactories:
    def test_count(self):
        assert isinstance(count(), CountAgg)

    def test_count_distinct(self):
        result = count_distinct(type_())
        assert isinstance(result, CountDistinctAgg)

    def test_min(self):
        assert isinstance(min_(time_()), MinAgg)

    def test_max(self):
        assert isinstance(max_(time_()), MaxAgg)

    def test_sum(self):
        assert isinstance(sum_(attr("price", cast=float)), SumAgg)

    def test_avg(self):
        assert isinstance(avg(attr("price", cast=float)), AvgAgg)


# ── IN predicate ─────────────────────────────────────────────────────────


class TestInPredicate:
    def test_basic(self):
        result = in_(type_(), ["Order", "Invoice"])
        assert isinstance(result, InExpr)
        assert result.values == ("Order", "Invoice")

    def test_single_value(self):
        result = in_(type_(), ["Order"])
        assert len(result.values) == 1

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="at least one value"):
            in_(type_(), [])

    def test_accepts_generator(self):
        result = in_(type_(), (x for x in ["A", "B"]))
        assert result.values == ("A", "B")


# ── Relation builders ───────────────────────────────────────────────────


class TestRelationBuilders:
    def test_related_creates_builder(self):
        b = related("customer")
        assert isinstance(b, RelationBuilder)
        assert b.kind == "related"
        assert b.target_type == "customer"
        assert b.filters == ()

    def test_linked_creates_builder(self):
        b = linked("order")
        assert isinstance(b, RelationBuilder)
        assert b.kind == "linked"

    def test_has_event_creates_builder(self):
        b = has_event("Pay Order")
        assert isinstance(b, RelationBuilder)
        assert b.kind == "has_event"
        assert b.target_type == "Pay Order"

    def test_where_adds_filters(self):
        f = type_() == "customer"
        b = related("customer").where(f)
        assert len(b.filters) == 1

    def test_where_is_immutable(self):
        b1 = related("customer")
        b2 = b1.where(type_() == "customer")
        assert b1.filters == ()
        assert len(b2.filters) == 1

    def test_where_empty_returns_self(self):
        b = related("customer")
        assert b.where() is b

    def test_where_chains(self):
        f1 = type_() == "customer"
        f2 = id_() == "o3"
        b = related("customer").where(f1).where(f2)
        assert len(b.filters) == 2

    def test_exists(self):
        result = related("customer").exists()
        assert isinstance(result, RelationExistsExpr)

    def test_count(self):
        result = related("customer").count()
        assert isinstance(result, RelationCountExpr)

    def test_any(self):
        cond = id_() == "o3"
        result = related("customer").any(cond)
        assert isinstance(result, RelationExistsExpr)
        assert len(result.spec.filters) == 1

    def test_all(self):
        cond = attr("name").not_null()
        result = related("customer").all(cond)
        assert isinstance(result, RelationAllExpr)
        assert result.condition is cond


# ── Sorting ──────────────────────────────────────────────────────────────


class TestSorting:
    def test_asc(self):
        result = asc(time_())
        assert isinstance(result, OrderExpr)
        assert result.direction == "ASC"

    def test_desc(self):
        result = desc(time_())
        assert isinstance(result, OrderExpr)
        assert result.direction == "DESC"

    def test_asc_with_string(self):
        result = asc("my_alias")
        assert result.expr == "my_alias"
        assert result.direction == "ASC"
