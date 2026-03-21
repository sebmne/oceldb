"""Tests for the expression system (expr module)."""

from __future__ import annotations

import pytest

from oceldb.expr import BinOp, Domain, Literal, SetOp, UnaOp, event, obj

# -- Alias resolution ---------------------------------------------------------


class TestNamespaceAliases:
    def test_type_alias(self):
        assert event.type.ocel_column == "ocel_type"

    def test_id_alias(self):
        assert event.id.ocel_column == "ocel_id"

    def test_time_alias(self):
        assert event.time.ocel_column == "ocel_time"

    def test_passthrough(self):
        assert event.amount.ocel_column == "amount"

    def test_object_namespace(self):
        assert obj.type.ocel_column == "ocel_type"

    def test_event_domain(self):
        assert event.type.domain == Domain.EVENT

    def test_object_domain(self):
        assert obj.type.domain == Domain.OBJECT


# -- Comparison operators -----------------------------------------------------


class TestComparisons:
    def test_eq(self):
        expr = event.type == "Order"
        assert isinstance(expr, BinOp)
        assert expr.to_sql() == "(ocel_type = 'Order')"

    def test_ne(self):
        expr = event.type != "Order"
        assert expr.to_sql() == "(ocel_type != 'Order')"

    def test_gt(self):
        expr = event.time > "2022-01-01"
        assert expr.to_sql() == "(ocel_time > '2022-01-01')"

    def test_ge(self):
        expr = event.time >= "2022-01-01"
        assert expr.to_sql() == "(ocel_time >= '2022-01-01')"

    def test_lt(self):
        expr = event.time < "2022-12-31"
        assert expr.to_sql() == "(ocel_time < '2022-12-31')"

    def test_le(self):
        expr = event.time <= "2022-12-31"
        assert expr.to_sql() == "(ocel_time <= '2022-12-31')"

    def test_numeric_literal(self):
        expr = event.amount > 100
        assert expr.to_sql() == "(amount > 100)"

    def test_float_literal(self):
        expr = event.amount >= 99.5
        assert expr.to_sql() == "(amount >= 99.5)"


# -- Predicates ---------------------------------------------------------------


class TestPredicates:
    def test_is_in(self):
        expr = event.type.is_in("A", "B", "C")
        assert isinstance(expr, SetOp)
        assert expr.to_sql() == "(ocel_type IN ('A', 'B', 'C'))"

    def test_not_in(self):
        expr = event.type.not_in("X")
        assert expr.to_sql() == "(ocel_type NOT IN ('X'))"

    def test_is_null(self):
        expr = event.amount.is_null()
        assert expr.to_sql() == "(amount IS NULL)"

    def test_not_null(self):
        expr = event.amount.not_null()
        assert expr.to_sql() == "(amount IS NOT NULL)"

    def test_is_like(self):
        expr = event.type.is_like("%Order%")
        assert expr.to_sql() == "(ocel_type LIKE '%Order%')"

    def test_not_like(self):
        expr = event.type.not_like("%Test%")
        assert expr.to_sql() == "(ocel_type NOT LIKE '%Test%')"

    def test_is_between(self):
        expr = event.amount.is_between(10, 100)
        assert expr.to_sql() == "((amount >= 10) AND (amount <= 100))"

    def test_not_between(self):
        expr = event.amount.not_between(10, 100)
        assert expr.to_sql() == "(NOT ((amount >= 10) AND (amount <= 100)))"


# -- Logical combinators ------------------------------------------------------


class TestLogical:
    def test_and(self):
        expr = (event.type == "A") & (event.time > "2022-01-01")
        assert expr.to_sql() == "((ocel_type = 'A') AND (ocel_time > '2022-01-01'))"

    def test_or(self):
        expr = (event.type == "A") | (event.type == "B")
        assert expr.to_sql() == "((ocel_type = 'A') OR (ocel_type = 'B'))"

    def test_not(self):
        expr = ~(event.type == "A")
        assert isinstance(expr, UnaOp)
        assert expr.to_sql() == "(NOT (ocel_type = 'A'))"

    def test_complex_combination(self):
        expr = ((event.type == "A") & (event.time > "2022-01-01")) | ~(
            event.amount.is_null()
        )
        sql = expr.to_sql()
        assert "AND" in sql
        assert "OR" in sql
        assert "NOT" in sql


# -- Literal SQL rendering ----------------------------------------------------


class TestLiteral:
    def test_string(self):
        assert Literal("hello").to_sql() == "'hello'"

    def test_string_with_quote(self):
        assert Literal("it's").to_sql() == "'it''s'"

    def test_int(self):
        assert Literal(42).to_sql() == "42"

    def test_float(self):
        assert Literal(3.14).to_sql() == "3.14"

    def test_bool_true(self):
        assert Literal(True).to_sql() == "TRUE"

    def test_bool_false(self):
        assert Literal(False).to_sql() == "FALSE"

    def test_none(self):
        assert Literal(None).to_sql() == "NULL"


# -- columns() traversal ------------------------------------------------------


class TestColumns:
    def test_attribute_columns(self):
        attr = event.type
        cols = attr.columns()
        assert len(cols) == 1
        assert next(iter(cols)).ocel_column == "ocel_type"

    def test_binop_columns(self):
        expr = (event.type == "A") & (event.time > "2022-01-01")
        cols = expr.columns()
        col_names = {c.ocel_column for c in cols}
        assert col_names == {"ocel_type", "ocel_time"}

    def test_unaop_columns(self):
        expr = ~(event.type == "A")
        cols = expr.columns()
        assert len(cols) == 1

    def test_setop_columns(self):
        expr = event.type.is_in("A", "B")
        cols = expr.columns()
        assert len(cols) == 1

    def test_literal_no_columns(self):
        assert Literal(42).columns() == frozenset()


# -- Domain classification (ViewBuilder._classify_by_domain) ------------------


class TestDomainClassification:
    def test_event_domain(self):
        from oceldb.ocel import ViewBuilder

        class FakeOcel:
            pass

        vb = ViewBuilder.__new__(ViewBuilder)
        vb._ocel = FakeOcel()
        vb._filters = [event.type == "A", event.time > "2022-01-01"]
        ev, ob = vb._classify_by_domain()
        assert len(ev) == 2
        assert len(ob) == 0

    def test_object_domain(self):
        from oceldb.ocel import ViewBuilder

        class FakeOcel:
            pass

        vb = ViewBuilder.__new__(ViewBuilder)
        vb._ocel = FakeOcel()
        vb._filters = [obj.type == "order"]
        ev, ob = vb._classify_by_domain()
        assert len(ev) == 0
        assert len(ob) == 1

    def test_mixed_domain_raises(self):
        from oceldb.ocel import ViewBuilder

        class FakeOcel:
            pass

        vb = ViewBuilder.__new__(ViewBuilder)
        vb._ocel = FakeOcel()
        # Build a single expression referencing both domains
        mixed = BinOp("AND", event.type == "A", obj.type == "order")
        vb._filters = [mixed]
        with pytest.raises(ValueError, match="cannot mix"):
            vb._classify_by_domain()
