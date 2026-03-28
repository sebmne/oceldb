"""Unit tests for the expression system — no database needed."""

from __future__ import annotations

import pytest

from oceldb import event, obj
from oceldb.expr import (
    And,
    Between,
    Col,
    Comparison,
    InSet,
    Not,
    Op,
    Or,
    Proxy,
)
from oceldb.types import Domain
from oceldb.utils import escape_string, sql_literal

# ---------------------------------------------------------------------------
# sql_literal / escape_string
# ---------------------------------------------------------------------------


class TestSqlLiteral:
    def test_string(self) -> None:
        assert sql_literal("hello") == "'hello'"

    def test_string_with_quotes(self) -> None:
        assert sql_literal("it's") == "'it''s'"

    def test_string_with_multiple_quotes(self) -> None:
        assert sql_literal("a''b") == "'a''''b'"

    def test_int(self) -> None:
        assert sql_literal(42) == "42"

    def test_negative_int(self) -> None:
        assert sql_literal(-7) == "-7"

    def test_float(self) -> None:
        assert sql_literal(3.14) == "3.14"

    def test_zero(self) -> None:
        assert sql_literal(0) == "0"

    def test_bool_true_raises(self) -> None:
        with pytest.raises(TypeError, match="Boolean"):
            sql_literal(True)

    def test_bool_false_raises(self) -> None:
        with pytest.raises(TypeError, match="Boolean"):
            sql_literal(False)

    def test_empty_string(self) -> None:
        assert sql_literal("") == "''"


class TestEscapeString:
    def test_no_quotes(self) -> None:
        assert escape_string("hello") == "hello"

    def test_single_quote(self) -> None:
        assert escape_string("it's") == "it''s"

    def test_empty(self) -> None:
        assert escape_string("") == ""


# ---------------------------------------------------------------------------
# Col
# ---------------------------------------------------------------------------


class TestCol:
    def test_properties(self) -> None:
        col = Col(Domain.EVENT, "ocel_type")
        assert col.domain is Domain.EVENT
        assert col.column == "ocel_type"

    def test_eq_produces_comparison(self) -> None:
        expr = Col(Domain.EVENT, "ocel_type") == "X"
        assert isinstance(expr, Comparison)

    def test_ne_produces_comparison(self) -> None:
        expr = Col(Domain.EVENT, "ocel_type") != "X"
        assert isinstance(expr, Comparison)

    def test_gt_produces_comparison(self) -> None:
        expr = Col(Domain.EVENT, "total_price") > 100
        assert isinstance(expr, Comparison)

    def test_ge_produces_comparison(self) -> None:
        expr = Col(Domain.EVENT, "total_price") >= 100
        assert isinstance(expr, Comparison)

    def test_lt_produces_comparison(self) -> None:
        expr = Col(Domain.EVENT, "total_price") < 100
        assert isinstance(expr, Comparison)

    def test_le_produces_comparison(self) -> None:
        expr = Col(Domain.EVENT, "total_price") <= 100
        assert isinstance(expr, Comparison)

    def test_is_in_produces_inset(self) -> None:
        expr = Col(Domain.EVENT, "ocel_type").is_in(["A", "B"])
        assert isinstance(expr, InSet)

    def test_not_in_produces_not_inset(self) -> None:
        expr = Col(Domain.EVENT, "ocel_type").not_in(["A"])
        assert isinstance(expr, Not)

    def test_is_between_produces_between(self) -> None:
        expr = Col(Domain.EVENT, "ocel_time").is_between("a", "b")
        assert isinstance(expr, Between)

    def test_not_between_produces_not_between(self) -> None:
        expr = Col(Domain.EVENT, "ocel_time").not_between("a", "b")
        assert isinstance(expr, Not)

    def test_repr(self) -> None:
        assert repr(Col(Domain.EVENT, "ocel_type")) == "Col(event.ocel_type)"
        assert repr(Col(Domain.OBJECT, "name")) == "Col(object.name)"


# ---------------------------------------------------------------------------
# Proxy
# ---------------------------------------------------------------------------


class TestProxy:
    def test_alias_type(self) -> None:
        col = event.type
        assert isinstance(col, Col)
        assert col.column == "ocel_type"

    def test_alias_time(self) -> None:
        assert event.time.column == "ocel_time"

    def test_alias_id(self) -> None:
        assert event.id.column == "ocel_id"

    def test_passthrough(self) -> None:
        assert event.total_price.column == "total_price"
        assert event.payment_method.column == "payment_method"

    def test_object_proxy_aliases(self) -> None:
        assert obj.type.column == "ocel_type"
        assert obj.time.column == "ocel_time"
        assert obj.id.column == "ocel_id"

    def test_object_proxy_passthrough(self) -> None:
        assert obj.status.column == "status"
        assert obj.name.column == "name"

    def test_getitem_bypasses_alias(self) -> None:
        col = event["type"]
        assert col.column == "type"  # NOT "ocel_type"

    def test_getitem_raw_name(self) -> None:
        col = event["ocel_type"]
        assert col.column == "ocel_type"

    def test_domain_propagation(self) -> None:
        assert event.type.domain is Domain.EVENT
        assert obj.type.domain is Domain.OBJECT

    def test_repr_event(self) -> None:
        assert repr(event) == "event"

    def test_repr_obj(self) -> None:
        assert repr(obj) == "object"


# ---------------------------------------------------------------------------
# Comparison — compile (no context)
# ---------------------------------------------------------------------------


class TestComparisonCompile:
    def test_eq_string(self) -> None:
        sql = (event.type == "Create Order").to_sql()
        assert sql == "\"ocel_type\" = 'Create Order'"

    def test_ne_string(self) -> None:
        sql = (event.type != "Pay Order").to_sql()
        assert sql == "\"ocel_type\" != 'Pay Order'"

    def test_gt_int(self) -> None:
        sql = (event.total_price > 100).to_sql()
        assert sql == '"total_price" > 100'

    def test_ge_float(self) -> None:
        sql = (event.total_price >= 99.5).to_sql()
        assert sql == '"total_price" >= 99.5'

    def test_lt_string(self) -> None:
        sql = (event.time < "2022-01-01").to_sql()
        assert sql == "\"ocel_time\" < '2022-01-01'"

    def test_le_int(self) -> None:
        sql = (event.total_price <= 0).to_sql()
        assert sql == '"total_price" <= 0'

    def test_domain_from_col(self) -> None:
        expr = event.type == "X"
        assert expr.domain is Domain.EVENT
        expr2 = obj.type == "Y"
        assert expr2.domain is Domain.OBJECT

    def test_string_escaping_in_value(self) -> None:
        sql = (event.type == "it's").to_sql()
        assert sql == "\"ocel_type\" = 'it''s'"


class TestComparisonRepr:
    def test_eq_repr(self) -> None:
        r = repr(event.type == "X")
        assert "==" in r
        assert "ocel_type" in r
        assert "'X'" in r

    def test_ne_repr(self) -> None:
        r = repr(event.type != "X")
        assert "!=" in r

    def test_gt_repr(self) -> None:
        r = repr(event.total_price > 100)
        assert ">" in r
        assert "100" in r

    def test_ge_repr(self) -> None:
        assert ">=" in repr(event.total_price >= 100)

    def test_lt_repr(self) -> None:
        assert "<" in repr(event.total_price < 100)

    def test_le_repr(self) -> None:
        assert "<=" in repr(event.total_price <= 100)


# ---------------------------------------------------------------------------
# InSet — compile (no context)
# ---------------------------------------------------------------------------


class TestInSetCompile:
    def test_single_value(self) -> None:
        sql = event.type.is_in(["Create Order"]).to_sql()
        assert sql == "\"ocel_type\" IN ('Create Order')"

    def test_multiple_values(self) -> None:
        sql = event.type.is_in(["A", "B", "C"]).to_sql()
        assert sql == "\"ocel_type\" IN ('A', 'B', 'C')"

    def test_numeric_values(self) -> None:
        sql = event.total_price.is_in([100, 200]).to_sql()
        assert sql == '"total_price" IN (100, 200)'

    def test_mixed_types(self) -> None:
        sql = event.total_price.is_in([100, 99.5]).to_sql()
        assert sql == '"total_price" IN (100, 99.5)'

    def test_empty_raises(self) -> None:
        with pytest.raises(ValueError, match="at least one value"):
            event.type.is_in([])

    def test_domain(self) -> None:
        assert event.type.is_in(["X"]).domain is Domain.EVENT
        assert obj.type.is_in(["Y"]).domain is Domain.OBJECT

    def test_repr(self) -> None:
        r = repr(event.type.is_in(["A", "B"]))
        assert "is_in" in r
        assert "'A'" in r


# ---------------------------------------------------------------------------
# Between — compile (no context)
# ---------------------------------------------------------------------------


class TestBetweenCompile:
    def test_string_range(self) -> None:
        sql = event.time.is_between("2022-01-01", "2022-12-31").to_sql()
        assert sql == "\"ocel_time\" BETWEEN '2022-01-01' AND '2022-12-31'"

    def test_numeric_range(self) -> None:
        sql = event.total_price.is_between(10, 100).to_sql()
        assert sql == '"total_price" BETWEEN 10 AND 100'

    def test_float_range(self) -> None:
        sql = event.total_price.is_between(9.5, 100.5).to_sql()
        assert sql == '"total_price" BETWEEN 9.5 AND 100.5'

    def test_domain(self) -> None:
        assert event.time.is_between("a", "b").domain is Domain.EVENT

    def test_repr(self) -> None:
        r = repr(event.time.is_between("a", "b"))
        assert "is_between" in r
        assert "'a'" in r
        assert "'b'" in r


# ---------------------------------------------------------------------------
# Not — compile (no context)
# ---------------------------------------------------------------------------


class TestNotCompile:
    def test_not_comparison(self) -> None:
        sql = (~(event.type == "X")).to_sql()
        assert sql == "NOT (\"ocel_type\" = 'X')"

    def test_not_inset(self) -> None:
        sql = event.type.not_in(["X"]).to_sql()
        assert sql == "NOT (\"ocel_type\" IN ('X'))"

    def test_not_between(self) -> None:
        sql = event.time.not_between("a", "b").to_sql()
        assert sql == "NOT (\"ocel_time\" BETWEEN 'a' AND 'b')"

    def test_double_negation(self) -> None:
        sql = (~(~(event.type == "X"))).to_sql()
        assert sql == "NOT (NOT (\"ocel_type\" = 'X'))"

    def test_domain(self) -> None:
        assert (~(event.type == "X")).domain is Domain.EVENT
        assert (~(obj.type == "Y")).domain is Domain.OBJECT

    def test_repr(self) -> None:
        r = repr(~(event.type == "X"))
        assert "~" in r


# ---------------------------------------------------------------------------
# And / Or — compile (no context)
# ---------------------------------------------------------------------------


class TestAndOrCompile:
    def test_and(self) -> None:
        sql = ((event.type == "A") & (event.type == "B")).to_sql()
        assert sql == "(\"ocel_type\" = 'A' AND \"ocel_type\" = 'B')"

    def test_or(self) -> None:
        sql = ((event.type == "A") | (event.type == "B")).to_sql()
        assert sql == "(\"ocel_type\" = 'A' OR \"ocel_type\" = 'B')"

    def test_nested(self) -> None:
        expr = (event.type == "A") & ((event.type == "B") | (event.type == "C"))
        sql = expr.to_sql()
        assert "AND" in sql
        assert "OR" in sql

    def test_and_domain(self) -> None:
        expr = (event.type == "A") & (event.total_price > 100)
        assert expr.domain is Domain.EVENT

    def test_or_domain(self) -> None:
        expr = (obj.type == "X") | (obj.name == "Y")
        assert expr.domain is Domain.OBJECT

    def test_and_repr(self) -> None:
        r = repr((event.type == "A") & (event.type == "B"))
        assert "&" in r

    def test_or_repr(self) -> None:
        r = repr((event.type == "A") | (event.type == "B"))
        assert "|" in r


# ---------------------------------------------------------------------------
# Domain validation
# ---------------------------------------------------------------------------


class TestDomainValidation:
    def test_and_cross_domain_raises(self) -> None:
        with pytest.raises(ValueError, match="Cannot combine event and object"):
            (event.type == "X") & (obj.type == "Y")

    def test_or_cross_domain_raises(self) -> None:
        with pytest.raises(ValueError, match="Cannot combine event and object"):
            (event.type == "X") | (obj.type == "Y")

    def test_and_with_non_expr_returns_not_implemented(self) -> None:
        result = (event.type == "X").__and__("not an expr")
        assert result is NotImplemented

    def test_or_with_non_expr_returns_not_implemented(self) -> None:
        result = (event.type == "X").__or__("not an expr")
        assert result is NotImplemented


# ---------------------------------------------------------------------------
# __bool__ guard
# ---------------------------------------------------------------------------


class TestBoolGuard:
    def test_comparison_in_if(self) -> None:
        with pytest.raises(TypeError, match="boolean context"):
            if event.type == "X":
                pass

    def test_and_in_if(self) -> None:
        with pytest.raises(TypeError, match="boolean context"):
            if (event.type == "X") & (event.type == "Y"):
                pass

    def test_python_and_keyword(self) -> None:
        with pytest.raises(TypeError, match="boolean context"):
            (event.type == "X") and (event.type == "Y")

    def test_python_or_keyword(self) -> None:
        with pytest.raises(TypeError, match="boolean context"):
            (event.type == "X") or (event.type == "Y")


# ---------------------------------------------------------------------------
# Op enum
# ---------------------------------------------------------------------------


class TestOp:
    def test_values(self) -> None:
        assert Op.EQ.value == "="
        assert Op.NE.value == "!="
        assert Op.GT.value == ">"
        assert Op.GE.value == ">="
        assert Op.LT.value == "<"
        assert Op.LE.value == "<="

    def test_all_members(self) -> None:
        assert len(Op) == 6


# ---------------------------------------------------------------------------
# Domain enum
# ---------------------------------------------------------------------------


class TestDomain:
    def test_values(self) -> None:
        assert Domain.EVENT.value == "event"
        assert Domain.OBJECT.value == "object"

    def test_all_members(self) -> None:
        assert len(Domain) == 2
