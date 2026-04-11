"""Integration tests for ViewQuery — execute against a real DuckDB."""

from __future__ import annotations

from oceldb.dsl import (
    attr,
    has_event,
    id_,
    in_,
    linked,
    related,
    type_,
)


class TestViewQueryObjects:
    def test_all_objects(self, ocel):
        assert ocel.objects().count() == 3

    def test_filter_by_type(self, ocel):
        assert ocel.objects("order").count() == 2

    def test_filter_by_multiple_types(self, ocel):
        assert ocel.objects("order", "customer").count() == 3

    def test_filter_by_nonexistent_type(self, ocel):
        assert ocel.objects("nonexistent").count() == 0

    def test_filter_expr(self, ocel):
        count = ocel.objects("order").filter(id_() == "o1").count()
        assert count == 1

    def test_filter_chain(self, ocel):
        count = (
            ocel.objects()
            .filter(type_() == "order")
            .filter(id_() == "o1")
            .count()
        )
        assert count == 1

    def test_ids(self, ocel):
        ids = ocel.objects("order").ids()
        assert sorted(ids) == ["o1", "o2"]

    def test_ids_customer(self, ocel):
        ids = ocel.objects("customer").ids()
        assert ids == ["o3"]

    def test_filter_in(self, ocel):
        count = ocel.objects().filter(in_(id_(), ["o1", "o3"])).count()
        assert count == 2

    def test_filter_attr(self, ocel):
        ids = ocel.objects("order").filter(attr("status") == "open").ids()
        assert ids == ["o1"]

    def test_to_sql_returns_string(self, ocel):
        sql = ocel.objects("order").to_sql()
        assert isinstance(sql, str)
        assert "order" in sql.lower() or "Order" in sql or "'order'" in sql


class TestViewQueryEvents:
    def test_all_events(self, ocel):
        assert ocel.events().count() == 5

    def test_filter_by_type(self, ocel):
        assert ocel.events("Create Order").count() == 3

    def test_filter_by_type_pay(self, ocel):
        assert ocel.events("Pay Order").count() == 2

    def test_filter_expr(self, ocel):
        count = ocel.events("Pay Order").filter(id_() == "e3").count()
        assert count == 1

    def test_ids(self, ocel):
        ids = ocel.events("Pay Order").ids()
        assert sorted(ids) == ["e3", "e4"]


class TestViewQueryRelations:
    def test_related_exists(self, ocel):
        ids = ocel.objects("order").filter(
            related("customer").exists()
        ).ids()
        assert sorted(ids) == ["o1"]

    def test_related_count(self, ocel):
        ids = ocel.objects("order").filter(
            related("customer").count() > 0
        ).ids()
        assert sorted(ids) == ["o1"]

    def test_linked_exists(self, ocel):
        ids = ocel.objects("order").filter(
            linked("customer").exists()
        ).ids()
        assert sorted(ids) == ["o1", "o2"]

    def test_has_event_exists(self, ocel):
        ids = ocel.objects("order").filter(
            has_event("Pay Order").exists()
        ).ids()
        assert sorted(ids) == ["o1", "o2"]

    def test_has_event_count(self, ocel):
        ids = ocel.objects("order").filter(
            has_event("Create Order").count() > 1
        ).ids()
        assert ids == ["o1"]

    def test_related_any(self, ocel):
        ids = ocel.objects("order").filter(
            related("customer").any(attr("name") == "Alice")
        ).ids()
        assert ids == ["o1"]

    def test_has_event_all(self, ocel):
        ids = ocel.objects("order").filter(
            has_event("Create Order").all(attr("total_price").not_null())
        ).ids()
        assert sorted(ids) == ["o1", "o2"]


class TestViewQueryImmutability:
    def test_filter_does_not_mutate(self, ocel):
        q1 = ocel.objects()
        q2 = q1.filter(type_() == "order")
        assert q1.filters == ()
        assert len(q2.filters) == 1

    def test_chain_produces_new_query(self, ocel):
        q1 = ocel.objects("order")
        q2 = q1.filter(id_() == "o1")
        assert q1 is not q2
        assert q1.count() == 2
        assert q2.count() == 1
