"""Tests for the filter API."""

from __future__ import annotations

from pathlib import Path

import pytest

from oceldb import Ocel, event, obj
from oceldb.expr import Col, Comparison, Op
from oceldb.types import Domain


class TestEventTypeFilter:
    def test_eq(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.type == "Create Order").create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e1", "e2", "e5"}

    def test_ne(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.type != "Create Order").create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e3", "e4"}

    def test_is_in(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(
                event.type.is_in(["Create Order", "Pay Order"])
            ).create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e1", "e2", "e3", "e4", "e5"}

    def test_not_in(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(
                event.type.not_in(["Create Order"])
            ).create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e3", "e4"}


class TestEventTimeFilter:
    def test_gt(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.time > "2022-01-01T11:00:00").create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e3", "e4", "e5"}

    def test_lt(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.time < "2022-01-01T12:00:00").create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e1", "e2"}

    def test_between(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(
                event.time.is_between("2022-01-01T11:00:00", "2022-01-01T13:00:00")
            ).create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e2", "e3", "e4"}


class TestEventAttributeFilter:
    def test_gt(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.total_price > 100).create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e2", "e5"}

    def test_eq(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(
                event.payment_method == "credit_card"
            ).create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e3"}

    def test_unknown_attribute(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with pytest.raises(ValueError, match="No event type has attribute"):
                ocel.view().where(event.nonexistent == "x").create()


class TestObjectFilter:
    def test_type_eq(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(obj.type == "order").create() as view:
                objects = view.objects().fetchall()
                assert {o[0] for o in objects} == {"o1", "o2"}

    def test_attribute(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(obj.name == "Alice").create() as view:
                objects = view.objects().fetchall()
                assert {o[0] for o in objects} == {"o3"}


class TestChainedFilters:
    def test_multiple_args(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(
                event.type == "Create Order",
                event.total_price > 100,
            ).create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e2", "e5"}

    def test_chained_where(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with (
                ocel.view()
                .where(event.type == "Create Order")
                .where(event.total_price > 100)
                .create()
            ) as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e2", "e5"}


class TestPropagation:
    def test_event_filter_keeps_referenced_objects(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.type == "Create Order").create() as view:
                objects = view.objects().fetchall()
                obj_ids = {o[0] for o in objects}
                assert obj_ids == {"o1", "o2", "o3"}

    def test_event_filter_prunes_unreferenced_objects(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            # e2 only relates to o2 — o1 and o3 should be pruned
            with ocel.view().where(event.id == "e2").create() as view:
                objects = view.objects().fetchall()
                assert {o[0] for o in objects} == {"o2"}

    def test_object_filter_prunes_orphaned_events(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            # o3 (customer) only relates to e1
            with ocel.view().where(obj.type == "customer").create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e1"}

    def test_e2o_filtered(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.type == "Create Order").create() as view:
                e2o = view.event_objects().fetchall()
                event_ids = {r[0] for r in e2o}
                # No Pay Order events in E2O
                assert event_ids <= {"e1", "e2", "e5"}

    def test_o2o_filtered(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            # Only keep e2 → only o2 survives → O2O (o2→o3) loses o3
            with ocel.view().where(event.id == "e2").create() as view:
                o2o = view.object_objects().fetchall()
                assert len(o2o) == 0


class TestComposition:
    def test_and_operator(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            expr = (event.type == "Create Order") & (event.total_price > 100)
            with ocel.view().where(expr).create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e2", "e5"}

    def test_or_operator(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            expr = (event.type == "Create Order") | (event.type == "Pay Order")
            with ocel.view().where(expr).create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e1", "e2", "e3", "e4", "e5"}

    def test_not_operator(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(~(event.type == "Create Order")).create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e3", "e4"}

    def test_nested_and_or(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            # (Create Order AND total_price > 100) OR Pay Order
            expr = (
                (event.type == "Create Order") & (event.total_price > 100)
            ) | (event.type == "Pay Order")
            with ocel.view().where(expr).create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e2", "e3", "e4", "e5"}

    def test_not_or(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            # NOT (Create Order OR Pay Order) → empty (all events are one of those)
            expr = ~(
                (event.type == "Create Order") | (event.type == "Pay Order")
            )
            with ocel.view().where(expr).create() as view:
                events = view.events().fetchall()
                assert len(events) == 0

    def test_domain_mismatch_and(self) -> None:
        with pytest.raises(ValueError, match="Cannot combine"):
            (event.type == "Create Order") & (obj.type == "order")

    def test_domain_mismatch_or(self) -> None:
        with pytest.raises(ValueError, match="Cannot combine"):
            (event.type == "Create Order") | (obj.type == "order")

    def test_bool_raises(self) -> None:
        with pytest.raises(TypeError, match="boolean context"):
            if event.type == "Create Order":
                pass


class TestRepr:
    def test_col(self) -> None:
        assert repr(event.type) == "Col(event.ocel_type)"

    def test_comparison(self) -> None:
        r = repr(event.type == "Create Order")
        assert "ocel_type" in r and "Create Order" in r

    def test_and(self) -> None:
        r = repr((event.type == "X") & (event.type == "Y"))
        assert "&" in r

    def test_or(self) -> None:
        r = repr((event.type == "X") | (event.type == "Y"))
        assert "|" in r

    def test_not(self) -> None:
        r = repr(~(event.type == "X"))
        assert "~" in r

    def test_proxy(self) -> None:
        assert repr(event) == "event"
        assert repr(obj) == "object"


class TestPowerUser:
    def test_col_direct(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            col = Col(Domain.EVENT, "ocel_type")
            expr = Comparison(col, Op.EQ, "Create Order")
            with ocel.view().where(expr).create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e1", "e2", "e5"}

    def test_getitem_bypass_alias(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event["ocel_type"] == "Create Order").create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e1", "e2", "e5"}


class TestEdgeCases:
    def test_is_in_empty_raises(self) -> None:
        with pytest.raises(ValueError, match="at least one value"):
            event.type.is_in([])

    def test_bool_value_raises(self) -> None:
        with pytest.raises(TypeError, match="Boolean"):
            (event.type == True).to_sql()  # noqa: E712


class TestViewExport:
    def test_to_sqlite_roundtrip(self, ocel_db: Path, tmp_path: Path) -> None:
        out = tmp_path / "filtered.sqlite"
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.type == "Create Order").create() as view:
                view.to_sqlite(out)

        with Ocel.read(out) as reloaded:
            events = reloaded.events().fetchall()
            assert {e[0] for e in events} == {"e1", "e2", "e5"}
