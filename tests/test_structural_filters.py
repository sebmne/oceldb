"""Tests for structural filters (EventuallyFollows, DirectlyFollows)."""

from __future__ import annotations

from pathlib import Path

import pytest

from oceldb import Ocel, event
from oceldb.filters import DirectlyFollows, EventuallyFollows
from oceldb.types import Domain


# ---------------------------------------------------------------------------
# EventuallyFollows
# ---------------------------------------------------------------------------


class TestEventuallyFollows:
    def test_create_then_pay(self, ocel_db: Path) -> None:
        # o1: CO(e1) → PO(e3), o2: CO(e2) → PO(e4)
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(EventuallyFollows("Create Order", "Pay Order")).create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e3", "e4"}

    def test_pay_then_create(self, ocel_db: Path) -> None:
        # o1: PO(e3) → CO(e5)
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(EventuallyFollows("Pay Order", "Create Order")).create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e5"}

    def test_no_match(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(EventuallyFollows("Pay Order", "Pay Order")).create() as view:
                events = view.events().fetchall()
                assert len(events) == 0

    def test_composition_with_attribute(self, ocel_db: Path) -> None:
        # EF targets are {e3, e4}; credit_card narrows to {e3}
        with Ocel.read(ocel_db) as ocel:
            expr = EventuallyFollows("Create Order", "Pay Order") & (
                event.payment_method == "credit_card"
            )
            with ocel.view().where(expr).create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e3"}

    def test_propagation_prunes_objects(self, ocel_db: Path) -> None:
        # EF("Pay Order", "Create Order") → {e5}, e5 relates to o1 only
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(EventuallyFollows("Pay Order", "Create Order")).create() as view:
                objects = view.objects().fetchall()
                assert {o[0] for o in objects} == {"o1"}

    def test_domain_is_event(self) -> None:
        assert EventuallyFollows("A", "B").domain is Domain.EVENT

    def test_repr(self) -> None:
        r = repr(EventuallyFollows("Create Order", "Pay Order"))
        assert r == "EventuallyFollows('Create Order', 'Pay Order')"

    def test_compile_without_context_raises(self) -> None:
        with pytest.raises(ValueError, match="requires a CompilationContext"):
            EventuallyFollows("A", "B").compile(None)

    def test_nonexistent_source_type(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(EventuallyFollows("Nonexistent", "Pay Order")).create() as view:
                assert len(view.events().fetchall()) == 0

    def test_nonexistent_target_type(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(EventuallyFollows("Create Order", "Nonexistent")).create() as view:
                assert len(view.events().fetchall()) == 0

    def test_or_composition(self, ocel_db: Path) -> None:
        # EF(CO→PO) = {e3,e4}, EF(PO→CO) = {e5} → union = {e3,e4,e5}
        with Ocel.read(ocel_db) as ocel:
            expr = (
                EventuallyFollows("Create Order", "Pay Order")
                | EventuallyFollows("Pay Order", "Create Order")
            )
            with ocel.view().where(expr).create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e3", "e4", "e5"}

    def test_composition_with_time(self, ocel_db: Path) -> None:
        # EF(CO→PO) = {e3,e4}; time > 12:30 → {e4} only
        with Ocel.read(ocel_db) as ocel:
            expr = EventuallyFollows("Create Order", "Pay Order") & (
                event.time > "2022-01-01T12:30:00"
            )
            with ocel.view().where(expr).create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e4"}

    def test_e2o_on_ef_result(self, ocel_db: Path) -> None:
        # EF(CO→PO) = {e3,e4}; surviving objects: o1(e3),o2(e4)
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(EventuallyFollows("Create Order", "Pay Order")).create() as view:
                e2o = view.event_objects().fetchall()
                assert {(r[0], r[1]) for r in e2o} == {("e3", "o1"), ("e4", "o2")}

    def test_export_ef_result(self, ocel_db: Path, tmp_path: Path) -> None:
        out = tmp_path / "ef.sqlite"
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(EventuallyFollows("Create Order", "Pay Order")).create() as view:
                view.to_sqlite(out)
        with Ocel.read(out) as reloaded:
            assert {e[0] for e in reloaded.events().fetchall()} == {"e3", "e4"}


# ---------------------------------------------------------------------------
# DirectlyFollows
# ---------------------------------------------------------------------------


class TestDirectlyFollows:
    def test_create_then_pay(self, ocel_db: Path) -> None:
        # o1: e1 directly → e3 (nothing between for o1)
        # o2: e2 directly → e4 (nothing between for o2)
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(DirectlyFollows("Create Order", "Pay Order")).create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e3", "e4"}

    def test_no_direct_create_create(self, ocel_db: Path) -> None:
        # o1: CO(e1) → PO(e3) → CO(e5) — e3 sits between e1 and e5
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(DirectlyFollows("Create Order", "Create Order")).create() as view:
                events = view.events().fetchall()
                assert len(events) == 0

    def test_pay_then_create(self, ocel_db: Path) -> None:
        # o1: PO(e3) directly → CO(e5) (nothing between for o1)
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(DirectlyFollows("Pay Order", "Create Order")).create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e5"}

    def test_composition_with_not(self, ocel_db: Path) -> None:
        # DF targets are {e3, e4}; exclude credit_card → {e4}
        with Ocel.read(ocel_db) as ocel:
            expr = DirectlyFollows("Create Order", "Pay Order") & ~(
                event.payment_method == "credit_card"
            )
            with ocel.view().where(expr).create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e4"}

    def test_domain_is_event(self) -> None:
        assert DirectlyFollows("A", "B").domain is Domain.EVENT

    def test_repr(self) -> None:
        r = repr(DirectlyFollows("Create Order", "Pay Order"))
        assert r == "DirectlyFollows('Create Order', 'Pay Order')"

    def test_compile_without_context_raises(self) -> None:
        with pytest.raises(ValueError, match="requires a CompilationContext"):
            DirectlyFollows("A", "B").compile(None)

    def test_nonexistent_types(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(DirectlyFollows("Nonexistent", "Also Nonexistent")).create() as view:
                assert len(view.events().fetchall()) == 0

    def test_or_composition(self, ocel_db: Path) -> None:
        # DF(CO→PO) = {e3,e4}, DF(PO→CO) = {e5}
        with Ocel.read(ocel_db) as ocel:
            expr = (
                DirectlyFollows("Create Order", "Pay Order")
                | DirectlyFollows("Pay Order", "Create Order")
            )
            with ocel.view().where(expr).create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e3", "e4", "e5"}

    def test_df_is_subset_of_ef(self, ocel_db: Path) -> None:
        """Directly follows should always be a subset of eventually follows."""
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(EventuallyFollows("Create Order", "Pay Order")).create() as ef_view:
                ef_ids = {e[0] for e in ef_view.events().fetchall()}
            with ocel.view().where(DirectlyFollows("Create Order", "Pay Order")).create() as df_view:
                df_ids = {e[0] for e in df_view.events().fetchall()}
            assert df_ids <= ef_ids

    def test_df_stricter_than_ef(self, ocel_db: Path) -> None:
        """DF(CO→CO) is empty but EF(CO→CO) via o1: e1→e5 is not."""
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(EventuallyFollows("Create Order", "Create Order")).create() as ef_view:
                ef_ids = {e[0] for e in ef_view.events().fetchall()}
            with ocel.view().where(DirectlyFollows("Create Order", "Create Order")).create() as df_view:
                df_ids = {e[0] for e in df_view.events().fetchall()}
            # EF finds e5 (via o1: e1→...→e5), but DF doesn't (e3 in between)
            assert ef_ids == {"e5"}
            assert df_ids == set()


# ---------------------------------------------------------------------------
# Convenience imports
# ---------------------------------------------------------------------------


class TestConvenienceImports:
    def test_import_from_filters(self) -> None:
        from oceldb.filters import DirectlyFollows, EventuallyFollows

        assert DirectlyFollows is not None
        assert EventuallyFollows is not None

    def test_import_from_filters_events(self) -> None:
        from oceldb.filters.events import DirectlyFollows, EventuallyFollows

        assert DirectlyFollows is not None
        assert EventuallyFollows is not None


# ---------------------------------------------------------------------------
# Cross-domain composition error
# ---------------------------------------------------------------------------


class TestCrossDomainError:
    def test_ef_and_object_expr_raises(self) -> None:
        from oceldb import obj

        with pytest.raises(ValueError, match="Cannot combine"):
            EventuallyFollows("A", "B") & (obj.type == "order")

    def test_df_and_object_expr_raises(self) -> None:
        from oceldb import obj

        with pytest.raises(ValueError, match="Cannot combine"):
            DirectlyFollows("A", "B") & (obj.type == "order")
