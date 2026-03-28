"""Tests for ViewBuilder — materialization, propagation, and lifecycle."""

from __future__ import annotations

from pathlib import Path

import pytest

from oceldb import Ocel, event, obj
from oceldb.types import Domain


# ---------------------------------------------------------------------------
# Identity view (no conditions)
# ---------------------------------------------------------------------------


class TestIdentityView:
    def test_no_conditions_returns_all_events(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().create() as view:
                events = view.events().fetchall()
                assert len(events) == 5

    def test_no_conditions_returns_all_objects(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().create() as view:
                objects = view.objects().fetchall()
                assert len(objects) == 3

    def test_no_conditions_preserves_e2o(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().create() as view:
                assert len(view.event_objects().fetchall()) == 6

    def test_no_conditions_preserves_o2o(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().create() as view:
                assert len(view.object_objects().fetchall()) == 2

    def test_no_conditions_preserves_per_type_tables(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().create() as view:
                rows = view.sql(
                    'SELECT * FROM "event_CreateOrder"'
                ).fetchall()
                assert len(rows) == 3


# ---------------------------------------------------------------------------
# Builder immutability
# ---------------------------------------------------------------------------


class TestBuilderImmutability:
    def test_where_returns_new_builder(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            b1 = ocel.view()
            b2 = b1.where(event.type == "Create Order")
            assert b1 is not b2

    def test_original_unchanged_after_where(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            b1 = ocel.view()
            b1.where(event.type == "Create Order")
            # b1 should still have no conditions
            with b1.create() as view:
                assert len(view.events().fetchall()) == 5

    def test_chained_where_accumulates(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with (
                ocel.view()
                .where(event.type == "Create Order")
                .where(event.total_price > 100)
                .create()
            ) as view:
                assert {e[0] for e in view.events().fetchall()} == {"e2", "e5"}


# ---------------------------------------------------------------------------
# Event-only conditions
# ---------------------------------------------------------------------------


class TestEventOnlyConditions:
    def test_single_event_filter(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.type == "Pay Order").create() as view:
                assert {e[0] for e in view.events().fetchall()} == {"e3", "e4"}

    def test_event_filter_propagates_to_objects(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.type == "Pay Order").create() as view:
                # e3→o1, e4→o2
                assert {o[0] for o in view.objects().fetchall()} == {"o1", "o2"}

    def test_event_filter_propagates_to_e2o(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.type == "Pay Order").create() as view:
                e2o = view.event_objects().fetchall()
                event_ids = {r[0] for r in e2o}
                assert event_ids == {"e3", "e4"}

    def test_event_filter_propagates_to_per_type(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.type == "Pay Order").create() as view:
                # CreateOrder per-type table should have 0 rows
                rows = view.sql('SELECT * FROM "event_CreateOrder"').fetchall()
                assert len(rows) == 0
                # PayOrder per-type table should have 2 rows
                rows = view.sql('SELECT * FROM "event_PayOrder"').fetchall()
                assert len(rows) == 2


# ---------------------------------------------------------------------------
# Object-only conditions
# ---------------------------------------------------------------------------


class TestObjectOnlyConditions:
    def test_single_object_filter(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(obj.type == "customer").create() as view:
                assert {o[0] for o in view.objects().fetchall()} == {"o3"}

    def test_object_filter_propagates_to_events(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            # o3 only touched by e1
            with ocel.view().where(obj.type == "customer").create() as view:
                assert {e[0] for e in view.events().fetchall()} == {"e1"}

    def test_object_filter_propagates_to_o2o(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            # Keep only o3 — O2O has o1→o3, o2→o3 but o1/o2 are pruned
            with ocel.view().where(obj.type == "customer").create() as view:
                o2o = view.object_objects().fetchall()
                assert len(o2o) == 0

    def test_object_attribute_filter(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(obj.status == "open").create() as view:
                # o1 and o2 are "open" orders
                assert {o[0] for o in view.objects().fetchall()} == {"o1", "o2"}


# ---------------------------------------------------------------------------
# Mixed domain conditions (event + object)
# ---------------------------------------------------------------------------


class TestMixedDomainConditions:
    def test_event_and_object_filter(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            # Events: Create Order → {e1, e2, e5}
            # Objects: customer → {o3}
            # Intersection: events touching o3 among {e1,e2,e5} → {e1}
            # Objects from those events among {o3} → {o3}
            with (
                ocel.view()
                .where(event.type == "Create Order")
                .where(obj.type == "customer")
                .create()
            ) as view:
                assert {e[0] for e in view.events().fetchall()} == {"e1"}
                assert {o[0] for o in view.objects().fetchall()} == {"o3"}

    def test_event_and_object_filter_e2o(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with (
                ocel.view()
                .where(event.type == "Create Order")
                .where(obj.type == "customer")
                .create()
            ) as view:
                e2o = view.event_objects().fetchall()
                assert len(e2o) == 1
                assert e2o[0][0] == "e1"
                assert e2o[0][1] == "o3"

    def test_narrow_both_domains(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with (
                ocel.view()
                .where(event.type == "Pay Order")
                .where(obj.type == "order")
                .create()
            ) as view:
                # Pay Order events: {e3, e4}; order objects: {o1, o2}
                # e3→o1, e4→o2 — all survive intersection
                assert {e[0] for e in view.events().fetchall()} == {"e3", "e4"}
                assert {o[0] for o in view.objects().fetchall()} == {"o1", "o2"}


# ---------------------------------------------------------------------------
# View-on-view (nested)
# ---------------------------------------------------------------------------


class TestNestedViews:
    def test_view_on_view(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.type == "Create Order").create() as v1:
                # e1=100, e2=250, e5=300 → >200 keeps e2 and e5
                with v1.view().where(event.total_price > 200).create() as v2:
                    events = v2.events().fetchall()
                    assert {e[0] for e in events} == {"e2", "e5"}

    def test_view_on_view_propagation(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.type == "Create Order").create() as v1:
                # e2→o2, e5→o1
                with v1.view().where(event.total_price > 200).create() as v2:
                    objects = v2.objects().fetchall()
                    assert {o[0] for o in objects} == {"o1", "o2"}

    def test_three_level_nesting(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.type == "Create Order").create() as v1:
                with v1.view().where(event.total_price > 100).create() as v2:
                    with v2.view().where(event.total_price < 300).create() as v3:
                        events = v3.events().fetchall()
                        assert {e[0] for e in events} == {"e2"}


# ---------------------------------------------------------------------------
# Map table pass-through
# ---------------------------------------------------------------------------


class TestMapTables:
    def test_map_tables_always_pass_through(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.type == "Pay Order").create() as view:
                # Map tables should still list all types
                event_maps = view.sql(
                    "SELECT ocel_type FROM event_map_type ORDER BY ocel_type"
                ).fetchall()
                assert [r[0] for r in event_maps] == ["Create Order", "Pay Order"]

    def test_object_map_tables_pass_through(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(obj.type == "customer").create() as view:
                object_maps = view.sql(
                    "SELECT ocel_type FROM object_map_type ORDER BY ocel_type"
                ).fetchall()
                assert [r[0] for r in object_maps] == ["customer", "order"]


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestViewEdgeCases:
    def test_filter_to_empty_set(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.type == "Nonexistent").create() as view:
                assert len(view.events().fetchall()) == 0
                assert len(view.objects().fetchall()) == 0
                assert len(view.event_objects().fetchall()) == 0

    def test_single_event_preserves_related_objects(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            # e1 relates to o1 and o3
            with ocel.view().where(event.id == "e1").create() as view:
                objects = view.objects().fetchall()
                assert {o[0] for o in objects} == {"o1", "o3"}

    def test_single_object_preserves_related_events(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            # o2 is related to e2, e4
            with ocel.view().where(obj.id == "o2").create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e2", "e4"}

    def test_summary_on_empty_view(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.type == "Nonexistent").create() as view:
                s = view.summary()
                assert s.num_events == 0
                assert s.num_objects == 0
                assert s.num_e2o_relations == 0

    def test_export_empty_view(self, ocel_db: Path, tmp_path: Path) -> None:
        out = tmp_path / "empty.sqlite"
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.type == "Nonexistent").create() as view:
                view.to_sqlite(out)

        with Ocel.read(out) as reloaded:
            assert reloaded.summary().num_events == 0

    def test_multiple_where_args(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(
                event.type == "Create Order",
                event.total_price > 100,
                event.total_price < 300,
            ).create() as view:
                events = view.events().fetchall()
                assert {e[0] for e in events} == {"e2"}
