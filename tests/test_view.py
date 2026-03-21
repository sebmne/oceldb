"""Tests for the ViewBuilder and filtered view creation."""

from __future__ import annotations

from pathlib import Path

import pytest

from oceldb import Ocel, event, obj


class TestEventFilters:
    def test_filter_by_event_type(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            filtered = ocel.view().filter(event.type == "Create Order").create()
            s = filtered.summary()
            assert s.num_events == 2
            assert s.event_types == ["Create Order"]
            filtered.close()

    def test_filter_by_event_type_is_in(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            filtered = ocel.view().filter(event.type.is_in("Pay Order")).create()
            assert filtered.summary().num_events == 1
            filtered.close()

    def test_filter_by_time(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            filtered = ocel.view().filter(event.time > "2022-01-01T10:30:00").create()
            # e2 (2022-01-02T11:00:00) and e3 (2022-01-03T12:00:00)
            assert filtered.summary().num_events == 2
            filtered.close()

    def test_filter_by_per_type_attribute(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            filtered = ocel.view().filter(event.total_price > 200).create()
            # Only e2 has total_price=250.0; e3 has NULL total_price
            assert filtered.summary().num_events == 1
            filtered.close()

    def test_chained_filters(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            filtered = (
                ocel.view()
                .filter(event.type == "Create Order")
                .filter(event.total_price > 200)
                .create()
            )
            assert filtered.summary().num_events == 1
            filtered.close()


class TestObjectFilters:
    def test_filter_by_object_type(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            filtered = ocel.view().filter(obj.type == "order").create()
            s = filtered.summary()
            assert s.num_objects == 2
            assert s.object_types == ["order"]
            filtered.close()

    def test_filter_by_object_attribute(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            filtered = ocel.view().filter(obj.name == "Alice").create()
            assert filtered.summary().num_objects == 1
            filtered.close()


class TestCombinedFilters:
    def test_event_and_object_filters(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            filtered = (
                ocel.view()
                .filter(event.type == "Create Order")
                .filter(obj.type == "order")
                .create()
            )
            s = filtered.summary()
            assert s.num_events == 2
            assert s.num_objects == 2
            filtered.close()


class TestRelationshipScoping:
    def test_event_object_scoped(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            filtered = (
                ocel.view()
                .filter(event.type == "Create Order")
                .filter(obj.type == "order")
                .create()
            )
            # Only e2o links where event is Create Order AND object is order
            # e1→o1, e2→o2 survive (e1→o3 dropped because o3 is customer,
            # e3→o1 dropped because e3 is Pay Order)
            assert filtered.summary().num_e2o_relations == 2
            filtered.close()

    def test_object_object_scoped(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            filtered = ocel.view().filter(obj.type == "order").create()
            # o2o: o1→o3, o2→o3 — but o3 is customer, filtered out
            assert filtered.summary().num_o2o_relations == 0
            filtered.close()


class TestFilteredExport:
    def test_filtered_to_sqlite(self, ocel_db: Path, tmp_path: Path):
        out = tmp_path / "filtered.sqlite"
        with Ocel.read(ocel_db) as ocel:
            filtered = ocel.view().filter(event.type == "Create Order").create()
            filtered.to_sqlite(out)
            filtered.close()

        with Ocel.read(out) as exported:
            assert exported.summary().num_events == 2

    def test_view_of_view(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            v1 = ocel.view().filter(event.type == "Create Order").create()
            v2 = v1.view().filter(event.total_price > 200).create()
            assert v2.summary().num_events == 1
            v2.close()
            v1.close()


class TestFilterErrors:
    def test_mixed_domain_raises(self, ocel_db: Path):
        from oceldb.expr import BinOp

        with Ocel.read(ocel_db) as ocel:
            mixed = BinOp("AND", event.type == "A", obj.type == "order")
            with pytest.raises(ValueError, match="cannot mix"):
                ocel.view().filter(mixed).create()

    def test_column_free_expression_raises(self, ocel_db: Path):
        from oceldb.expr import BinOp, Literal

        with Ocel.read(ocel_db) as ocel:
            column_free = BinOp("=", Literal(1), 1)
            with pytest.raises(ValueError, match="must reference"):
                ocel.view().filter(column_free).create()
