"""Tests for the Ocel class — lifecycle, schema inspection, and export."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from oceldb import Ocel, event, obj
from oceldb.types import Domain, Summary


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


class TestRead:
    def test_open_valid_file(self, ocel_db: Path) -> None:
        ocel = Ocel.read(ocel_db)
        assert ocel._owns_connection is True
        assert ocel._path == ocel_db
        ocel.close()

    def test_open_nonexistent_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="OCEL file not found"):
            Ocel.read(tmp_path / "does_not_exist.sqlite")

    def test_accepts_string_path(self, ocel_db: Path) -> None:
        with Ocel.read(str(ocel_db)) as ocel:
            assert ocel._path == ocel_db

    def test_context_manager_closes(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            con = ocel._con
        # After exit, connection is closed — executing should fail
        with pytest.raises(Exception):
            con.execute("SELECT 1")

    def test_repr(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            r = repr(ocel)
            assert "Ocel(" in r
            assert str(ocel_db) in r


# ---------------------------------------------------------------------------
# Schema inspection
# ---------------------------------------------------------------------------


class TestSchemaInspection:
    def test_event_types(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            types = [r[0] for r in ocel.event_types().fetchall()]
            assert types == ["Create Order", "Pay Order"]

    def test_object_types(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            types = [r[0] for r in ocel.object_types().fetchall()]
            assert types == ["customer", "order"]

    def test_events(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            events = ocel.events().fetchall()
            assert len(events) == 5
            assert {e[0] for e in events} == {"e1", "e2", "e3", "e4", "e5"}

    def test_objects(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            objects = ocel.objects().fetchall()
            assert len(objects) == 3
            assert {o[0] for o in objects} == {"o1", "o2", "o3"}

    def test_event_objects(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            e2o = ocel.event_objects().fetchall()
            assert len(e2o) == 6

    def test_object_objects(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            o2o = ocel.object_objects().fetchall()
            assert len(o2o) == 2
            pairs = {(r[0], r[1]) for r in o2o}
            assert ("o1", "o3") in pairs
            assert ("o2", "o3") in pairs


class TestAttributes:
    def test_event_type_attributes(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            cols = ocel.attributes(Domain.EVENT, "Create Order")
            assert "ocel_id" in cols
            assert "ocel_time" in cols
            assert "total_price" in cols

    def test_pay_order_attributes(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            cols = ocel.attributes(Domain.EVENT, "Pay Order")
            assert "payment_method" in cols

    def test_object_type_attributes(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            cols = ocel.attributes(Domain.OBJECT, "order")
            assert "ocel_id" in cols
            assert "status" in cols

    def test_unknown_event_type_raises(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with pytest.raises(ValueError, match="Unknown event type"):
                ocel.attributes(Domain.EVENT, "Nonexistent")

    def test_unknown_object_type_raises(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with pytest.raises(ValueError, match="Unknown object type"):
                ocel.attributes(Domain.OBJECT, "Nonexistent")


class TestSummary:
    def test_all_fields(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            s = ocel.summary()
            assert isinstance(s, Summary)
            assert s.num_events == 5
            assert s.num_objects == 3
            assert s.num_event_types == 2
            assert s.num_object_types == 2
            assert s.event_types == ["Create Order", "Pay Order"]
            assert s.object_types == ["customer", "order"]
            assert s.num_e2o_relations == 6
            assert s.num_o2o_relations == 2

    def test_summary_on_filtered_view(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.type == "Create Order").create() as view:
                s = view.summary()
                assert s.num_events == 3
                # o1, o2, o3 all referenced by Create Order events
                assert s.num_objects == 3
                # Only "Create Order" events survive → 1 distinct type
                assert s.num_event_types == 1
                assert s.num_e2o_relations == 4  # e1→o1, e1→o3, e2→o2, e5→o1


# ---------------------------------------------------------------------------
# SQL execution
# ---------------------------------------------------------------------------


class TestSql:
    def test_arbitrary_query(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            result = ocel.sql("SELECT COUNT(*) FROM event").fetchone()
            assert result is not None
            assert result[0] == 5

    def test_query_per_type_table(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            rows = ocel.sql(
                'SELECT ocel_id, total_price FROM "event_CreateOrder" '
                "ORDER BY total_price"
            ).fetchall()
            assert len(rows) == 3
            assert rows[0][1] == 100.0
            assert rows[-1][1] == 300.0

    def test_sql_on_view(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.type == "Pay Order").create() as view:
                result = view.sql("SELECT COUNT(*) FROM event").fetchone()
                assert result is not None
                assert result[0] == 2


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------


class TestToSqlite:
    def test_roundtrip_preserves_all_tables(self, ocel_db: Path, tmp_path: Path) -> None:
        out = tmp_path / "exported.sqlite"
        with Ocel.read(ocel_db) as ocel:
            ocel.to_sqlite(out)

        with Ocel.read(out) as reloaded:
            s = reloaded.summary()
            assert s.num_events == 5
            assert s.num_objects == 3
            assert s.num_e2o_relations == 6
            assert s.num_o2o_relations == 2

    def test_roundtrip_preserves_per_type_data(self, ocel_db: Path, tmp_path: Path) -> None:
        out = tmp_path / "exported.sqlite"
        with Ocel.read(ocel_db) as ocel:
            ocel.to_sqlite(out)

        with Ocel.read(out) as reloaded:
            cols = reloaded.attributes(Domain.EVENT, "Create Order")
            assert "total_price" in cols
            rows = reloaded.sql(
                'SELECT total_price FROM "event_CreateOrder" ORDER BY total_price'
            ).fetchall()
            assert [r[0] for r in rows] == [100.0, 250.0, 300.0]

    def test_overwrite_existing_file(self, ocel_db: Path, tmp_path: Path) -> None:
        out = tmp_path / "exported.sqlite"
        out.write_text("garbage")
        with Ocel.read(ocel_db) as ocel:
            ocel.to_sqlite(out)

        with Ocel.read(out) as reloaded:
            assert reloaded.summary().num_events == 5

    def test_filtered_export(self, ocel_db: Path, tmp_path: Path) -> None:
        out = tmp_path / "filtered.sqlite"
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.type == "Pay Order").create() as view:
                view.to_sqlite(out)

        with Ocel.read(out) as reloaded:
            assert reloaded.summary().num_events == 2
            events = reloaded.events().fetchall()
            assert {e[0] for e in events} == {"e3", "e4"}

    def test_object_filtered_export(self, ocel_db: Path, tmp_path: Path) -> None:
        out = tmp_path / "filtered.sqlite"
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(obj.type == "customer").create() as view:
                view.to_sqlite(out)

        with Ocel.read(out) as reloaded:
            objects = reloaded.objects().fetchall()
            assert {o[0] for o in objects} == {"o3"}
            # Only e1 touches o3
            events = reloaded.events().fetchall()
            assert {e[0] for e in events} == {"e1"}


class TestToPm4py:
    def test_import_error_without_pm4py(self, ocel_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        import builtins

        real_import = builtins.__import__

        def mock_import(name: str, *args, **kwargs):
            if name == "pm4py":
                raise ImportError("mocked")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", mock_import)
        with Ocel.read(ocel_db) as ocel:
            with pytest.raises(ImportError, match="pm4py is required"):
                ocel.to_pm4py()


# ---------------------------------------------------------------------------
# View-backed lifecycle
# ---------------------------------------------------------------------------


class TestViewBacked:
    def test_view_does_not_own_connection(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.type == "Pay Order").create() as view:
                assert view._owns_connection is False

    def test_view_close_drops_schema(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            view = ocel.view().where(event.type == "Pay Order").create()
            schema = view._schema_prefix
            view.close()
            # Schema should be gone — querying should fail
            with pytest.raises(Exception):
                ocel._con.execute(f"SELECT * FROM {schema}.event")

    def test_parent_still_works_after_view_close(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.type == "Pay Order").create():
                pass
            # Parent should still work
            assert ocel.summary().num_events == 5

    def test_multiple_views_independent(self, ocel_db: Path) -> None:
        with Ocel.read(ocel_db) as ocel:
            with ocel.view().where(event.type == "Create Order").create() as v1:
                with ocel.view().where(event.type == "Pay Order").create() as v2:
                    assert {e[0] for e in v1.events().fetchall()} == {"e1", "e2", "e5"}
                    assert {e[0] for e in v2.events().fetchall()} == {"e3", "e4"}
