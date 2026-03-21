"""Tests for the Ocel class (reading, inspection, export)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from oceldb import Domain, Ocel, Summary


class TestRead:
    def test_read_valid_file(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            assert ocel._schema_prefix == "ocel_db.main"
            assert ocel._owns_connection is True

    def test_read_missing_file(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            Ocel.read(tmp_path / "nonexistent.sqlite")

    def test_context_manager(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            assert ocel._con is not None


class TestSql:
    def test_sql_returns_relation(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            result = ocel.sql("SELECT COUNT(*) AS cnt FROM event")
            assert result.fetchone()[0] == 3


class TestSchemaInspection:
    def test_event_types(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            types = [r[0] for r in ocel.event_types().fetchall()]
            assert sorted(types) == ["Create Order", "Pay Order"]

    def test_object_types(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            types = [r[0] for r in ocel.object_types().fetchall()]
            assert sorted(types) == ["customer", "order"]

    def test_events(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            rows = ocel.events().fetchall()
            assert len(rows) == 3

    def test_objects(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            rows = ocel.objects().fetchall()
            assert len(rows) == 3

    def test_event_objects(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            rows = ocel.event_objects().fetchall()
            assert len(rows) == 4

    def test_object_objects(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            rows = ocel.object_objects().fetchall()
            assert len(rows) == 2

    def test_attributes_event(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            attrs = ocel.attributes(Domain.EVENT, "Create Order")
            assert "total_price" in attrs
            assert "ocel_id" not in attrs
            assert "ocel_time" not in attrs

    def test_attributes_object(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            attrs = ocel.attributes(Domain.OBJECT, "customer")
            assert "name" in attrs

    def test_attributes_unknown_type(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            with pytest.raises(ValueError, match="Unknown"):
                ocel.attributes(Domain.EVENT, "Nonexistent")


class TestSummary:
    def test_summary(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            s = ocel.summary()
            assert isinstance(s, Summary)
            assert s.num_events == 3
            assert s.num_objects == 3
            assert s.num_event_types == 2
            assert s.num_object_types == 2
            assert sorted(s.event_types) == ["Create Order", "Pay Order"]
            assert sorted(s.object_types) == ["customer", "order"]
            assert s.num_e2o_relations == 4
            assert s.num_o2o_relations == 2


class TestExport:
    def test_to_sqlite_roundtrip(self, ocel_db: Path, tmp_path: Path):
        out = tmp_path / "exported.sqlite"
        with Ocel.read(ocel_db) as ocel:
            ocel.to_sqlite(out)

        assert out.exists()
        with Ocel.read(out) as exported:
            assert exported.summary().num_events == 3
            assert exported.summary().num_objects == 3

    def test_to_sqlite_overwrites(self, ocel_db: Path, tmp_path: Path):
        out = tmp_path / "exported.sqlite"
        out.write_text("placeholder")
        with Ocel.read(ocel_db) as ocel:
            ocel.to_sqlite(out)
        with Ocel.read(out) as exported:
            assert exported.summary().num_events == 3

    def test_to_pm4py_import_error(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            with patch.dict("sys.modules", {"pm4py": None}):
                with pytest.raises(ImportError, match="pip install oceldb"):
                    ocel.to_pm4py()

    def test_to_sqlite_path_with_quote(self, ocel_db: Path, tmp_path: Path):
        out = tmp_path / "it's a test.sqlite"
        with Ocel.read(ocel_db) as ocel:
            ocel.to_sqlite(out)
        with Ocel.read(out) as exported:
            assert exported.summary().num_events == 3

    def test_repr(self, ocel_db: Path):
        with Ocel.read(ocel_db) as ocel:
            assert "test.sqlite" in repr(ocel)
