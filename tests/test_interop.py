from __future__ import annotations

import sys
from types import ModuleType

import pytest

from oceldb.interop import to_pm4py


def test_to_pm4py_requires_optional_dependency(ocel, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setitem(sys.modules, "pandas", None)
    monkeypatch.setitem(sys.modules, "pm4py", None)

    with pytest.raises(ImportError, match="oceldb\\[pm4py\\]"):
        to_pm4py(ocel)


def _install_fake_pm4py(monkeypatch: pytest.MonkeyPatch):
    class FakeDataFrame:
        def __init__(self, rows, columns):
            self.rows = list(rows)
            self.columns = list(columns)

        def to_records(self):
            return [
                dict(zip(self.columns, row, strict=False))
                for row in self.rows
            ]

    class FakePandas(ModuleType):
        def DataFrame(self, rows, columns):
            return FakeDataFrame(rows, columns)

    class FakePM4PyOCEL:
        def __init__(
            self,
            events=None,
            objects=None,
            relations=None,
            globals=None,
            parameters=None,
            o2o=None,
            e2e=None,
            object_changes=None,
        ):
            self.events = events
            self.objects = objects
            self.relations = relations
            self.globals = globals
            self.parameters = parameters
            self.o2o = o2o
            self.e2e = e2e
            self.object_changes = object_changes

    pm4py_module = ModuleType("pm4py")
    pm4py_objects = ModuleType("pm4py.objects")
    pm4py_ocel = ModuleType("pm4py.objects.ocel")
    pm4py_ocel_obj = ModuleType("pm4py.objects.ocel.obj")
    pm4py_ocel_obj.OCEL = FakePM4PyOCEL
    pm4py_module.objects = pm4py_objects
    pm4py_objects.ocel = pm4py_ocel
    pm4py_ocel.obj = pm4py_ocel_obj

    monkeypatch.setitem(sys.modules, "pandas", FakePandas("pandas"))
    monkeypatch.setitem(sys.modules, "pm4py", pm4py_module)
    monkeypatch.setitem(sys.modules, "pm4py.objects", pm4py_objects)
    monkeypatch.setitem(sys.modules, "pm4py.objects.ocel", pm4py_ocel)
    monkeypatch.setitem(sys.modules, "pm4py.objects.ocel.obj", pm4py_ocel_obj)

    return FakePM4PyOCEL


def test_to_pm4py_builds_pm4py_ocel_like_object(ocel, monkeypatch: pytest.MonkeyPatch):
    FakePM4PyOCEL = _install_fake_pm4py(monkeypatch)

    result = to_pm4py(ocel)

    assert isinstance(result, FakePM4PyOCEL)
    assert result.globals == {
        "oceldb:path": str(ocel.path),
        "oceldb:source": ocel.manifest.source,
        "oceldb:storage_version": ocel.manifest.storage_version,
    }

    event_rows = result.events.to_records()
    assert event_rows[0]["ocel:eid"] == "e1"
    assert event_rows[0]["ocel:activity"] == "Create Order"
    assert "total_price" in event_rows[0]

    object_rows = result.objects.to_records()
    assert object_rows[0]["ocel:oid"] == "o1"
    assert object_rows[0]["ocel:type"] == "order"
    assert object_rows[0]["status"] == "open"
    assert object_rows[0]["name"] is None
    assert object_rows[2]["ocel:oid"] == "o3"
    assert object_rows[2]["name"] == "Alice"

    relation_rows = result.relations.to_records()
    assert relation_rows[0]["ocel:eid"] == "e1"
    assert relation_rows[0]["ocel:oid"] == "o1"
    assert relation_rows[0]["ocel:qualifier"] is None

    o2o_rows = result.o2o.to_records()
    assert o2o_rows == [
        {"ocel:oid": "o1", "ocel:oid_2": "o3", "ocel:qualifier": None},
        {"ocel:oid": "o2", "ocel:oid_2": "o3", "ocel:qualifier": None},
    ]

    object_change_rows = result.object_changes.to_records()
    assert object_change_rows == []


def test_to_pm4py_splits_initial_object_state_from_later_updates(
    ocel_with_object_changes,
    monkeypatch: pytest.MonkeyPatch,
):
    _install_fake_pm4py(monkeypatch)

    result = to_pm4py(ocel_with_object_changes)

    object_rows = result.objects.to_records()
    assert object_rows[0]["ocel:oid"] == "o1"
    assert object_rows[0]["status"] == "open"
    assert object_rows[1]["ocel:oid"] == "o2"
    assert object_rows[1]["name"] == "Alice"

    object_change_rows = result.object_changes.to_records()
    assert len(object_change_rows) == 2
    assert object_change_rows[0]["ocel:oid"] == "o1"
    assert object_change_rows[0]["ocel:type"] == "order"
    assert object_change_rows[0]["ocel:field"] == "status"
    assert object_change_rows[0]["status"] == "closed"
    assert object_change_rows[0]["name"] is None
    assert object_change_rows[1]["ocel:oid"] == "o2"
    assert object_change_rows[1]["ocel:type"] == "customer"
    assert object_change_rows[1]["ocel:field"] == "name"
    assert object_change_rows[1]["status"] is None
    assert object_change_rows[1]["name"] == "Alice B."
