"""Typed accessors return only the selected types' attribute columns."""

from oceldb import OCEL
from oceldb.operations.filters import filter_events_by_type


def test_probe_narrows_events(ocel: OCEL) -> None:
    assert ocel.events("Load").collect_schema().names() == [
        "ocel_id",
        "ocel_time",
        "weight",
        "ocel_type",
    ]
    assert ocel.events("Pay").collect_schema().names() == [
        "ocel_id",
        "ocel_time",
        "amount",
        "ocel_type",
    ]
    assert set(ocel.events("Load", "Pay").collect_schema().names()) == {
        "ocel_id",
        "ocel_time",
        "weight",
        "amount",
        "ocel_type",
    }


def test_probe_narrows_object_changes(ocel: OCEL) -> None:
    names = ocel.object_changes("Container").collect_schema().names()
    assert "status" in names
    assert "total" not in names
    names = ocel.object_changes("Order").collect_schema().names()
    assert "total" in names
    assert "status" not in names


def test_untyped_accessors_return_union(ocel: OCEL) -> None:
    names = ocel.events().collect_schema().names()
    assert {"weight", "amount"} <= set(names)


def test_declared_narrowing_after_open(native_ocel: OCEL) -> None:
    assert native_ocel._presence is not None
    names = native_ocel.events("Load").collect_schema().names()
    assert "weight" in names
    assert "amount" not in names
    names = native_ocel.object_changes("Order").collect_schema().names()
    assert "total" in names
    assert "status" not in names


def test_filtered_log_reprobes(ocel: OCEL) -> None:
    sub = filter_events_by_type(ocel, "Load")
    assert sub._presence is None
    names = sub.events("Load").collect_schema().names()
    assert "weight" in names
    # Pay events no longer exist, so their attribute is not narrowed in.
    assert sub.events("Pay").collect_schema().names() == [
        "ocel_id",
        "ocel_time",
        "ocel_type",
    ]
    assert sub.events("Pay").collect().height == 0


def test_typed_selection_rows(ocel: OCEL) -> None:
    loads = ocel.events("Load").collect()
    assert loads.get_column("ocel_id").to_list() == ["e1", "e2"]
    assert loads.get_column("weight").to_list() == [1.5, 2.5]
