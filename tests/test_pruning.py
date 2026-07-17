"""Hive partition pruning of native datasets through the query optimizer."""

from oceldb import OCEL
from oceldb.operations.filters import filter_events_by_type


def _events_partitions(plan: str) -> list[str]:
    return [
        line
        for line in plan.splitlines()
        if "events/ocel_type=" in line or "events\\ocel_type=" in line
    ]


def test_typed_scan_prunes_to_one_partition(native_ocel: OCEL) -> None:
    plan = native_ocel.events("Load").explain()
    assert "ocel_type=Load" in plan
    assert "ocel_type=Pay" not in plan


def test_pruning_survives_chained_filter(native_ocel: OCEL) -> None:
    sub = filter_events_by_type(native_ocel, "Load")
    plan = sub.events("Load").explain()
    partitions = _events_partitions(plan)
    assert partitions, "expected a parquet scan of the events table"
    assert all("ocel_type=Pay" not in line for line in partitions)


def test_url_encoded_type_names_roundtrip(tmp_path) -> None:
    from conftest import build_ocel
    from oceldb.operations.transformations import rename_types

    renamed = rename_types(
        build_ocel(),
        events={"Load": "load container/goods"},
        objects={"Container": "shipping container"},
    )
    target = tmp_path / "encoded"
    renamed.write(target)
    reopened = OCEL.open(target)
    assert "load container/goods" in reopened.describe().event_types
    loads = reopened.events("load container/goods").collect()
    assert loads.height == 2
    assert "weight" in loads.columns
