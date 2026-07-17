"""The performance contract of the native scan layer.

Opened logs must prune type predicates to the matching partition files,
narrow typed accessors to the columns each type actually stores, execute a
write's source plan exactly once, and fail fast on malformed layouts.
"""

import polars as pl
import pytest

from conftest import build_ocel
from oceldb import OCEL


@pytest.fixture
def opened(tmp_path) -> OCEL:
    target = tmp_path / "native"
    build_ocel().write(target)
    return OCEL.open(target)


def test_typed_scan_prunes_to_matching_partitions(opened: OCEL) -> None:
    plan = opened.events("Load").explain()
    assert plan.count("Parquet SCAN") == 1
    assert "ocel_type=Load" in plan
    assert "ocel_type=Pay" not in plan


def test_opened_accessors_narrow_to_the_types_columns(opened: OCEL) -> None:
    assert opened.events("Load").collect_schema().names() == [
        "ocel_id",
        "ocel_time",
        "weight",
        "ocel_type",
    ]
    assert opened.events("Pay").collect_schema().names() == [
        "ocel_id",
        "ocel_time",
        "amount",
        "ocel_type",
    ]
    assert set(opened.events("Load", "Pay").collect_schema().names()) == {
        "ocel_id",
        "ocel_time",
        "weight",
        "amount",
        "ocel_type",
    }
    changes = opened.object_changes("Container").collect_schema().names()
    assert "status" in changes
    assert "total" not in changes


def test_untyped_accessors_return_the_full_union(opened: OCEL) -> None:
    assert {"weight", "amount"} <= set(opened.events().collect_schema().names())


def test_manual_ocel_accessors_filter_without_narrowing() -> None:
    ocel = build_ocel()
    names = ocel.events("Load").collect_schema().names()
    assert {"weight", "amount"} <= set(names)
    assert ocel.events("Load").collect().get_column("ocel_id").to_list() == [
        "e1",
        "e2",
    ]


def test_write_executes_the_source_plan_once(tmp_path) -> None:
    calls = 0

    def count(batch: pl.DataFrame) -> pl.DataFrame:
        nonlocal calls
        calls += 1
        return batch

    ocel = build_ocel()
    counted = OCEL(
        events=ocel.events().map_batches(count, streamable=False),
        objects=ocel.objects(),
        object_changes=ocel.object_changes(),
        e2o=ocel.e2o(),
        o2o=ocel.o2o(),
    )
    counted.write(tmp_path / "once")
    assert calls == 1


def test_write_leaves_no_spill_files(opened: OCEL, tmp_path) -> None:
    target = tmp_path / "clean"
    opened.write(target)
    assert not list(target.rglob(".spill*"))


def test_open_rejects_wrong_core_dtype(tmp_path) -> None:
    target = tmp_path / "native"
    build_ocel().write(target)
    partition = target / "events" / "ocel_type=Load" / "data.parquet"
    broken = pl.read_parquet(partition).with_columns(
        pl.col("ocel_time").cast(pl.String)
    )
    broken.write_parquet(partition)
    with pytest.raises(ValueError, match="ocel_time"):
        OCEL.open(target)


def test_open_rejects_conflicting_shared_attribute_dtypes(tmp_path) -> None:
    target = tmp_path / "native"
    build_ocel().write(target)
    partition = target / "events" / "ocel_type=Load" / "data.parquet"
    conflicting = pl.read_parquet(partition).rename({"weight": "amount"})
    conflicting.write_parquet(partition)
    with pytest.raises(ValueError, match="amount"):
        OCEL.open(target)
