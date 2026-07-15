"""Filters, transformations, merging, and write behavior."""

import polars as pl
import pytest

from conftest import assert_same_log, build_ocel
from oceldb import OCEL, OCELValidationError
from oceldb.filters import (
    filter_events_by_type,
    filter_objects_by_type,
    filter_objects_by_event_count,
)
from oceldb.transformations import flatten, project, rename_types, view


def test_filter_events_prunes_connected_core(ocel: OCEL) -> None:
    sub = filter_events_by_type(ocel, "Load")
    assert list(sub.describe().event_types) == ["Load"]
    # Pay-only objects and their changes are gone; containers remain.
    assert set(sub.objects().collect().get_column("ocel_id")) == {"c1", "c2"}
    assert set(sub.object_changes().collect().get_column("ocel_id")) == {"c1"}
    # O2O edges to removed objects are pruned.
    assert sub.object_object().collect().height == 0
    sub.validate()


def test_filter_objects_by_type(ocel: OCEL) -> None:
    sub = filter_objects_by_type(ocel, "Order")
    assert set(sub.describe().object_types) == {"Order"}
    assert list(sub.describe().event_types) == ["Pay"]
    sub.validate()


def test_filter_objects_by_event_count(ocel: OCEL) -> None:
    sub = filter_objects_by_event_count(ocel, min_count=2)
    assert set(sub.objects().collect().get_column("ocel_id")) == {"o1"}


def test_view_composes_type_filters(ocel: OCEL) -> None:
    sub = view(ocel, event_types="Load", object_types="Container")
    assert list(sub.describe().event_types) == ["Load"]
    assert set(sub.describe().object_types) == {"Container"}


def test_project_keeps_connected_component(ocel: OCEL) -> None:
    sub = project(ocel, "c1")
    assert sub.events().collect().get_column("ocel_id").to_list() == ["e1"]


def test_rename_types_updates_all_tables_and_narrowing(ocel: OCEL) -> None:
    renamed = rename_types(ocel, events={"Load": "Move"}, objects={"Order": "Sale"})
    assert set(renamed.describe().event_types) == {"Move", "Pay"}
    assert set(renamed.describe().object_types) == {"Container", "Sale"}
    e2o = renamed.event_object().collect()
    assert set(e2o.get_column("ocel_event_type")) == {"Move", "Pay"}
    assert set(e2o.get_column("ocel_object_type")) == {"Container", "Sale"}
    o2o = renamed.object_object().collect()
    assert o2o.get_column("ocel_target_type").to_list() == ["Sale"]
    # The attribute directory is recomputed against the new names.
    assert "weight" in renamed.events("Move").collect_schema().names()
    renamed.validate()


def test_merge_deduplicates(ocel: OCEL) -> None:
    merged = OCEL.merge(ocel, filter_events_by_type(ocel, "Load"))
    assert_same_log(ocel, merged)


def test_materialize_keeps_rows_and_narrowing(ocel: OCEL) -> None:
    sub = view(ocel, event_types="Load").materialize()
    assert list(sub.describe().event_types) == ["Load"]
    assert "weight" in sub.events("Load").collect_schema().names()


def test_flatten(ocel: OCEL) -> None:
    log = flatten(ocel, "Container").collect()
    assert log.height == 2
    assert "case:concept:name" in log.columns
    with pytest.raises(ValueError, match="Unknown object type"):
        flatten(ocel, "Nope")


def test_object_states_forward_fill(ocel: OCEL) -> None:
    states = ocel.object_states("Container").collect()
    assert states.get_column("status").to_list() == ["packed", "shipped"]


def test_validate_rejects_dangling_references(ocel: OCEL) -> None:
    broken = ocel._replace(
        event_object=ocel.event_object().with_columns(
            pl.lit("missing").alias("ocel_object_id")
        )
    )
    with pytest.raises(OCELValidationError):
        broken.validate()


def test_write_fast_copies_pristine_source(native_ocel: OCEL, tmp_path) -> None:
    assert native_ocel._source is not None
    target = tmp_path / "copy"
    native_ocel.write(target)
    reopened = OCEL.open(target)
    assert_same_log(native_ocel, reopened)
    assert (target / "manifest.json").read_text() == (
        native_ocel._source / "manifest.json"
    ).read_text()


def test_transformations_drop_the_source_pointer(native_ocel: OCEL) -> None:
    assert filter_events_by_type(native_ocel, "Load")._source is None
    assert rename_types(native_ocel, events={"Load": "Move"})._source is None
    assert native_ocel.materialize()._source is None


def test_filtered_write_drops_removed_types(native_ocel: OCEL, tmp_path) -> None:
    sub = filter_objects_by_type(native_ocel, "Container")
    target = tmp_path / "filtered"
    sub.write(target)
    reopened = OCEL.open(target)
    assert set(reopened.describe().object_types) == {"Container"}
    assert not (target / "objects" / "ocel_type=Order").exists()
    reopened.validate()


def test_sql_over_logical_tables(ocel: OCEL) -> None:
    result = ocel.sql("SELECT count(*) AS n FROM events")
    assert result.get_column("n").to_list() == [4]


def test_pipeline_syntax(ocel: OCEL) -> None:
    sub = ocel >> view(event_types="Load") >> filter_events_by_type("Load")
    assert list(sub.describe().event_types) == ["Load"]


def test_merge_requires_input() -> None:
    with pytest.raises(ValueError):
        OCEL.merge()


def test_empty_log_roundtrip(tmp_path) -> None:
    empty = build_ocel()._replace(
        events=build_ocel().events().filter(pl.lit(False)),
        objects=build_ocel().objects().filter(pl.lit(False)),
        object_changes=build_ocel().object_changes().filter(pl.lit(False)),
        event_object=build_ocel().event_object().filter(pl.lit(False)),
        object_object=build_ocel().object_object().filter(pl.lit(False)),
    )
    target = tmp_path / "empty"
    empty.write(target)
    reopened = OCEL.open(target)
    assert reopened.describe().events == 0
