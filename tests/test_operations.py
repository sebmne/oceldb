from datetime import datetime, timezone

import polars as pl

from oceldb import OCEL
from oceldb.operations import (
    filter_e2o_by_qualifier,
    filter_events_by_attribute,
    filter_events_by_id,
    filter_events_by_object_count,
    filter_events_by_time,
    filter_events_by_type,
    filter_o2o_by_qualifier,
    filter_objects_by_attribute,
    filter_objects_by_event_count,
    filter_objects_by_id,
    filter_objects_by_o2o_count,
    filter_objects_by_type,
    sample_events,
    sample_objects,
    flatten,
    project,
    rename_types,
    view,
)


def _ids(frame: pl.LazyFrame) -> set[str]:
    return set(frame.collect().get_column("ocel_id"))


def test_event_filters_prune_the_connected_core(ocel: OCEL) -> None:
    loads = filter_events_by_type(ocel, "Load")
    assert _ids(loads.events()) == {"e1", "e2"}
    assert _ids(loads.objects()) == {"c1", "c2"}
    assert _ids(loads.object_changes()) == {"c1"}
    assert loads.o2o().collect().is_empty()

    heavy = filter_events_by_attribute(ocel, pl.col("weight") > 2)
    assert _ids(heavy.events()) == {"e2"}
    assert _ids(heavy.objects()) == {"c2"}

    recent = filter_events_by_time(
        ocel, start=datetime(2024, 1, 3, tzinfo=timezone.utc)
    )
    assert _ids(recent.events()) == {"e3", "e4"}
    assert _ids(recent.objects()) == {"o1"}

    assert _ids(filter_events_by_id(ocel, "e1").events()) == {"e1"}
    assert _ids(filter_events_by_object_count(ocel, min_count=1).events()) == {
        "e1",
        "e2",
        "e3",
        "e4",
    }
    assert sample_events(ocel, n=1, seed=1).events().collect().height == 1


def test_object_filters_prune_events_changes_and_relations(ocel: OCEL) -> None:
    orders = filter_objects_by_type(ocel, "Order")
    assert _ids(orders.objects()) == {"o1"}
    assert _ids(orders.events()) == {"e3", "e4"}

    frequent = filter_objects_by_event_count(ocel, min_count=2)
    assert _ids(frequent.objects()) == {"o1"}

    outgoing = filter_objects_by_o2o_count(ocel, min_count=1, direction="out")
    assert _ids(outgoing.objects()) == {"c1"}

    shipped = filter_objects_by_attribute(
        ocel,
        pl.col("status") == "shipped",
        object_types="Container",
        when="sometimes",
    )
    assert _ids(shipped.objects()) == {"c1", "o1"}
    assert _ids(filter_objects_by_id(ocel, "c2").objects()) == {"c2"}
    assert sample_objects(ocel, n=1, seed=1).objects().collect().height == 1


def test_relation_filters_use_the_short_accessors(ocel: OCEL) -> None:
    pays = filter_e2o_by_qualifier(ocel, "pays")
    assert _ids(pays.events()) == {"e3", "e4"}
    assert _ids(pays.objects()) == {"o1"}

    without_edge = filter_o2o_by_qualifier(ocel, "belongs to", mode="exclude")
    assert without_edge.o2o().collect().is_empty()
    assert _ids(without_edge.objects()) == {"c1", "c2", "o1"}


def test_transformations_and_pipeline_use_operations_namespace(ocel: OCEL) -> None:
    selected = ocel >> view(event_types="Load") >> project("c1")
    assert _ids(selected.events()) == {"e1"}

    renamed = rename_types(ocel, events={"Load": "Move"}, objects={"Order": "Sale"})
    assert set(renamed.e2o().collect().get_column("ocel_event_type")) == {
        "Move",
        "Pay",
    }
    assert renamed.o2o().collect().get_column("ocel_target_type").to_list() == ["Sale"]

    flattened = flatten(ocel, "Container").collect()
    assert flattened.height == 2
    assert "case:concept:name" in flattened.columns
