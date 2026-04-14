from __future__ import annotations

from datetime import datetime

from oceldb.inspect import (
    attributes,
    event_object_stats,
    event_types,
    object_object_stats,
    object_types,
    overview,
    table_counts,
    time_range,
)


def test_overview(ocel):
    result = overview(ocel)

    assert result.event_count == 5
    assert result.object_count == 3
    assert result.event_type_count == 2
    assert result.object_type_count == 2
    assert result.earliest_event_time < result.latest_event_time


def test_types_and_attributes(ocel):
    assert event_types(ocel) == ["Create Order", "Pay Order"]
    assert object_types(ocel) == ["customer", "order"]

    attrs = attributes(ocel)
    assert attrs["event"]["Create Order"] == ["total_price"]
    assert attrs["event"]["Pay Order"] == ["method"]
    assert attrs["object"]["order"] == ["status"]
    assert attrs["object"]["customer"] == ["name"]


def test_event_object_stats(ocel):
    stats = event_object_stats(ocel)

    assert stats.max_objects_per_event == 2
    assert stats.max_events_per_object == 3


def test_object_object_stats(ocel):
    stats = object_object_stats(ocel)

    assert stats.edge_count == 2
    assert stats.source_object_count == 2
    assert stats.target_object_count == 1
    assert stats.linked_object_count == 3
    assert stats.avg_outgoing_links_per_source_object == 1.0
    assert stats.max_incoming_links_per_target_object == 2


def test_object_object_stats_handles_empty_relation_table(ocel_with_stateless_object):
    stats = object_object_stats(ocel_with_stateless_object)

    assert stats.edge_count == 0
    assert stats.source_object_count == 0
    assert stats.target_object_count == 0
    assert stats.linked_object_count == 0
    assert stats.avg_outgoing_links_per_source_object is None
    assert stats.avg_incoming_links_per_target_object is None


def test_table_counts_and_time_range(ocel):
    counts = table_counts(ocel)
    window = time_range(ocel)

    assert counts.event_count == 5
    assert counts.object_count == 3
    assert counts.object_change_count == 3
    assert counts.event_object_count == 6
    assert counts.object_object_count == 2
    assert window.earliest_time == datetime(2022, 1, 1, 9, 0)
    assert window.latest_time == datetime(2022, 1, 1, 14, 0)
    assert window.earliest_event_time == datetime(2022, 1, 1, 10, 0)
    assert window.latest_event_time == datetime(2022, 1, 1, 14, 0)
    assert window.earliest_object_change_time == datetime(2022, 1, 1, 9, 0)
    assert window.latest_object_change_time == datetime(2022, 1, 1, 11, 0)


def test_overview_counts_logical_objects(ocel_with_object_changes):
    result = overview(ocel_with_object_changes)

    assert result.event_count == 2
    assert result.object_count == 2
