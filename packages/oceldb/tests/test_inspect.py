from __future__ import annotations


def test_overview(ocel):
    result = ocel.inspect.overview()

    assert result.event_count == 5
    assert result.object_count == 3
    assert result.event_type_count == 2
    assert result.object_type_count == 2
    assert result.earliest_event_time < result.latest_event_time


def test_types_and_attributes(ocel):
    assert ocel.inspect.event_types() == ["Create Order", "Pay Order"]
    assert ocel.inspect.object_types() == ["customer", "order"]

    attrs = ocel.inspect.attributes()
    assert attrs["event"]["Create Order"] == ["total_price"]
    assert attrs["event"]["Pay Order"] == ["method"]
    assert attrs["object"]["order"] == ["status"]
    assert attrs["object"]["customer"] == ["name"]


def test_event_object_stats(ocel):
    stats = ocel.inspect.event_object_stats()

    assert stats.max_objects_per_event == 2
    assert stats.max_events_per_object == 3
