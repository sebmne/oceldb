from __future__ import annotations

import pytest

from oceldb.discovery import object_lifecycle, ocdfg, projected_dfg


def test_ocdfg_for_single_object_type(ocel):
    graph = ocdfg(ocel, "order")

    assert graph.object_types == ("order",)

    create = graph.node("Create Order")
    pay = graph.node("Pay Order")
    assert create.count == 3
    assert create.start_count == 2
    assert create.end_count == 1
    assert pay.count == 2
    assert pay.start_count == 0
    assert pay.end_count == 1

    create_to_pay = graph.edge("Create Order", "Pay Order")
    pay_to_create = graph.edge("Pay Order", "Create Order")
    assert create_to_pay.count == 2
    assert create_to_pay.mean_duration_seconds == 7200.0
    assert create_to_pay.median_duration_seconds == 7200.0
    assert pay_to_create.count == 1
    assert pay_to_create.mean_duration_seconds == 7200.0


def test_ocdfg_defaults_to_all_object_types(ocel):
    graph = ocdfg(ocel)

    assert graph.object_types == ("customer", "order")
    assert graph.node("Create Order").count == 4
    assert graph.node("Create Order").start_count == 3
    assert graph.node("Create Order").end_count == 2


def test_projected_dfg_matches_ocdfg(ocel):
    projected = projected_dfg(ocel, "order")
    classic = ocdfg(ocel, "order")

    assert projected == classic


def test_object_lifecycle_defaults_to_all_object_attributes(ocel_with_object_lifecycle):
    lifecycle = object_lifecycle(ocel_with_object_lifecycle, "order")

    assert lifecycle.attributes == ("priority", "status")
    assert lifecycle.object_count == 4
    assert lifecycle.objects_with_changes == 3
    assert lifecycle.objects_without_changes == 1
    assert lifecycle.objects_with_lifecycle == 2
    assert lifecycle.objects_without_lifecycle == 2
    assert lifecycle.avg_steps_per_object == 1.25
    assert lifecycle.median_steps_per_object == 1.0
    assert lifecycle.min_steps_per_object == 0
    assert lifecycle.max_steps_per_object == 3

    state_counts = {
        tuple(value for _, value in entry.state.attributes): entry.count
        for entry in lifecycle.states
    }
    assert state_counts == {
        ("low", "open"): 2,
        ("high", "open"): 1,
        ("low", "packed"): 1,
        ("low", "shipped"): 1,
    }

    start_counts = {
        tuple(value for _, value in entry.state.attributes): entry.count
        for entry in lifecycle.starts
    }
    assert start_counts == {
        ("high", "open"): 1,
        ("low", "open"): 1,
    }

    end_counts = {
        tuple(value for _, value in entry.state.attributes): entry.count
        for entry in lifecycle.ends
    }
    assert end_counts == {
        ("low", "packed"): 1,
        ("low", "shipped"): 1,
    }

    transitions = {
        (
            tuple(value for _, value in transition.source.attributes),
            tuple(value for _, value in transition.target.attributes),
        ): transition
        for transition in lifecycle.transitions
    }

    first = transitions[(("high", "open"), ("low", "open"))]
    assert first.changed_attributes == ("priority",)
    assert first.count == 1
    assert first.mean_duration_seconds == 3600.0

    second = transitions[(("low", "open"), ("low", "packed"))]
    assert second.changed_attributes == ("status",)
    assert second.mean_duration_seconds == 3600.0

    third = transitions[(("low", "open"), ("low", "shipped"))]
    assert third.changed_attributes == ("status",)
    assert third.mean_duration_seconds == 7200.0


def test_object_lifecycle_attribute_projection_collapses_repeated_states(
    ocel_with_object_lifecycle,
):
    lifecycle = object_lifecycle(
        ocel_with_object_lifecycle,
        "order",
        attributes=("status",),
    )

    assert lifecycle.attributes == ("status",)
    assert lifecycle.avg_steps_per_object == 1.0
    assert lifecycle.median_steps_per_object == 1.0
    assert lifecycle.min_steps_per_object == 0
    assert lifecycle.max_steps_per_object == 2

    state_counts = {
        entry.state.get("status"): entry.count
        for entry in lifecycle.states
    }
    assert state_counts == {
        "open": 2,
        "packed": 1,
        "shipped": 1,
    }

    transitions = {
        (
            transition.source.get("status"),
            transition.target.get("status"),
        ): transition
        for transition in lifecycle.transitions
    }
    assert transitions[("open", "packed")].mean_duration_seconds == 7200.0
    assert transitions[("open", "packed")].changed_attributes == ("status",)
    assert transitions[("open", "shipped")].mean_duration_seconds == 7200.0


def test_object_lifecycle_project_matches_direct_projection(ocel_with_object_lifecycle):
    lifecycle = object_lifecycle(ocel_with_object_lifecycle, "order")
    projected = lifecycle.project("status")
    direct = object_lifecycle(
        ocel_with_object_lifecycle,
        "order",
        attributes=("status",),
    )

    assert projected == direct


def test_object_lifecycle_state_lookup(ocel_with_object_lifecycle):
    lifecycle = object_lifecycle(ocel_with_object_lifecycle, "order")

    state = lifecycle.state(priority="low", status="open")

    assert state.count == 2
    assert state.state.get("priority") == "low"
    assert state.state.get("status") == "open"


def test_object_lifecycle_transition_lookup(ocel_with_object_lifecycle):
    lifecycle = object_lifecycle(
        ocel_with_object_lifecycle,
        "order",
        attributes=("status",),
    )

    transition = lifecycle.transition(
        source={"status": "open"},
        target={"status": "shipped"},
    )

    assert transition.count == 1
    assert transition.changed_attributes == ("status",)
    assert transition.mean_duration_seconds == 7200.0


def test_object_lifecycle_include_null_retains_null_only_states(ocel_with_object_lifecycle):
    lifecycle = object_lifecycle(
        ocel_with_object_lifecycle,
        "order",
        attributes=("status",),
        include_null=True,
    )

    assert lifecycle.objects_with_lifecycle == 3
    assert lifecycle.objects_without_lifecycle == 1

    state_counts = {
        entry.state.get("status"): entry.count
        for entry in lifecycle.states
    }
    assert state_counts[None] == 1


def test_object_lifecycle_rejects_unknown_attributes(ocel_with_object_lifecycle):
    with pytest.raises(ValueError, match="Unknown object attributes"):
        object_lifecycle(
            ocel_with_object_lifecycle,
            "order",
            attributes=("name",),
        )

    lifecycle = object_lifecycle(ocel_with_object_lifecycle, "order")
    with pytest.raises(ValueError, match="Unknown lifecycle attributes for projection"):
        lifecycle.project("name")


def test_object_lifecycle_rejects_conflicting_same_timestamp_updates(
    ocel_with_conflicting_lifecycle_changes,
):
    with pytest.raises(ValueError, match="deterministic post-timestamp states"):
        object_lifecycle(ocel_with_conflicting_lifecycle_changes, "order")


def test_object_lifecycle_batches_simultaneous_updates(
    ocel_with_simultaneous_object_updates,
):
    lifecycle = object_lifecycle(ocel_with_simultaneous_object_updates, "order")

    assert lifecycle.attributes == ("priority", "status")
    assert [
        entry.state.as_dict()
        for entry in lifecycle.states
    ] == [
        {"priority": "high", "status": "done"},
        {"priority": "high", "status": "open"},
    ]
