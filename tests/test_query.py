from __future__ import annotations

from datetime import datetime

import oceldb
import oceldb.dsl as dsl
import pytest

from oceldb.dsl import col, count, desc, has_event, has_object, linked, related


def test_event_root_count(ocel):
    assert ocel.query.events().count() == 5


def test_object_type_filter_and_ids(ocel):
    assert sorted(ocel.query.objects("order").ids()) == ["o1", "o2"]


def test_with_columns_and_select(ocel):
    rows = (
        ocel.query
        .object_states("order")
        .latest()
        .with_columns(status_copy=col("status"))
        .select("ocel_id", "status_copy")
        .sort("ocel_id")
        .collect()
        .fetchall()
    )
    assert rows == [("o1", "open"), ("o2", "closed")]


def test_group_by_and_agg(ocel):
    rows = (
        ocel.query
        .events()
        .group_by("ocel_type")
        .agg(count().alias("n"))
        .having(col("n") > 1)
        .sort(desc("n"), "ocel_type")
        .collect()
        .fetchall()
    )
    assert rows[0] == ("Create Order", 3)
    assert rows[1] == ("Pay Order", 2)


def test_relation_predicates(ocel):
    orders = ocel.query.object_states("order").latest()
    ids = (
        orders
        .where(related("customer").any(col("name") == "Alice"))
        .ids()
    )
    assert ids == ["o1"]


def test_has_event_predicate(ocel):
    orders = ocel.query.objects("order")
    ids = (
        orders
        .where(has_event("Pay Order").exists())
        .sort("ocel_id")
        .ids()
    )
    assert ids == ["o1", "o2"]


def test_query_objects_do_not_expose_bound_relation_builders(ocel):
    assert not hasattr(ocel.query.objects("order"), "related")
    assert not hasattr(ocel.query.object_states("order").latest(), "linked")
    assert not hasattr(ocel.query.objects("order"), "has_event")
    assert not hasattr(ocel.query.events(), "has_object")


def test_public_modules_export_free_relation_builders():
    assert hasattr(oceldb, "related")
    assert hasattr(oceldb, "linked")
    assert hasattr(oceldb, "has_event")
    assert hasattr(oceldb, "has_object")
    assert hasattr(dsl, "related")
    assert hasattr(dsl, "linked")
    assert hasattr(dsl, "has_event")
    assert hasattr(dsl, "has_object")


def test_has_object_predicate_uses_event_time_object_state(ocel_with_object_changes):
    rows = (
        ocel_with_object_changes.query
        .events()
        .where(has_object("order").any(col("status") == "open"))
        .sort("ocel_id")
        .select("ocel_id")
        .collect()
        .fetchall()
    )
    assert rows == [("e1",)]


def test_relation_root_query(ocel):
    rows = (
        ocel.query
        .event_objects()
        .group_by("ocel_object_id")
        .agg(count().alias("n"))
        .sort(desc("n"))
        .collect()
        .fetchall()
    )
    assert rows[0] == ("o1", 3)


def test_objects_query_uses_logical_object_identities(ocel_with_object_changes):
    assert ocel_with_object_changes.query.objects().count() == 2

    rows = (
        ocel_with_object_changes.query
        .objects()
        .sort("ocel_id")
        .select("ocel_id", "ocel_type")
        .collect()
        .fetchall()
    )
    assert rows == [
        ("o1", "order"),
        ("o2", "customer"),
    ]


def test_objects_reject_dynamic_object_attribute_filters(ocel):
    with pytest.raises(ValueError, match="Unknown column 'status'"):
        ocel.query.objects("order").where(col("status") == "open").ids()


def test_object_states_require_explicit_temporal_projection(ocel):
    with pytest.raises(AttributeError, match="count"):
        ocel.query.object_states("order").count()

    seed = ocel.query.object_states("order")
    assert hasattr(seed, "latest")
    assert hasattr(seed, "as_of")
    assert not hasattr(seed, "where")


def test_object_states_latest_uses_latest_non_null_attribute_values(ocel_with_object_changes):
    ids = (
        ocel_with_object_changes.query
        .object_states("order")
        .latest()
        .where(col("status") == "closed")
        .ids()
    )
    assert ids == ["o1"]

    rows = (
        ocel_with_object_changes.query
        .object_states()
        .latest()
        .sort("ocel_id")
        .select("ocel_id", "ocel_time", "status", "name")
        .collect()
        .fetchall()
    )
    assert rows == [
        ("o1", datetime(2022, 1, 2, 12, 0), "closed", None),
        ("o2", datetime(2022, 1, 4, 8, 0), None, "Alice B."),
    ]


def test_object_states_as_of_reconstructs_temporal_state(ocel_with_object_changes):
    rows = (
        ocel_with_object_changes.query
        .object_states()
        .as_of(datetime(2022, 1, 2, 0, 0))
        .sort("ocel_id")
        .select("ocel_id", "status", "name")
        .collect()
        .fetchall()
    )
    assert rows == [
        ("o1", "open", None),
        ("o2", None, "Alice"),
    ]


def test_relation_counts_do_not_duplicate_object_change_rows(ocel_with_object_changes):
    orders = ocel_with_object_changes.query.objects("order")
    rows = (
        orders
        .with_columns(customer_count=linked("customer").count())
        .select("ocel_id", "customer_count")
        .collect()
        .fetchall()
    )
    assert rows == [("o1", 1)]


def test_object_changes_query_exposes_raw_history_rows(ocel_with_object_changes):
    assert ocel_with_object_changes.query.object_changes().count() == 4

    rows = (
        ocel_with_object_changes.query
        .object_changes("order")
        .sort("ocel_time")
        .select("ocel_id", "ocel_time", "ocel_changed_field", "status")
        .collect()
        .fetchall()
    )
    assert rows == [
        ("o1", datetime(2022, 1, 1, 10, 0), None, "open"),
        ("o1", datetime(2022, 1, 2, 12, 0), "status", "closed"),
    ]


def test_selected_rows_drop_identity_and_materialization_helpers(ocel):
    selected = ocel.query.events().select("ocel_id")

    assert not hasattr(selected, "ids")
    assert not hasattr(selected, "to_ocel")
    assert not hasattr(selected, "write")


def test_where_rejects_aggregate_predicates(ocel):
    with pytest.raises(TypeError, match="where\\(\\.\\.\\.\\) does not accept aggregate expressions"):
        ocel.query.events().where(count() == 1)


def test_where_rejects_non_boolean_expressions(ocel):
    with pytest.raises(TypeError, match="where\\(\\.\\.\\.\\) only accepts boolean expressions"):
        ocel.query.events().where(col("ocel_id"))


def test_row_level_expression_methods_reject_aggregate_expressions(ocel):
    with pytest.raises(TypeError, match="select\\(\\.\\.\\.\\) does not accept aggregate expressions"):
        ocel.query.events().select(count())

    with pytest.raises(TypeError, match="with_columns\\(\\.\\.\\.\\) does not accept aggregate expressions"):
        ocel.query.events().with_columns(n=count())

    with pytest.raises(TypeError, match="group_by\\(\\.\\.\\.\\) does not accept aggregate expressions"):
        ocel.query.events().group_by(count())


def test_agg_rejects_non_aggregate_expressions(ocel):
    with pytest.raises(TypeError, match="agg\\(\\.\\.\\.\\) only accepts aggregate expressions"):
        ocel.query.events().group_by("ocel_type").agg(col("ocel_id"))


def test_having_rejects_aggregate_expressions(ocel):
    with pytest.raises(TypeError, match="having\\(\\.\\.\\.\\) does not accept aggregate expressions directly"):
        (
            ocel.query.events()
            .group_by("ocel_type")
            .agg(count().alias("n"))
            .having(count() > 1)
        )


def test_object_states_keep_objects_without_history_rows(ocel_with_stateless_object):
    rows = (
        ocel_with_stateless_object.query
        .object_states("order")
        .latest()
        .sort("ocel_id")
        .select("ocel_id", "ocel_time", "status")
        .collect()
        .fetchall()
    )
    assert rows == [
        ("o1", datetime(2022, 1, 1, 10, 0), "open"),
        ("o2", None, None),
    ]
