from __future__ import annotations

from datetime import datetime

import oceldb
import oceldb.dsl as dsl
import pytest

from oceldb.dsl import (
    abs_,
    coalesce,
    cooccurs_with,
    col,
    count,
    desc,
    has_event,
    has_object,
    linked,
    row_number,
    round_,
    when,
)


def test_event_root_count(ocel):
    assert ocel.query.events().count() == 5


def test_event_occurrences_root_exposes_object_timeline_rows(ocel):
    rows = (
        ocel.query
        .event_occurrences("order")
        .sort("ocel_object_id", "ocel_event_time", "ocel_event_id")
        .select(
            "ocel_object_id",
            "ocel_event_id",
            "ocel_event_type",
            "ocel_event_time",
        )
        .collect()
        .fetchall()
    )

    assert rows == [
        ("o1", "e1", "Create Order", datetime(2022, 1, 1, 10, 0)),
        ("o1", "e3", "Pay Order", datetime(2022, 1, 1, 12, 0)),
        ("o1", "e5", "Create Order", datetime(2022, 1, 1, 14, 0)),
        ("o2", "e2", "Create Order", datetime(2022, 1, 1, 11, 0)),
        ("o2", "e4", "Pay Order", datetime(2022, 1, 1, 13, 0)),
    ]


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


def test_expression_functions_and_operators_in_queries(ocel_with_object_lifecycle):
    rows = (
        ocel_with_object_lifecycle.query
        .object_states("order")
        .latest()
        .with_columns(
            normalized_status=coalesce(col("status"), "missing").str.upper(),
            created_year=col("ocel_time").dt.year(),
            lifecycle_bucket=(
                when(col("status") == "packed")
                .then("active")
                .when(col("status") == "shipped")
                .then("finished")
                .otherwise("missing")
            ),
        )
        .sort("ocel_id")
        .select("ocel_id", "normalized_status", "created_year", "lifecycle_bucket")
        .collect()
        .fetchall()
    )

    assert rows == [
        ("o1", "PACKED", 2022, "active"),
        ("o2", "SHIPPED", 2022, "finished"),
        ("o3", "MISSING", None, "missing"),
        ("o4", "MISSING", 2022, "missing"),
    ]


def test_window_functions_on_event_occurrences(ocel):
    rows = (
        ocel.query
        .event_occurrences("order")
        .with_columns(
            position=row_number().over(
                partition_by="ocel_object_id",
                order_by=("ocel_event_time", "ocel_event_id"),
            ),
            next_event_type=col("ocel_event_type").lead().over(
                partition_by="ocel_object_id",
                order_by=("ocel_event_time", "ocel_event_id"),
            ),
            previous_event_type=col("ocel_event_type").lag().over(
                partition_by="ocel_object_id",
                order_by=("ocel_event_time", "ocel_event_id"),
            ),
        )
        .sort("ocel_object_id", "position")
        .select(
            "ocel_object_id",
            "position",
            "previous_event_type",
            "ocel_event_type",
            "next_event_type",
        )
        .collect()
        .fetchall()
    )

    assert rows == [
        ("o1", 1, None, "Create Order", "Pay Order"),
        ("o1", 2, "Create Order", "Pay Order", "Create Order"),
        ("o1", 3, "Pay Order", "Create Order", None),
        ("o2", 1, None, "Create Order", "Pay Order"),
        ("o2", 2, "Create Order", "Pay Order", None),
    ]


def test_arithmetic_expressions_in_queries(ocel):
    rows = (
        ocel.query
        .events("Create Order")
        .with_columns(
            double_price=col("total_price").cast("DOUBLE") * 2,
            rounded_price=round_(col("total_price").cast("DOUBLE") / 3, 2),
            absolute_price=abs_(col("total_price").cast("DOUBLE") * -1),
        )
        .sort("ocel_id")
        .select("ocel_id", "double_price", "rounded_price", "absolute_price")
        .collect()
        .fetchall()
    )

    assert rows == [
        ("e1", 200.0, 33.33, 100.0),
        ("e2", 500.0, 83.33, 250.0),
        ("e5", 600.0, 100.0, 300.0),
    ]


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
        .where(cooccurs_with("customer").any(col("name") == "Alice"))
        .ids()
    )
    assert ids == ["o1"]


def test_string_predicate_namespace_in_where(ocel):
    ids = (
        ocel.query
        .events("Pay Order")
        .where(col("method").str.contains("credit"))
        .ids()
    )
    assert ids == ["e3"]


def test_where_rejects_window_expressions_directly(ocel):
    with pytest.raises(TypeError, match="window expressions directly"):
        (
            ocel.query
            .event_occurrences("order")
            .where(
                col("ocel_event_type")
                .lead()
                .over(
                    partition_by="ocel_object_id",
                    order_by=("ocel_event_time", "ocel_event_id"),
                )
                .not_null()
            )
        )


def test_group_by_rejects_window_expressions(ocel):
    with pytest.raises(TypeError, match="does not accept window expressions"):
        (
            ocel.query
            .event_occurrences("order")
            .group_by(
                row_number().over(
                    partition_by="ocel_object_id",
                    order_by=("ocel_event_time", "ocel_event_id"),
                )
            )
        )


def test_rename_preserves_row_query_capabilities(ocel):
    renamed = (
        ocel.query
        .events("Create Order")
        .rename(total_price="amount")
        .sort("ocel_id")
    )

    assert renamed.ids() == ["e1", "e2", "e5"]
    assert renamed.select("ocel_id", "amount").collect().fetchall() == [
        ("e1", 100.0),
        ("e2", 250.0),
        ("e5", 300.0),
    ]


def test_rename_rejects_duplicate_targets(ocel):
    with pytest.raises(ValueError, match="duplicate output columns"):
        ocel.query.events().rename({"total_price": "method"})


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
    assert not hasattr(ocel.query.objects("order"), "cooccurs_with")
    assert not hasattr(ocel.query.object_states("order").latest(), "linked")
    assert not hasattr(ocel.query.objects("order"), "has_event")
    assert not hasattr(ocel.query.events(), "has_object")


def test_public_modules_export_free_relation_builders():
    assert hasattr(oceldb, "cooccurs_with")
    assert hasattr(oceldb, "linked")
    assert hasattr(oceldb, "has_event")
    assert hasattr(oceldb, "has_object")
    assert hasattr(dsl, "cooccurs_with")
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


def test_object_state_queries_reject_attributes_from_other_object_types(ocel):
    with pytest.raises(ValueError, match="Unknown column 'name'"):
        (
            ocel.query
            .object_states("order")
            .latest()
            .where(col("name") == "Alice")
            .ids()
        )


def test_event_queries_reject_attributes_from_other_event_types(ocel):
    with pytest.raises(ValueError, match="Unknown column 'method'"):
        (
            ocel.query
            .events("Create Order")
            .where(col("method") == "credit_card")
            .ids()
        )


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


def test_object_states_batch_simultaneous_updates_per_timestamp(
    ocel_with_simultaneous_object_updates,
):
    rows = (
        ocel_with_simultaneous_object_updates.query
        .object_states("order")
        .as_of(datetime(2022, 1, 1, 9, 0))
        .select("ocel_id", "status", "priority")
        .collect()
        .fetchall()
    )
    assert rows == [("o1", "open", "high")]

    latest_rows = (
        ocel_with_simultaneous_object_updates.query
        .object_states("order")
        .latest()
        .select("ocel_id", "status", "priority")
        .collect()
        .fetchall()
    )
    assert latest_rows == [("o1", "done", "high")]


def test_object_states_sql_uses_batched_history_source(
    ocel_with_simultaneous_object_updates,
):
    sql = (
        ocel_with_simultaneous_object_updates.query
        .object_states("order")
        .latest()
        .to_sql()
    )

    assert 'GROUP BY c."ocel_id", c."ocel_time"' in sql


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


def test_linked_direction_controls(ocel_with_link_graph):
    package_query = ocel_with_link_graph.query.objects("package")

    assert package_query.where(linked("order").exists()).ids() == ["o2"]
    assert package_query.where(linked("order").incoming().exists()).ids() == ["o2"]
    assert package_query.where(linked("order").outgoing().exists()).ids() == []


def test_linked_max_hops_and_unbounded_reachability(ocel_with_link_graph):
    orders = ocel_with_link_graph.query.objects("order")

    assert orders.where(linked("customer").exists()).ids() == []
    assert orders.where(linked("customer").max_hops(2).exists()).ids() == []
    assert orders.where(linked("customer").max_hops(3).exists()).ids() == ["o1"]
    assert orders.where(linked("customer").max_hops(None).exists()).ids() == ["o1"]


def test_linked_unbounded_handles_cycles_without_duplicate_counts(ocel_with_link_graph):
    rows = (
        ocel_with_link_graph.query
        .objects("order")
        .where(col("ocel_id") == "o1")
        .with_columns(customer_count=linked("customer").max_hops(None).count())
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


def test_object_changes_reject_attributes_from_other_object_types(ocel_with_object_changes):
    with pytest.raises(ValueError, match="Unknown column 'name'"):
        (
            ocel_with_object_changes.query
            .object_changes("order")
            .where(col("name") == "Alice")
            .count()
        )


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
