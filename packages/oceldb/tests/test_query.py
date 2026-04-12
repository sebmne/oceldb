from __future__ import annotations

from oceldb.dsl import col, count, desc, has_event, related


def test_event_root_count(ocel):
    assert ocel.query().events().count() == 5


def test_object_type_filter_and_ids(ocel):
    ids = (
        ocel.query()
        .objects("order")
        .filter(col("status") == "open")
        .ids()
    )
    assert ids == ["o1"]


def test_with_columns_and_select(ocel):
    rows = (
        ocel.query()
        .objects("order")
        .with_columns(status_copy=col("status"))
        .select("ocel_id", "status_copy")
        .sort("ocel_id")
        .collect()
        .fetchall()
    )
    assert rows == [("o1", "open"), ("o2", "closed")]


def test_group_by_and_agg(ocel):
    rows = (
        ocel.query()
        .events()
        .group_by("ocel_type")
        .agg(count().alias("n"))
        .sort(desc("n"), "ocel_type")
        .collect()
        .fetchall()
    )
    assert rows[0] == ("Create Order", 3)
    assert rows[1] == ("Pay Order", 2)


def test_relation_predicates(ocel):
    ids = (
        ocel.query()
        .objects("order")
        .filter(related("customer").any(col("name") == "Alice"))
        .ids()
    )
    assert ids == ["o1"]


def test_has_event_predicate(ocel):
    ids = (
        ocel.query()
        .objects("order")
        .filter(has_event("Pay Order").exists())
        .sort("ocel_id")
        .ids()
    )
    assert ids == ["o1", "o2"]


def test_relation_root_query(ocel):
    rows = (
        ocel.query()
        .event_objects()
        .group_by("ocel_object_id")
        .agg(count().alias("n"))
        .sort(desc("n"))
        .collect()
        .fetchall()
    )
    assert rows[0] == ("o1", 3)
