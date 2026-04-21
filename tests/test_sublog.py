"""Tests for ``Sublog`` cross-axis filter propagation and ``to_ocel()``.

Uses the ``ocel`` fixture whose shape is:
  events  e1..e5 ('Create Order' x3, 'Pay Order' x2)
  objects o1,o2 (order), o3 (customer)
  event_object:  e1->(o1,o3), e2->o2, e3->o1, e4->o2, e5->o1
"""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# event_types filter
# ---------------------------------------------------------------------------


def test_sublog_event_types_filters_events(ocel):
    ids = ocel.query.sublog(event_types=["Pay Order"]).events().ids()
    assert sorted(ids) == ["e3", "e4"]


def test_sublog_event_types_restricts_flatten(ocel):
    rows = (
        ocel.query.sublog(event_types=["Pay Order"])
        .flatten("order")
        .select("ocel_event_id", "ocel_object_id")
        .sort("ocel_event_id", "ocel_object_id")
        .collect()
        .fetchall()
    )
    assert rows == [("e3", "o1"), ("e4", "o2")]


def test_sublog_event_types_catalog_helpers(ocel):
    sub = ocel.query.sublog(event_types=["Pay Order"])
    assert sub.event_count() == 2
    assert sorted(sub.event_ids()) == ["e3", "e4"]
    assert sub.event_type_names() == ["Pay Order"]


# ---------------------------------------------------------------------------
# object_types filter + drop_orphan_events
# ---------------------------------------------------------------------------


def test_sublog_object_types_filters_objects(ocel):
    ids = ocel.query.sublog(object_types=["order"]).objects().ids()
    assert sorted(ids) == ["o1", "o2"]


def test_sublog_object_types_drops_orphan_events(ocel):
    # e1 touches o1 (order) and o3 (customer); restricting to 'order' still
    # keeps e1 because at least one of its objects survives. Add a second
    # sublog restricted to 'customer' — e2/e4 lose their sole object and
    # should be dropped from events() under drop_orphan_events=True.
    ids = ocel.query.sublog(object_types=["customer"]).events().ids()
    assert sorted(ids) == ["e1"]


def test_sublog_object_types_keeps_orphans_when_disabled(ocel):
    ids = (
        ocel.query.sublog(object_types=["customer"], drop_orphan_events=False)
        .events()
        .ids()
    )
    assert sorted(ids) == ["e1", "e2", "e3", "e4", "e5"]


# ---------------------------------------------------------------------------
# event_types filter restricts the surviving object set
# ---------------------------------------------------------------------------


def test_sublog_event_types_restricts_objects(ocel):
    # Only Pay Order events survive → e3 (→o1), e4 (→o2). Customer o3 isn't
    # touched by any Pay event so it should disappear from objects().
    ids = ocel.query.sublog(event_types=["Pay Order"]).objects().ids()
    assert sorted(ids) == ["o1", "o2"]


# ---------------------------------------------------------------------------
# Narrowing: sublog(...).sublog(...) intersects
# ---------------------------------------------------------------------------


def test_sublog_narrow_intersects_event_types(ocel):
    sub = (
        ocel.query
        .sublog(event_types=["Pay Order", "Create Order"])
        .sublog(event_types=["Pay Order"])
    )
    assert sorted(sub.events().ids()) == ["e3", "e4"]


def test_sublog_narrow_empty_overlap_raises(ocel):
    sub = ocel.query.sublog(event_types=["Pay Order"])
    with pytest.raises(ValueError, match="no overlap"):
        sub.sublog(event_types=["Create Order"])


def test_sublog_method_arg_must_be_subset_of_filter(ocel):
    sub = ocel.query.sublog(event_types=["Pay Order"])
    with pytest.raises(ValueError, match="not in the current sublog"):
        sub.events("Create Order")


def test_sublog_empty_filter_list_raises(ocel):
    with pytest.raises(ValueError, match="empty"):
        ocel.query.sublog(event_types=[])


# ---------------------------------------------------------------------------
# to_ocel() materialization
# ---------------------------------------------------------------------------


def test_sublog_to_ocel_event_types_round_trip(ocel):
    new = ocel.query.sublog(event_types=["Pay Order"]).to_ocel()
    try:
        assert sorted(new.query.events().ids()) == ["e3", "e4"]
        # Only objects touching surviving events (o1, o2 via e3, e4).
        assert sorted(new.query.objects().ids()) == ["o1", "o2"]
        # event_object rows are narrowed to surviving events & objects.
        rows = new.sql(
            'SELECT ocel_event_id, ocel_object_id FROM "event_object" '
            "ORDER BY ocel_event_id, ocel_object_id"
        ).fetchall()
        assert rows == [("e3", "o1"), ("e4", "o2")]
    finally:
        new.close()


def test_sublog_to_ocel_object_types_drops_orphans(ocel):
    # object_types=['customer'] with drop_orphan_events=True (default):
    # only e1 survives because only e1 touches o3.
    new = ocel.query.sublog(object_types=["customer"]).to_ocel()
    try:
        assert new.query.events().ids() == ["e1"]
        assert new.query.objects().ids() == ["o3"]
        rows = new.sql(
            'SELECT ocel_event_id, ocel_object_id FROM "event_object"'
        ).fetchall()
        assert rows == [("e1", "o3")]
    finally:
        new.close()


def test_sublog_to_ocel_object_types_keep_orphans(ocel):
    new = (
        ocel.query
        .sublog(object_types=["order"], drop_orphan_events=False)
        .to_ocel()
    )
    try:
        # All events retained; event_object only keeps links to surviving
        # 'order' objects (o1, o2). e1 kept its o1 link, lost its o3 link.
        assert sorted(new.query.events().ids()) == ["e1", "e2", "e3", "e4", "e5"]
        assert sorted(new.query.objects().ids()) == ["o1", "o2"]
        rows = new.sql(
            'SELECT ocel_event_id, ocel_object_id FROM "event_object" '
            "ORDER BY ocel_event_id, ocel_object_id"
        ).fetchall()
        assert rows == [
            ("e1", "o1"),
            ("e2", "o2"),
            ("e3", "o1"),
            ("e4", "o2"),
            ("e5", "o1"),
        ]
    finally:
        new.close()


def test_sublog_to_ocel_identity_copies_dataset(ocel):
    new = ocel.query.to_ocel()
    try:
        assert sorted(new.query.events().ids()) == ["e1", "e2", "e3", "e4", "e5"]
        assert sorted(new.query.objects().ids()) == ["o1", "o2", "o3"]
    finally:
        new.close()
