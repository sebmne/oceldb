"""Shared synthetic logs for the oceldb test suite."""

from datetime import datetime, timezone

import polars as pl
import pytest

from oceldb import OCEL

UTC_DT = pl.Datetime("us", "UTC")


def _ts(day: int) -> datetime:
    return datetime(2024, 1, day, tzinfo=timezone.utc)


def build_ocel() -> OCEL:
    """A small two-event-type, two-object-type log with typed attributes.

    ``Load`` events carry a float ``weight``; ``Pay`` events carry an integer
    ``amount``. ``Container`` objects have a string ``status`` history;
    ``Order`` objects have an integer ``total``.
    """
    events = pl.LazyFrame(
        {
            "ocel_id": ["e1", "e2", "e3", "e4"],
            "ocel_time": pl.Series([_ts(1), _ts(2), _ts(3), _ts(4)], dtype=UTC_DT),
            "ocel_type": ["Load", "Load", "Pay", "Pay"],
            "weight": [1.5, 2.5, None, None],
            "amount": pl.Series([None, None, 10, 20], dtype=pl.Int64),
        }
    )
    objects = pl.LazyFrame(
        {
            "ocel_id": ["c1", "c2", "o1"],
            "ocel_type": ["Container", "Container", "Order"],
        }
    )
    object_changes = pl.LazyFrame(
        {
            "ocel_id": ["c1", "c1", "o1"],
            "ocel_time": pl.Series([_ts(1), _ts(2), _ts(1)], dtype=UTC_DT),
            "ocel_type": ["Container", "Container", "Order"],
            "ocel_changed_field": ["status", "status", "total"],
            "status": ["packed", "shipped", None],
            "total": pl.Series([None, None, 30], dtype=pl.Int64),
        }
    )
    event_object = pl.LazyFrame(
        {
            "ocel_event_id": ["e1", "e2", "e3", "e4"],
            "ocel_event_type": ["Load", "Load", "Pay", "Pay"],
            "ocel_object_id": ["c1", "c2", "o1", "o1"],
            "ocel_object_type": ["Container", "Container", "Order", "Order"],
            "ocel_qualifier": ["loads", "loads", "pays", "pays"],
        }
    )
    object_object = pl.LazyFrame(
        {
            "ocel_source_id": ["c1"],
            "ocel_source_type": ["Container"],
            "ocel_target_id": ["o1"],
            "ocel_target_type": ["Order"],
            "ocel_qualifier": ["belongs to"],
        }
    )
    return OCEL.from_frames(
        events=events,
        objects=objects,
        object_changes=object_changes,
        event_object=event_object,
        object_object=object_object,
    )


@pytest.fixture
def ocel() -> OCEL:
    return build_ocel()


@pytest.fixture
def native_ocel(ocel: OCEL, tmp_path) -> OCEL:
    """The synthetic log written to and re-opened from native storage."""
    target = tmp_path / "native"
    ocel.write(target)
    return OCEL.open(target)


def collect_sorted(frame: pl.LazyFrame, *keys: str) -> pl.DataFrame:
    df = frame.collect()
    return df.sort(list(keys) if keys else df.columns).select(sorted(df.columns))


def assert_same_log(left: OCEL, right: OCEL) -> None:
    """Assert both logs contain identical rows in all five tables."""
    assert collect_sorted(left.events(), "ocel_id").equals(
        collect_sorted(right.events(), "ocel_id")
    )
    assert collect_sorted(left.objects(), "ocel_id").equals(
        collect_sorted(right.objects(), "ocel_id")
    )
    assert collect_sorted(left.object_changes()).equals(
        collect_sorted(right.object_changes())
    )
    assert collect_sorted(left.event_object()).equals(
        collect_sorted(right.event_object())
    )
    assert collect_sorted(left.object_object()).equals(
        collect_sorted(right.object_object())
    )
