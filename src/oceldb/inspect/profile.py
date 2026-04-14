from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from oceldb.core.ocel import OCEL


@dataclass(frozen=True)
class TableCounts:
    event_count: int
    object_count: int
    object_change_count: int
    event_object_count: int
    object_object_count: int


@dataclass(frozen=True)
class TimeRange:
    earliest_time: datetime | None
    latest_time: datetime | None
    earliest_event_time: datetime | None
    latest_event_time: datetime | None
    earliest_object_change_time: datetime | None
    latest_object_change_time: datetime | None


def table_counts(ocel: OCEL) -> TableCounts:
    row = ocel.sql("""
        SELECT
            (SELECT COUNT(*) FROM "event") AS "event_count",
            (SELECT COUNT(*) FROM "object") AS "object_count",
            (SELECT COUNT(*) FROM "object_change") AS "object_change_count",
            (SELECT COUNT(*) FROM "event_object") AS "event_object_count",
            (SELECT COUNT(*) FROM "object_object") AS "object_object_count"
    """).fetchone()

    if row is None:
        raise RuntimeError("table_counts() returned no row")

    return TableCounts(
        event_count=int(row[0]),
        object_count=int(row[1]),
        object_change_count=int(row[2]),
        event_object_count=int(row[3]),
        object_object_count=int(row[4]),
    )


def time_range(ocel: OCEL) -> TimeRange:
    row = ocel.sql("""
        WITH event_range AS (
            SELECT
                MIN("ocel_time") AS "earliest_event_time",
                MAX("ocel_time") AS "latest_event_time"
            FROM "event"
        ),
        object_change_range AS (
            SELECT
                MIN("ocel_time") AS "earliest_object_change_time",
                MAX("ocel_time") AS "latest_object_change_time"
            FROM "object_change"
        )
        SELECT
            CASE
                WHEN er."earliest_event_time" IS NULL THEN ocr."earliest_object_change_time"
                WHEN ocr."earliest_object_change_time" IS NULL THEN er."earliest_event_time"
                ELSE LEAST(er."earliest_event_time", ocr."earliest_object_change_time")
            END AS "earliest_time",
            CASE
                WHEN er."latest_event_time" IS NULL THEN ocr."latest_object_change_time"
                WHEN ocr."latest_object_change_time" IS NULL THEN er."latest_event_time"
                ELSE GREATEST(er."latest_event_time", ocr."latest_object_change_time")
            END AS "latest_time",
            er."earliest_event_time",
            er."latest_event_time",
            ocr."earliest_object_change_time",
            ocr."latest_object_change_time"
        FROM event_range er
        CROSS JOIN object_change_range ocr
    """).fetchone()

    if row is None:
        raise RuntimeError("time_range() returned no row")

    return TimeRange(
        earliest_time=row[0],
        latest_time=row[1],
        earliest_event_time=row[2],
        latest_event_time=row[3],
        earliest_object_change_time=row[4],
        latest_object_change_time=row[5],
    )
