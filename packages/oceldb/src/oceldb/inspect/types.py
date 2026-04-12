from __future__ import annotations

from typing import Dict, List

from oceldb.core.ocel import OCEL
from oceldb.dsl import asc, col, count, desc


def event_types(ocel: OCEL) -> List[str]:
    rows = (
        ocel.query()
        .events()
        .select(col("ocel_type"))
        .unique()
        .sort(asc("ocel_type"))
        .collect()
        .fetchall()
    )
    return [row[0] for row in rows]


def object_types(ocel: OCEL) -> List[str]:
    rows = (
        ocel.query()
        .objects()
        .select(col("ocel_type"))
        .unique()
        .sort(asc("ocel_type"))
        .collect()
        .fetchall()
    )
    return [row[0] for row in rows]


def types(ocel: OCEL) -> Dict[str, List[str]]:
    return {
        "event": event_types(ocel),
        "object": object_types(ocel),
    }


def event_type_counts(ocel: OCEL) -> Dict[str, int]:
    rows = (
        ocel.query()
        .events()
        .group_by("ocel_type")
        .agg(count().alias("event_count"))
        .sort(desc("event_count"), asc("ocel_type"))
        .collect()
        .fetchall()
    )

    return {row[0]: int(row[1]) for row in rows}


def object_type_counts(ocel: OCEL) -> Dict[str, int]:
    rows = (
        ocel.query()
        .objects()
        .group_by("ocel_type")
        .agg(count().alias("object_count"))
        .sort(desc("object_count"), asc("ocel_type"))
        .collect()
        .fetchall()
    )

    return {row[0]: int(row[1]) for row in rows}
