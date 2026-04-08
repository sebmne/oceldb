from __future__ import annotations

from typing import Dict, List

from oceldb.analysis.api import analyze
from oceldb.core.ocel import OCEL
from oceldb.dsl import asc, count, count_distinct, desc, id_, type_


def event_types(ocel: OCEL) -> List[str]:
    rows = (
        analyze(ocel)
        .events()
        .select(type_().as_("ocel_type"))
        .distinct()
        .order_by(asc("ocel_type"))
        .relation()
        .fetchall()
    )
    return [row[0] for row in rows]


def object_types(ocel: OCEL) -> List[str]:
    rows = (
        analyze(ocel)
        .objects()
        .select(type_().as_("ocel_type"))
        .distinct()
        .order_by(asc("ocel_type"))
        .relation()
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
        analyze(ocel)
        .events()
        .select(type_().as_("event_type"))
        .group_by(type_())
        .agg(count().as_("event_count"))
        .order_by(desc("event_count"), asc("event_type"))
        .relation()
        .fetchall()
    )

    return {row[0]: int(row[1]) for row in rows}


def object_type_counts(ocel: OCEL) -> Dict[str, int]:
    rows = (
        analyze(ocel)
        .objects()
        .select(type_().as_("object_type"))
        .group_by(type_())
        .agg(count_distinct(id_()).as_("object_count"))
        .order_by(desc("object_count"), asc("object_type"))
        .relation()
        .fetchall()
    )

    return {row[0]: int(row[1]) for row in rows}
