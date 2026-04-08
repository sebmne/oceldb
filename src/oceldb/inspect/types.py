from __future__ import annotations

from typing import Dict, List

from oceldb.core.ocel import OCEL
from oceldb.dsl import asc, count, count_distinct, desc, id_, type_


def event_types(ocel: OCEL) -> List[str]:
    """
    Return the sorted list of event types present in the OCEL.
    """
    rows = (
        ocel.events()
        .table()
        .select(type_().as_("ocel_type"))
        .distinct()
        .order_by(asc("ocel_type"))
        .relation()
        .fetchall()
    )
    return [row[0] for row in rows]


def object_types(ocel: OCEL) -> List[str]:
    """
    Return the sorted list of object types present in the OCEL.

    Types are derived from the physical object-history table, but duplicates are
    removed through the analytical DSL.
    """
    rows = (
        ocel.objects()
        .table()
        .select(type_().as_("ocel_type"))
        .distinct()
        .order_by(asc("ocel_type"))
        .relation()
        .fetchall()
    )
    return [row[0] for row in rows]


def types(ocel: OCEL) -> Dict[str, List[str]]:
    """
    Return both event and object types.
    """
    return {
        "event": event_types(ocel),
        "object": object_types(ocel),
    }


def event_type_counts(ocel: OCEL) -> Dict[str, int]:
    """
    Return the number of events per event type.
    """
    rows = (
        ocel.events()
        .table()
        .select(type_().as_("event_type"))
        .group_by(type_())
        .agg(count().as_("event_count"))
        .order_by(desc("event_count"), asc("event_type"))
        .relation()
        .fetchall()
    )

    return {row[0]: int(row[1]) for row in rows}


def object_type_counts(ocel: OCEL) -> Dict[str, int]:
    """
    Return the number of logical objects per object type.

    Logical objects are counted via DISTINCT `ocel_id`, not via raw
    object-history row counts.
    """
    rows = (
        ocel.objects()
        .table()
        .select(type_().as_("object_type"))
        .group_by(type_())
        .agg(count_distinct(id_()).as_("object_count"))
        .order_by(desc("object_count"), asc("object_type"))
        .relation()
        .fetchall()
    )

    return {row[0]: int(row[1]) for row in rows}
