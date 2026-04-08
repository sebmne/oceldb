from __future__ import annotations

from typing import Dict, List

from oceldb.core.ocel import OCEL
from oceldb.inspect.types import event_types, object_types


def event_attributes(ocel: OCEL, event_type: str) -> List[str]:
    """
    Return the sorted list of custom attributes used by a given event type.

    This currently uses SQL directly because JSON key discovery is not yet
    modeled naturally in the analytical DSL.
    """
    escaped = event_type.replace("'", "''")
    rows = ocel.sql(f"""
        SELECT DISTINCT UNNEST(json_keys(attributes::JSON))
        FROM {ocel.schema}.event
        WHERE ocel_type = '{escaped}'
          AND attributes IS NOT NULL
        ORDER BY 1
    """).fetchall()
    return [row[0] for row in rows]


def object_attributes(ocel: OCEL, object_type: str) -> List[str]:
    """
    Return the sorted list of custom attributes used by a given object type.

    This currently uses SQL directly because JSON key discovery is not yet
    modeled naturally in the analytical DSL.
    """
    escaped = object_type.replace("'", "''")
    rows = ocel.sql(f"""
        SELECT DISTINCT UNNEST(json_keys(attributes::JSON))
        FROM {ocel.schema}.object
        WHERE ocel_type = '{escaped}'
          AND attributes IS NOT NULL
        ORDER BY 1
    """).fetchall()
    return [row[0] for row in rows]


def attributes(ocel: OCEL) -> Dict[str, Dict[str, List[str]]]:
    """
    Return all discovered custom attributes grouped by type.
    """
    return {
        "event": {
            event_type_name: event_attributes(ocel, event_type_name)
            for event_type_name in event_types(ocel)
        },
        "object": {
            object_type_name: object_attributes(ocel, object_type_name)
            for object_type_name in object_types(ocel)
        },
    }
