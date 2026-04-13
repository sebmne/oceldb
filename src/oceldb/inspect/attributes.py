from __future__ import annotations

from typing import Dict, List, Literal

from oceldb.core.ocel import OCEL
from oceldb.inspect.types import event_types, object_types


def event_attributes(ocel: OCEL, event_type: str) -> List[str]:
    return _type_attributes(ocel, table_name="event", type_name=event_type)


def object_attributes(ocel: OCEL, object_type: str) -> List[str]:
    return _type_attributes(ocel, table_name="object_change", type_name=object_type)


def attributes(ocel: OCEL) -> Dict[str, Dict[str, List[str]]]:
    return {
        "event": {
            name: event_attributes(ocel, name)
            for name in event_types(ocel)
        },
        "object": {
            name: object_attributes(ocel, name)
            for name in object_types(ocel)
        },
    }


def _type_attributes(
    ocel: OCEL,
    *,
    table_name: Literal["event", "object_change"],
    type_name: str,
) -> List[str]:
    custom_columns = sorted(ocel.manifest.table(table_name).custom_columns)
    escaped_type = type_name.replace("'", "''")

    present: list[str] = []
    for column_name in custom_columns:
        escaped_column = column_name.replace('"', '""')
        row = ocel.sql(f"""
            SELECT EXISTS(
                SELECT 1
                FROM "{table_name}"
                WHERE "ocel_type" = '{escaped_type}'
                  AND "{escaped_column}" IS NOT NULL
            )
        """).fetchone()
        if row and bool(row[0]):
            present.append(column_name)

    return present
