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
    return sorted(ocel.manifest.table(table_name).attributes_for_type(type_name))
