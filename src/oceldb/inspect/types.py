from __future__ import annotations

from typing import Dict, List

from oceldb.core.ocel import OCEL


def event_types(ocel: OCEL) -> List[str]:
    return ocel.query.event_type_names()


def object_types(ocel: OCEL) -> List[str]:
    return ocel.query.object_type_names()


def types(ocel: OCEL) -> Dict[str, List[str]]:
    return {
        "event": event_types(ocel),
        "object": object_types(ocel),
    }


def event_type_counts(ocel: OCEL) -> Dict[str, int]:
    return ocel.query.event_type_counts()


def object_type_counts(ocel: OCEL) -> Dict[str, int]:
    return ocel.query.object_type_counts()
