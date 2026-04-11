from __future__ import annotations

from dataclasses import asdict

from oceldb import OCEL


def get_overview_payload(ocel: OCEL) -> dict:
    return {
        "overview": asdict(ocel.inspect.overview()),
        "event_type_counts": ocel.inspect.event_type_counts(),
        "object_type_counts": ocel.inspect.object_type_counts(),
    }
