from __future__ import annotations

from typing import Literal

ScopeKind = Literal[
    "event",
    "object",
    "event_occurrence",
    "object_state",
    "object_state_at_event",
    "object_change",
    "event_object",
    "object_object",
    "grouped",
]

RESERVED_COLUMNS: frozenset[str] = frozenset({"ocel_id", "ocel_type", "ocel_time"})


def is_materializable(kind: ScopeKind) -> bool:
    return kind in {"event", "object", "object_state"}
