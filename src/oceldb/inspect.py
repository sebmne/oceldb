"""Manifest-based inspection helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from oceldb.ocel import OCEL


@dataclass(frozen=True)
class LogOverview:
    """High-level summary of an OCEL log."""

    event_count: int
    object_count: int
    e2o_count: int
    o2o_count: int
    time_min: str | None
    time_max: str | None
    event_types: list[str]
    object_types: list[str]

    def __str__(self) -> str:
        lines = [
            f"Events:        {self.event_count:>10,}  ({len(self.event_types)} types)",
            f"Objects:       {self.object_count:>10,}  ({len(self.object_types)} types)",
            f"E2O relations: {self.e2o_count:>10,}",
            f"O2O relations: {self.o2o_count:>10,}",
        ]
        if self.time_min:
            lines.append(f"Time range:    {self.time_min}  →  {self.time_max}")
        return "\n".join(lines)


@dataclass(frozen=True)
class EventTypeSummary:
    """Summary for one event type."""

    name: str
    count: int
    time_min: str | None
    time_max: str | None
    attributes: dict[str, str]

    def __str__(self) -> str:
        parts = [f"{self.name!r}  ({self.count:,} events)"]
        if self.attributes:
            attr_str = ", ".join(f"{k}: {v}" for k, v in self.attributes.items())
            parts.append(f"  attributes: {attr_str}")
        return "\n".join(parts)


@dataclass(frozen=True)
class ObjectTypeSummary:
    """Summary for one object type."""

    name: str
    object_count: int
    attributes: dict[str, str]

    def __str__(self) -> str:
        parts = [f"{self.name!r}  ({self.object_count:,} objects)"]
        if self.attributes:
            attr_str = ", ".join(f"{k}: {v}" for k, v in self.attributes.items())
            parts.append(f"  attributes: {attr_str}")
        return "\n".join(parts)


def overview(ocel: OCEL) -> LogOverview:
    """Return a high-level summary of *ocel*."""
    m = ocel.manifest
    time_range: list[str | None] = m.totals.get("time_range") or [None, None]
    return LogOverview(
        event_count=m.totals["event_count"],
        object_count=m.totals["object_count"],
        e2o_count=m.totals["e2o_count"],
        o2o_count=m.totals["o2o_count"],
        time_min=time_range[0],
        time_max=time_range[1],
        event_types=list(m.event_types),
        object_types=list(m.object_types),
    )


def event_types(ocel: OCEL) -> list[EventTypeSummary]:
    """Return event types sorted by descending count."""
    return sorted(
        [
            EventTypeSummary(
                name=name,
                count=info.count,
                time_min=info.time_range[0],
                time_max=info.time_range[1],
                attributes=dict(info.attributes),
            )
            for name, info in ocel.manifest.event_types.items()
        ],
        key=lambda s: s.count,
        reverse=True,
    )


def object_types(ocel: OCEL) -> list[ObjectTypeSummary]:
    """Return object types sorted by descending count."""
    return sorted(
        [
            ObjectTypeSummary(
                name=name,
                object_count=info.object_count,
                attributes=dict(info.attributes),
            )
            for name, info in ocel.manifest.object_types.items()
        ],
        key=lambda s: s.object_count,
        reverse=True,
    )
