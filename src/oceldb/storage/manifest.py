"""Manifest dataclass and I/O for the oceldb on-disk format."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from oceldb.storage.types import manifest_attributes

SUPPORTED_FORMAT_VERSION = "1"


class UnsupportedFormatVersionError(Exception):
    """Raised when a manifest declares a format version this library cannot handle."""


@dataclass(frozen=True)
class EventTypeInfo:
    """Metadata for a single event type stored in the log."""

    count: int
    time_range: tuple[str | None, str | None]
    attributes: dict[str, str]


@dataclass(frozen=True)
class ObjectTypeInfo:
    """Metadata for a single object type stored in the log."""

    object_count: int
    change_count: int
    attributes: dict[str, str]


@dataclass(frozen=True)
class Manifest:
    """Contents of a persisted or generated log manifest."""

    oceldb_format_version: str
    ocel_version: str
    created_at: str
    source: dict[str, Any]
    layout: str
    totals: dict[str, Any]
    event_types: dict[str, EventTypeInfo]
    object_types: dict[str, ObjectTypeInfo]

    @classmethod
    def load(cls, path: Path) -> Manifest:
        """Load and validate a manifest from *path*."""
        raw: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))

        version = raw.get("oceldb_format_version", "")
        if version != SUPPORTED_FORMAT_VERSION:
            raise UnsupportedFormatVersionError(
                f"Cannot read oceldb format version {version!r}; "
                f"only {SUPPORTED_FORMAT_VERSION!r} is supported."
            )

        event_types = {
            name: EventTypeInfo(
                count=info["count"],
                time_range=(info["time_range"][0], info["time_range"][1]),
                attributes=manifest_attributes(dict(info.get("attributes", {}))),
            )
            for name, info in raw["event_types"].items()
        }

        object_types = {
            name: ObjectTypeInfo(
                object_count=info["object_count"],
                change_count=info["change_count"],
                attributes=manifest_attributes(dict(info.get("attributes", {}))),
            )
            for name, info in raw["object_types"].items()
        }

        return cls(
            oceldb_format_version=raw["oceldb_format_version"],
            ocel_version=raw["ocel_version"],
            created_at=raw["created_at"],
            source=raw["source"],
            layout=raw["layout"],
            totals=raw["totals"],
            event_types=event_types,
            object_types=object_types,
        )

    def save(self, path: Path) -> None:
        """Serialize this manifest to *path* as UTF-8 JSON."""
        data: dict[str, Any] = {
            "oceldb_format_version": self.oceldb_format_version,
            "ocel_version": self.ocel_version,
            "created_at": self.created_at,
            "source": self.source,
            "layout": self.layout,
            "totals": self.totals,
            "event_types": {
                name: {
                    "count": info.count,
                    "time_range": list(info.time_range),
                    "attributes": manifest_attributes(info.attributes),
                }
                for name, info in self.event_types.items()
            },
            "object_types": {
                name: {
                    "object_count": info.object_count,
                    "change_count": info.change_count,
                    "attributes": manifest_attributes(info.attributes),
                }
                for name, info in self.object_types.items()
            },
        }
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
