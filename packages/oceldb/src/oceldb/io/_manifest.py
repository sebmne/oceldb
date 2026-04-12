from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from oceldb.core.metadata import LogicalTableName, OCELManifest, TableSchema

MANIFEST_FILE = "manifest.json"
STORAGE_VERSION = "1"
LOGICAL_TABLES: tuple[LogicalTableName, ...] = (
    "event",
    "object",
    "event_object",
    "object_object",
)
CORE_COLUMNS: dict[LogicalTableName, dict[str, str]] = {
    "event": {
        "ocel_id": "VARCHAR",
        "ocel_type": "VARCHAR",
        "ocel_time": "TIMESTAMP",
    },
    "object": {
        "ocel_id": "VARCHAR",
        "ocel_type": "VARCHAR",
        "ocel_time": "TIMESTAMP",
        "ocel_changed_field": "VARCHAR",
    },
    "event_object": {
        "ocel_event_id": "VARCHAR",
        "ocel_object_id": "VARCHAR",
    },
    "object_object": {
        "ocel_source_id": "VARCHAR",
        "ocel_target_id": "VARCHAR",
    },
}


def load_manifest(path: Path) -> OCELManifest:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid manifest.json in '{path.parent}': {e}") from e

    required = {
        "format",
        "storage_version",
        "oceldb_version",
        "source",
        "created_at",
        "packaging",
        "tables",
    }
    missing = required - raw.keys()
    if missing:
        raise ValueError(
            f"manifest.json in '{path.parent}' is missing required keys: "
            f"{', '.join(sorted(missing))}"
        )

    if raw["format"] != "oceldb":
        raise ValueError(
            f"Unsupported manifest format in '{path.parent}': {raw['format']!r}"
        )

    try:
        created_at = datetime.fromisoformat(raw["created_at"])
    except Exception as e:
        raise ValueError(
            f"Invalid created_at value in manifest.json: {raw['created_at']!r}"
        ) from e

    raw_tables = raw["tables"]
    tables = {
        name: TableSchema(
            name=name,
            core_columns=CORE_COLUMNS[name],
            custom_columns=dict(raw_tables.get(name, {}).get("custom_columns", {})),
        )
        for name in LOGICAL_TABLES
    }

    return OCELManifest(
        oceldb_version=str(raw["oceldb_version"]),
        storage_version=str(raw["storage_version"]),
        source=str(raw["source"]),
        created_at=created_at,
        packaging=str(raw["packaging"]),
        tables=tables,
    )


def write_manifest(path: Path, manifest: OCELManifest) -> None:
    raw = {
        "format": "oceldb",
        "storage_version": manifest.storage_version,
        "oceldb_version": manifest.oceldb_version,
        "source": manifest.source,
        "created_at": manifest.created_at.isoformat(),
        "packaging": manifest.packaging,
        "tables": {
            name: {
                "custom_columns": dict(schema.custom_columns),
            }
            for name, schema in manifest.tables.items()
        },
    }

    path.write_text(json.dumps(raw, indent=2), encoding="utf-8")
