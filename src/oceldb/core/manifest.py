from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Mapping

LogicalTableName = Literal["event", "object", "object_change", "event_object", "object_object"]
QuerySourceKind = Literal[
    "event",
    "object",
    "object_state",
    "object_change",
    "event_object",
    "object_object",
]


@dataclass(frozen=True)
class TableSchema:
    """
    Schema description for one logical OCEL table.

    The schema is split into required core columns and user-facing custom
    columns discovered during conversion. Query validation and inspection use
    this metadata instead of inspecting parquet files on every operation.
    """

    name: LogicalTableName
    core_columns: Mapping[str, str]
    custom_columns: Mapping[str, str] = field(default_factory=lambda: {})

    @property
    def columns(self) -> dict[str, str]:
        return {
            **dict(self.core_columns),
            **dict(self.custom_columns),
        }


@dataclass(frozen=True)
class OCELManifest:
    """
    Immutable manifest describing one on-disk oceldb dataset.

    The manifest is the canonical contract between conversion, reading,
    validation, and materialization. It stores only stable storage metadata:
    source provenance, creation time, storage version, and logical table
    schemas.
    """

    oceldb_version: str
    storage_version: str
    source: str
    created_at: datetime
    tables: Mapping[LogicalTableName, TableSchema]

    def table(self, name: LogicalTableName) -> TableSchema:
        return self.tables[name]

    def with_source(self, source: str) -> "OCELManifest":
        return OCELManifest(
            oceldb_version=self.oceldb_version,
            storage_version=self.storage_version,
            source=source,
            created_at=self.created_at,
            tables=self.tables,
        )

    def with_created_at(self, created_at: datetime) -> "OCELManifest":
        return OCELManifest(
            oceldb_version=self.oceldb_version,
            storage_version=self.storage_version,
            source=self.source,
            created_at=created_at,
            tables=self.tables,
        )
