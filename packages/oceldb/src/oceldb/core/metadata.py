from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Mapping

PackagingKind = Literal["directory", "archive"]
LogicalTableName = Literal["event", "object", "event_object", "object_object"]


@dataclass(frozen=True)
class TableSchema:
    """
    Typed schema information for one logical OCEL table.
    """

    name: LogicalTableName
    core_columns: Mapping[str, str]
    custom_columns: Mapping[str, str] = field(default_factory=dict)

    @property
    def columns(self) -> dict[str, str]:
        return {
            **dict(self.core_columns),
            **dict(self.custom_columns),
        }


@dataclass(frozen=True)
class OCELMetadata:
    """
    Stable metadata about an OCEL dataset.
    """

    oceldb_version: str
    storage_version: str
    source: str
    created_at: datetime
    packaging: PackagingKind


@dataclass(frozen=True)
class OCELManifest:
    """
    Internal storage manifest used for IO and query validation.
    """

    oceldb_version: str
    storage_version: str
    source: str
    created_at: datetime
    packaging: PackagingKind
    tables: Mapping[LogicalTableName, TableSchema]

    @property
    def metadata(self) -> OCELMetadata:
        return OCELMetadata(
            oceldb_version=self.oceldb_version,
            storage_version=self.storage_version,
            source=self.source,
            created_at=self.created_at,
            packaging=self.packaging,
        )

    def table(self, name: LogicalTableName) -> TableSchema:
        return self.tables[name]

    def with_packaging(self, packaging: PackagingKind) -> "OCELManifest":
        return OCELManifest(
            oceldb_version=self.oceldb_version,
            storage_version=self.storage_version,
            source=self.source,
            created_at=self.created_at,
            packaging=packaging,
            tables=self.tables,
        )
