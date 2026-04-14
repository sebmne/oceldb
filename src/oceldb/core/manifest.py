from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Mapping

LogicalTableName = Literal["event", "object", "object_change", "event_object", "object_object"]
QuerySourceKind = Literal[
    "event",
    "object",
    "event_occurrence",
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
    columns discovered during conversion. For wide event and object-history
    tables, `type_attributes` records which custom attributes logically belong
    to which OCEL type.

    Query validation, inspection, and discovery use this metadata instead of
    inferring attribute ownership from sparse parquet rows.
    """

    name: LogicalTableName
    core_columns: Mapping[str, str]
    custom_columns: Mapping[str, str] = field(default_factory=lambda: {})
    type_attributes: Mapping[str, tuple[str, ...]] = field(default_factory=lambda: {})

    @property
    def columns(self) -> dict[str, str]:
        return {
            **dict(self.core_columns),
            **dict(self.custom_columns),
        }

    def attributes_for_type(self, type_name: str) -> tuple[str, ...]:
        return tuple(self.type_attributes.get(type_name, ()))

    def custom_columns_for_types(
        self,
        type_names: Iterable[str],
    ) -> dict[str, str]:
        selected = {
            attribute
            for type_name in type_names
            for attribute in self.attributes_for_type(type_name)
        }
        return {
            name: sql_type
            for name, sql_type in self.custom_columns.items()
            if name in selected
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
