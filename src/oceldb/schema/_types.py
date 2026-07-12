"""Declared OCEL type and attribute metadata.

OCEL 2.0 exchange formats carry schemas independently from their event and
object instances.  Keeping that information on :class:`oceldb.OCEL` is what
makes lossless cross-format conversion possible, including for unused types and
attributes whose values are always null.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from collections.abc import Iterable, Mapping

import polars as pl


class AttributeType(str, Enum):
    """The five attribute types defined by OCEL 2.0."""

    STRING = "string"
    TIME = "time"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"

    @classmethod
    def parse(cls, value: str) -> "AttributeType":
        """Parse JSON/XML or SQLite spelling into a canonical type."""
        normalized = value.strip().lower()
        aliases = {
            "text": cls.STRING,
            "varchar": cls.STRING,
            "timestamp": cls.TIME,
            "datetime": cls.TIME,
            "int": cls.INTEGER,
            "bigint": cls.INTEGER,
            "real": cls.FLOAT,
            "double": cls.FLOAT,
            "numeric": cls.FLOAT,
            "bool": cls.BOOLEAN,
        }
        if normalized in aliases:
            return aliases[normalized]
        try:
            return cls(normalized)
        except ValueError as exc:
            raise ValueError(f"Unsupported OCEL attribute type: {value!r}") from exc

    @classmethod
    def from_polars(cls, dtype: pl.DataType) -> "AttributeType":
        """Map a Polars dtype to the closest lossless OCEL type."""
        if isinstance(dtype, (pl.Datetime, pl.Date)):
            return cls.TIME
        if isinstance(dtype, pl.Boolean):
            return cls.BOOLEAN
        if dtype.is_integer():
            return cls.INTEGER
        if isinstance(dtype, (pl.Float32, pl.Float64, pl.Decimal)):
            return cls.FLOAT
        return cls.STRING

    def polars_dtype(self) -> pl.DataType:
        """Return the canonical Polars representation for this type."""
        return {
            AttributeType.STRING: pl.String(),
            AttributeType.TIME: pl.Datetime("us", "UTC"),
            AttributeType.INTEGER: pl.Int64(),
            AttributeType.FLOAT: pl.Float64(),
            AttributeType.BOOLEAN: pl.Boolean(),
        }[self]

    def sqlite_type(self) -> str:
        """Return the OCEL 2.0 SQLite declaration for this type."""
        return {
            AttributeType.STRING: "TEXT",
            AttributeType.TIME: "TIMESTAMP",
            AttributeType.INTEGER: "INTEGER",
            AttributeType.FLOAT: "REAL",
            AttributeType.BOOLEAN: "BOOLEAN",
        }[self]


TypeAttributes = Mapping[str, AttributeType]


def _freeze_types(
    values: Mapping[str, Mapping[str, AttributeType | str]],
) -> Mapping[str, TypeAttributes]:
    return MappingProxyType(
        {
            str(type_name): MappingProxyType(
                {
                    str(name): value
                    if isinstance(value, AttributeType)
                    else AttributeType.parse(value)
                    for name, value in attributes.items()
                }
            )
            for type_name, attributes in values.items()
        }
    )


@dataclass(frozen=True)
class OCELSchema:
    """Declared event and object types, including their attribute schemas."""

    event_types: Mapping[str, TypeAttributes] = field(default_factory=dict)
    object_types: Mapping[str, TypeAttributes] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_types", _freeze_types(self.event_types))
        object.__setattr__(self, "object_types", _freeze_types(self.object_types))

    @classmethod
    def empty(cls) -> "OCELSchema":
        return cls()

    def rename(
        self,
        *,
        events: Mapping[str, str] | None = None,
        objects: Mapping[str, str] | None = None,
    ) -> "OCELSchema":
        """Return a schema with type names remapped consistently."""
        return OCELSchema(
            event_types=_rename_types(self.event_types, events),
            object_types=_rename_types(self.object_types, objects),
        )

    @classmethod
    def merge(cls, *schemas: "OCELSchema") -> "OCELSchema":
        """Union schemas, widening conflicting declarations to strings."""
        return cls(
            event_types=_merge_types(schema.event_types for schema in schemas),
            object_types=_merge_types(schema.object_types for schema in schemas),
        )


def _rename_types(
    types: Mapping[str, TypeAttributes], mapping: Mapping[str, str] | None
) -> dict[str, dict[str, AttributeType]]:
    renamed: dict[str, dict[str, AttributeType]] = {}
    for old_name, attributes in types.items():
        name = mapping.get(old_name, old_name) if mapping else old_name
        current = renamed.setdefault(name, {})
        for attribute, attr_type in attributes.items():
            previous = current.get(attribute)
            current[attribute] = _widen(previous, attr_type)
    return renamed


def _merge_types(
    groups: Iterable[Mapping[str, TypeAttributes]],
) -> dict[str, dict[str, AttributeType]]:
    merged: dict[str, dict[str, AttributeType]] = {}
    for types in groups:
        for type_name, attributes in types.items():
            current = merged.setdefault(type_name, {})
            for attribute, attr_type in attributes.items():
                current[attribute] = _widen(current.get(attribute), attr_type)
    return merged


def _widen(previous: AttributeType | None, current: AttributeType) -> AttributeType:
    if previous is None or previous == current:
        return current
    if {previous, current} <= {AttributeType.INTEGER, AttributeType.FLOAT}:
        return AttributeType.FLOAT
    return AttributeType.STRING
