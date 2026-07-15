"""Declared OCEL type and attribute metadata for IO boundaries.

OCEL 2.0 exchange formats carry schemas independently from their event and
object instances. Readers parse these declarations and seed the attribute
directory on :class:`oceldb.OCEL`; the native manifest stores them at rest.
That keeps lossless cross-format conversion possible, including for unused
types and attributes whose values are always null.
"""

from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from collections.abc import Mapping

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
    """Declared event and object types, including their attribute schemas.

    This is an IO-boundary record: exchange readers parse it, the native
    manifest stores it, and writers derive it from the data. It is never
    carried through transformations.
    """

    event_types: Mapping[str, TypeAttributes] = field(default_factory=dict)
    object_types: Mapping[str, TypeAttributes] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_types", _freeze_types(self.event_types))
        object.__setattr__(self, "object_types", _freeze_types(self.object_types))
