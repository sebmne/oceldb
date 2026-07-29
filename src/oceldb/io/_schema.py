"""Declared OCEL exchange schemas and native dtype reconciliation."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any

import polars as pl

from oceldb.core import schema as s
from oceldb.io._errors import conversion_error


class AttributeType(str, Enum):
    """Scalar attribute types defined by the OCEL 2.0 specification."""

    STRING = "string"
    TIME = "time"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"

    @property
    def dtype(self) -> pl.DataType:
        """Return the closest canonical Polars dtype."""
        return {
            AttributeType.STRING: pl.String(),
            AttributeType.TIME: pl.Datetime("us", "UTC"),
            AttributeType.INTEGER: pl.Int64(),
            AttributeType.FLOAT: pl.Float64(),
            AttributeType.BOOLEAN: pl.Boolean(),
        }[self]

    @classmethod
    def parse(cls, value: object, *, context: str) -> AttributeType:
        """Parse one exact OCEL 2.0 attribute type name."""
        if not isinstance(value, str):
            raise conversion_error(context, "attribute type must be a string")
        try:
            return cls(value)
        except ValueError as exc:
            supported = ", ".join(member.value for member in cls)
            raise conversion_error(
                context,
                f"unsupported attribute type {value!r}; expected one of {supported}",
            ) from exc


TypeDeclarations = dict[str, dict[str, AttributeType]]


@dataclass(frozen=True)
class ExchangeSchema:
    """Validated declarations plus reconciled native attribute dtypes."""

    event_types: TypeDeclarations
    object_types: TypeDeclarations
    event_attributes: dict[str, pl.DataType]
    object_attributes: dict[str, pl.DataType]

    @classmethod
    def build(
        cls,
        event_types: TypeDeclarations,
        object_types: TypeDeclarations,
    ) -> ExchangeSchema:
        """Validate declarations and reconcile attributes shared by types."""
        _validate_declarations(event_types, "event")
        _validate_declarations(object_types, "object")
        return cls(
            event_types=event_types,
            object_types=object_types,
            event_attributes=_reconcile(event_types),
            object_attributes=_reconcile(object_types),
        )

    @property
    def event_schema(self) -> dict[str, pl.DataType]:
        """Return the staged event table schema."""
        return {**s.EVENT_SCHEMA, **self.event_attributes}

    @property
    def change_schema(self) -> dict[str, pl.DataType]:
        """Return the staged object-change table schema."""
        return {**s.CHANGE_SCHEMA, **self.object_attributes}


def declarations_from_json(
    values: Iterable[object],
    *,
    kind: str,
) -> TypeDeclarations:
    """Parse one JSON eventTypes or objectTypes collection."""
    declarations: TypeDeclarations = {}
    for index, raw in enumerate(values):
        context = f"{kind}Types[{index}]"
        item = _mapping(raw, context)
        name = required_string(item.get("name"), f"{context}.name")
        raw_attributes = item.get("attributes")
        if not isinstance(raw_attributes, list):
            raise conversion_error(f"{context}.attributes", "must be an array")
        attributes: dict[str, AttributeType] = {}
        for attribute_index, raw_attribute in enumerate(raw_attributes):
            attribute_context = f"{context}.attributes[{attribute_index}]"
            attribute = _mapping(raw_attribute, attribute_context)
            attribute_name = required_string(
                attribute.get("name"),
                f"{attribute_context}.name",
            )
            if attribute_name in attributes:
                raise conversion_error(
                    attribute_context,
                    f"duplicate attribute declaration {attribute_name!r}",
                )
            attributes[attribute_name] = AttributeType.parse(
                attribute.get("type"),
                context=f"{attribute_context}.type",
            )
        if name in declarations:
            raise conversion_error(context, f"duplicate type declaration {name!r}")
        declarations[name] = attributes
    return declarations


def required_string(value: object, context: str) -> str:
    """Require a non-empty string."""
    if not isinstance(value, str) or not value:
        raise conversion_error(context, "must be a non-empty string")
    return value


def require_declared_type(
    declarations: Mapping[str, Mapping[str, AttributeType]],
    type_name: str,
    *,
    context: str,
) -> Mapping[str, AttributeType]:
    """Return one type declaration or raise a contextualized error."""
    result = declarations.get(type_name)
    if result is None:
        raise conversion_error(context, f"undeclared type {type_name!r}")
    return result


def _mapping(value: object, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise conversion_error(context, "must be an object")
    return value


def _validate_declarations(declarations: TypeDeclarations, kind: str) -> None:
    for type_name, attributes in declarations.items():
        required_string(type_name, f"{kind} type")
        reserved = sorted(set(attributes) & s.RESERVED_COLUMNS)
        if reserved:
            raise conversion_error(
                f"{kind} type {type_name!r}",
                f"attribute names are reserved by oceldb: {reserved}",
            )


def _reconcile(
    declarations: Mapping[str, Mapping[str, AttributeType]],
) -> dict[str, pl.DataType]:
    by_name: dict[str, set[AttributeType]] = {}
    for attributes in declarations.values():
        for name, attribute_type in attributes.items():
            by_name.setdefault(name, set()).add(attribute_type)
    return {
        name: _common_dtype(attribute_types)
        for name, attribute_types in sorted(by_name.items())
    }


def _common_dtype(attribute_types: set[AttributeType]) -> pl.DataType:
    if len(attribute_types) == 1:
        return next(iter(attribute_types)).dtype
    if attribute_types <= {AttributeType.INTEGER, AttributeType.FLOAT}:
        return pl.Float64()
    # Native scans require a shared attribute name to have one dtype across
    # type partitions. String is the only lossless common representation for
    # all remaining mixed OCEL scalar types.
    return pl.String()
