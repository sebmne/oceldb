"""Versioned manifest for native oceldb Parquet datasets."""

import json
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from collections.abc import Mapping
from typing import Any

from oceldb.io.native.layout import NativeStorageError, manifest_layout
from oceldb.schema import AttributeType, OCELSchema, TypeAttributes

MANIFEST_FILENAME = "manifest.json"
NATIVE_FORMAT = "oceldb"
NATIVE_FORMAT_VERSION = 1


class NativeManifestError(NativeStorageError):
    """A native manifest is malformed or uses an unsupported version."""


@dataclass(frozen=True)
class NativeManifest:
    """Stable metadata committed alongside a native Parquet dataset."""

    schema: OCELSchema
    metadata: Mapping[str, Any] = field(default_factory=dict)
    format_version: int = NATIVE_FORMAT_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": NATIVE_FORMAT,
            "formatVersion": self.format_version,
            "schema": {
                "eventTypes": _types_to_dict(self.schema.event_types),
                "objectTypes": _types_to_dict(self.schema.object_types),
            },
            "tables": manifest_layout(),
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, value: object) -> "NativeManifest":
        if not isinstance(value, dict):
            raise NativeManifestError("Native manifest must be a JSON object.")
        if value.get("format") != NATIVE_FORMAT:
            raise NativeManifestError(
                f"Unsupported native manifest format: {value.get('format')!r}."
            )
        version = value.get("formatVersion")
        if version != NATIVE_FORMAT_VERSION:
            raise NativeManifestError(
                f"Unsupported native format version {version!r}; "
                f"this library supports version {NATIVE_FORMAT_VERSION}."
            )
        if value.get("tables") != manifest_layout():
            raise NativeManifestError(
                "Native manifest table layout does not match format version 1."
            )
        schema_value = value.get("schema")
        if not isinstance(schema_value, dict):
            raise NativeManifestError("Native manifest schema must be an object.")
        metadata = value.get("metadata", {})
        if not isinstance(metadata, dict) or not all(
            isinstance(key, str) for key in metadata
        ):
            raise NativeManifestError("Native manifest metadata must be an object.")
        return cls(
            schema=OCELSchema(
                event_types=_parse_types(schema_value.get("eventTypes"), "event"),
                object_types=_parse_types(schema_value.get("objectTypes"), "object"),
            ),
            metadata=metadata,
            format_version=version,
        )


def read_manifest(path: str | Path) -> NativeManifest:
    """Read and validate the required native dataset manifest."""
    file = Path(path) / MANIFEST_FILENAME
    if not file.exists():
        raise NativeManifestError(f"Native dataset has no {MANIFEST_FILENAME}: {path}")
    try:
        value = json.loads(file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise NativeManifestError(f"Cannot read native manifest {file}: {exc}") from exc
    return NativeManifest.from_dict(value)


def write_manifest(
    path: str | Path,
    *,
    schema: OCELSchema,
    metadata: Mapping[str, Any] | None = None,
) -> None:
    """Write the commit manifest for an otherwise complete staged dataset."""
    file = Path(path) / MANIFEST_FILENAME
    document = NativeManifest(schema=schema, metadata=metadata or {}).to_dict()
    try:
        contents = json.dumps(document, ensure_ascii=False, indent=2) + "\n"
    except (TypeError, ValueError) as exc:
        raise NativeManifestError(
            f"Cannot write native manifest {file}: {exc}"
        ) from exc
    file.write_text(contents, encoding="utf-8")


def _types_to_dict(types: Mapping[str, TypeAttributes]) -> dict[str, Any]:
    return {
        type_name: {name: attr_type.value for name, attr_type in attributes.items()}
        for type_name, attributes in types.items()
    }


def _parse_types(value: object, kind: str) -> dict[str, dict[str, AttributeType]]:
    if not isinstance(value, dict):
        raise NativeManifestError(f"Native manifest {kind}Types must be an object.")
    result: dict[str, dict[str, AttributeType]] = {}
    for type_name, attributes in value.items():
        if not isinstance(type_name, str) or not isinstance(attributes, dict):
            raise NativeManifestError(
                f"Native manifest {kind} type declarations are malformed."
            )
        parsed: dict[str, AttributeType] = {}
        for name, attr_type in attributes.items():
            if not isinstance(name, str) or not isinstance(attr_type, str):
                raise NativeManifestError(
                    f"Native manifest {kind} attribute declarations are malformed."
                )
            try:
                parsed[name] = AttributeType.parse(attr_type)
            except ValueError as exc:
                raise NativeManifestError(
                    f"Unsupported {kind} attribute type {attr_type!r}."
                ) from exc
        result[type_name] = parsed
    return result
