"""Write OCEL 2.0 JSON documents."""

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from oceldb.io._paths import atomic_file
from oceldb.io.errors import ValidationMode, check_validation_mode
from oceldb.io.exchange._common import prepare_exchange
from oceldb.schema import TypeAttributes
from oceldb.ocel import OCEL


def write_json(
    ocel: OCEL,
    path: str | Path,
    *,
    overwrite: bool = False,
    indent: int | None = 2,
    validation: ValidationMode = "strict",
) -> None:
    """Write a schema-complete OCEL 2.0 JSON document."""
    validation = check_validation_mode(validation)
    data = prepare_exchange(ocel, style="json", validation=validation)
    document = {
        "eventTypes": _type_declarations(data.schema.event_types),
        "objectTypes": _type_declarations(data.schema.object_types),
        "events": [
            {
                "id": event.id,
                "type": event.type,
                "time": event.time,
                "attributes": [
                    {"name": attribute.name, "value": attribute.value}
                    for attribute in event.attributes
                ],
                "relationships": [
                    {
                        "objectId": relation.object_id,
                        "qualifier": relation.qualifier,
                    }
                    for relation in event.relationships
                ],
            }
            for event in data.events
        ],
        "objects": [
            {
                "id": obj.id,
                "type": obj.type,
                "attributes": [
                    {
                        "name": attribute.name,
                        "time": attribute.time,
                        "value": attribute.value,
                    }
                    for attribute in obj.attributes
                ],
                "relationships": [
                    {
                        "objectId": relation.object_id,
                        "qualifier": relation.qualifier,
                    }
                    for relation in obj.relationships
                ],
            }
            for obj in data.objects
        ],
    }
    with atomic_file(path, overwrite=overwrite) as (_, staging):
        with staging.open("w", encoding="utf-8") as file:
            json.dump(document, file, ensure_ascii=False, indent=indent)
            file.write("\n")


def _type_declarations(
    types: Mapping[str, TypeAttributes],
) -> list[dict[str, Any]]:
    return [
        {
            "name": type_name,
            "attributes": [
                {"name": name, "type": attr_type.value}
                for name, attr_type in sorted(attributes.items())
            ],
        }
        for type_name, attributes in sorted(types.items())
    ]
