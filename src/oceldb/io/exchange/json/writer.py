"""Write OCEL 2.0 JSON documents."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import polars as pl

from oceldb import schema as s
from oceldb.io._paths import atomic_file
from oceldb.io._schema import materialize, validate_for_exchange
from oceldb.io._values import encode_attribute, format_datetime, qualifier
from oceldb.io.errors import ValidationMode, check_validation_mode
from oceldb.schema import AttributeType, TypeAttributes
from oceldb.ocel import OCEL

_EVENT_FIXED = {s.OCEL_ID, s.OCEL_TIME, s.OCEL_TYPE}
_CHANGE_FIXED = {s.OCEL_ID, s.OCEL_TIME, s.OCEL_TYPE, s.OCEL_CHANGED_FIELD}


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
    data = materialize(ocel)
    validate_for_exchange(data, validation)
    event_relations = _group_relations(
        data.e2o,
        source=s.OCEL_EVENT_ID,
        target=s.OCEL_OBJECT_ID,
        validation=validation,
    )
    object_relations = _group_relations(
        data.o2o,
        source=s.OCEL_SOURCE_ID,
        target=s.OCEL_TARGET_ID,
        validation=validation,
    )

    events: list[dict[str, Any]] = []
    for row in data.events.sort(s.OCEL_TIME, s.OCEL_ID).iter_rows(named=True):
        type_name = str(row[s.OCEL_TYPE])
        events.append(
            {
                "id": str(row[s.OCEL_ID]),
                "type": type_name,
                "time": format_datetime(row[s.OCEL_TIME]),
                "attributes": _event_attributes(
                    row,
                    data.schema.event_types.get(type_name, {}),
                    validation,
                    f"event {row[s.OCEL_ID]!r}",
                ),
                "relationships": event_relations.get(str(row[s.OCEL_ID]), []),
            }
        )

    changes_by_object: dict[str, list[dict[str, Any]]] = {}
    for row in data.object_changes.sort(
        s.OCEL_ID, s.OCEL_TIME, s.OCEL_CHANGED_FIELD
    ).iter_rows(named=True):
        object_id = str(row[s.OCEL_ID])
        type_name = str(row[s.OCEL_TYPE])
        declarations = data.schema.object_types.get(type_name, {})
        changed = row.get(s.OCEL_CHANGED_FIELD)
        names = [str(changed)] if changed else list(declarations)
        for name in names:
            if name not in row or row[name] is None:
                continue
            attr_type = declarations.get(name, AttributeType.STRING)
            changes_by_object.setdefault(object_id, []).append(
                {
                    "name": name,
                    "time": format_datetime(row[s.OCEL_TIME]),
                    "value": encode_attribute(
                        row[name],
                        attr_type,
                        style="json",
                        validation=validation,
                        context=f"object {object_id!r} attribute {name!r}",
                    ),
                }
            )

    objects = [
        {
            "id": str(row[s.OCEL_ID]),
            "type": str(row[s.OCEL_TYPE]),
            "attributes": changes_by_object.get(str(row[s.OCEL_ID]), []),
            "relationships": object_relations.get(str(row[s.OCEL_ID]), []),
        }
        for row in data.objects.sort(s.OCEL_ID).iter_rows(named=True)
    ]
    document = {
        "eventTypes": _type_declarations(data.schema.event_types),
        "objectTypes": _type_declarations(data.schema.object_types),
        "events": events,
        "objects": objects,
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


def _event_attributes(
    row: dict[str, Any],
    declarations: TypeAttributes,
    validation: ValidationMode,
    context: str,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    names = list(declarations) or [name for name in row if name not in _EVENT_FIXED]
    for name in names:
        value = row.get(name)
        if value is None:
            continue
        attr_type = declarations.get(name, AttributeType.STRING)
        result.append(
            {
                "name": name,
                "value": encode_attribute(
                    value,
                    attr_type,
                    style="json",
                    validation=validation,
                    context=f"{context} attribute {name!r}",
                ),
            }
        )
    return result


def _group_relations(
    frame: pl.DataFrame,
    *,
    source: str,
    target: str,
    validation: ValidationMode,
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    if source not in frame.columns:
        return grouped
    for row in frame.iter_rows(named=True):
        relation = {
            "objectId": str(row[target]),
            "qualifier": qualifier(
                row[s.OCEL_QUALIFIER],
                validation=validation,
                context=f"relation {row[source]!r} -> {row[target]!r}",
            ),
        }
        grouped.setdefault(str(row[source]), []).append(relation)
    return grouped
