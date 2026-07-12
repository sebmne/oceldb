"""Incremental OCEL 2.0 JSON import into native storage."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import ijson

from oceldb import schema as s
from oceldb.io._values import coerce_attribute, parse_datetime
from oceldb.io.errors import ValidationMode, check_validation_mode, issue
from oceldb.io.exchange._common import EPOCH, EPOCH_DATETIME
from oceldb.io.exchange.json.reader import (
    _attribute_type,
    _list_value,
    _read_attributes,
    _read_type_declarations,
    _required_string,
    read_json,
)
from oceldb.io.native.batch import NativeBatchSink
from oceldb.schema import AttributeType, OCELSchema


def import_json(
    source: str | Path,
    target: str | Path,
    *,
    overwrite: bool = False,
    validation: ValidationMode = "strict",
    batch_size: int = 10_000,
) -> None:
    """Incrementally import OCEL JSON into staged native Parquet storage."""
    validation = check_validation_mode(validation)
    source_path = Path(source)
    if not source_path.exists():
        raise FileNotFoundError(f"File not found: {source_path}")
    if validation != "strict":
        read_json(source_path, validation=validation).write(target, overwrite=overwrite)
        return

    event_types = _read_type_declarations(
        list(_json_items(source_path, "eventTypes.item")), validation, "event"
    )
    object_types = _read_type_declarations(
        list(_json_items(source_path, "objectTypes.item")), validation, "object"
    )
    object_lookup: dict[str, str] = {}
    for index, obj in enumerate(_json_items(source_path, "objects.item")):
        if not isinstance(obj, dict):
            issue(validation, f"objects[{index}] must be an object.")
            continue
        context = f"objects[{index}]"
        object_id = _required_string(obj, "id", validation, context)
        type_name = _required_string(obj, "type", validation, context)
        if object_id in object_lookup:
            issue(validation, f"Duplicate object id {object_id!r}.")
        object_lookup[object_id] = type_name
        if type_name not in object_types:
            issue(
                validation, f"Object {object_id!r} uses undeclared type {type_name!r}."
            )

    schema = OCELSchema(event_types=event_types, object_types=object_types)
    with NativeBatchSink(
        target, schema, overwrite=overwrite, batch_size=batch_size
    ) as sink:
        _stream_objects(source_path, sink, object_types, object_lookup, validation)
        _stream_events(source_path, sink, event_types, object_lookup, validation)


def _stream_events(
    source: Path,
    sink: NativeBatchSink,
    event_types: dict[str, dict[str, AttributeType]],
    object_lookup: dict[str, str],
    validation: ValidationMode,
) -> None:
    event_ids: set[str] = set()
    for index, event in enumerate(_json_items(source, "events.item")):
        if not isinstance(event, dict):
            issue(validation, f"events[{index}] must be an object.")
            continue
        context = f"events[{index}]"
        event_id = _required_string(event, "id", validation, context)
        if event_id in event_ids:
            issue(validation, f"Duplicate event id {event_id!r}.")
        event_ids.add(event_id)
        type_name = _required_string(event, "type", validation, context)
        declarations = event_types.get(type_name)
        if declarations is None:
            issue(validation, f"Event {event_id!r} uses undeclared type {type_name!r}.")
            continue
        row: dict[str, Any] = {
            s.OCEL_ID: event_id,
            s.OCEL_TYPE: type_name,
            s.OCEL_TIME: parse_datetime(
                event.get("time"), validation=validation, context=f"{context}.time"
            ),
        }
        _read_attributes(
            event.get("attributes", []), row, declarations, validation, context
        )
        sink.add_events([row])
        relations: list[dict[str, Any]] = []
        for rel_index, relation in enumerate(
            _list_value(
                event.get("relationships", []), validation, f"{context}.relationships"
            )
        ):
            rel_context = f"{context}.relationships[{rel_index}]"
            if not isinstance(relation, dict):
                issue(validation, f"{rel_context} must be an object.")
                continue
            object_id = _required_string(relation, "objectId", validation, rel_context)
            target_type = object_lookup.get(object_id)
            if target_type is None:
                issue(
                    validation,
                    f"{rel_context} references unknown object {object_id!r}.",
                )
                continue
            qualifier = relation.get("qualifier")
            if not isinstance(qualifier, str):
                issue(validation, f"{rel_context}.qualifier must be a string.")
                continue
            relations.append(
                {
                    s.OCEL_EVENT_ID: event_id,
                    s.OCEL_EVENT_TYPE: type_name,
                    s.OCEL_OBJECT_ID: object_id,
                    s.OCEL_OBJECT_TYPE: target_type,
                    s.OCEL_QUALIFIER: qualifier,
                }
            )
        sink.add_e2o(relations)


def _stream_objects(
    source: Path,
    sink: NativeBatchSink,
    object_types: dict[str, dict[str, AttributeType]],
    object_lookup: dict[str, str],
    validation: ValidationMode,
) -> None:
    for index, obj in enumerate(_json_items(source, "objects.item")):
        if not isinstance(obj, dict):
            continue
        context = f"objects[{index}]"
        object_id = _required_string(obj, "id", validation, context)
        type_name = _required_string(obj, "type", validation, context)
        declarations = object_types[type_name]
        sink.add_objects([{s.OCEL_ID: object_id, s.OCEL_TYPE: type_name}])
        changes: list[dict[str, Any]] = []
        for attr_index, attribute in enumerate(
            _list_value(obj.get("attributes", []), validation, f"{context}.attributes")
        ):
            attr_context = f"{context}.attributes[{attr_index}]"
            if not isinstance(attribute, dict):
                issue(validation, f"{attr_context} must be an object.")
                continue
            name = _required_string(attribute, "name", validation, attr_context)
            attr_type = _attribute_type(
                name, attribute.get("value"), declarations, validation, attr_context
            )
            timestamp = cast(Any, attribute.get("time", EPOCH))
            if "time" not in attribute:
                issue(validation, f"{attr_context}.time is required by OCEL 2.0.")
            parsed_time = parse_datetime(
                timestamp, validation=validation, context=f"{attr_context}.time"
            )
            changes.append(
                {
                    s.OCEL_ID: object_id,
                    s.OCEL_TYPE: type_name,
                    s.OCEL_TIME: parsed_time,
                    s.OCEL_CHANGED_FIELD: None
                    if parsed_time == EPOCH_DATETIME
                    else name,
                    name: coerce_attribute(
                        attribute.get("value"),
                        attr_type,
                        validation=validation,
                        context=f"{attr_context}.value",
                    ),
                }
            )
        sink.add_object_changes(changes)
        relations: list[dict[str, Any]] = []
        for rel_index, relation in enumerate(
            _list_value(
                obj.get("relationships", []), validation, f"{context}.relationships"
            )
        ):
            rel_context = f"{context}.relationships[{rel_index}]"
            if not isinstance(relation, dict):
                issue(validation, f"{rel_context} must be an object.")
                continue
            target_id = _required_string(relation, "objectId", validation, rel_context)
            target_type = object_lookup.get(target_id)
            if target_type is None:
                issue(
                    validation,
                    f"{rel_context} references unknown object {target_id!r}.",
                )
                continue
            qualifier = relation.get("qualifier")
            if not isinstance(qualifier, str):
                issue(validation, f"{rel_context}.qualifier must be a string.")
                continue
            relations.append(
                {
                    s.OCEL_SOURCE_ID: object_id,
                    s.OCEL_SOURCE_TYPE: type_name,
                    s.OCEL_TARGET_ID: target_id,
                    s.OCEL_TARGET_TYPE: target_type,
                    s.OCEL_QUALIFIER: qualifier,
                }
            )
        sink.add_o2o(relations)


def _json_items(path: Path, prefix: str) -> Iterator[Any]:
    with path.open("rb") as file:
        yield from ijson.items(file, prefix, use_float=True)
