"""Incremental OCEL 2.0 XML import into native storage."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from oceldb import schema as s
from oceldb.io._values import coerce_attribute, parse_datetime
from oceldb.io.errors import ValidationMode, check_validation_mode, issue
from oceldb.io.exchange._common import EPOCH_DATETIME
from oceldb.io.exchange.xml.reader import (
    _findall,
    _local_name,
    _read_value_elements,
    _required_attr,
    _resolve_attribute,
    read_xml,
)
from oceldb.io.native.batch import NativeBatchSink
from oceldb.schema import AttributeType, OCELSchema


def import_xml(
    source: str | Path,
    target: str | Path,
    *,
    overwrite: bool = False,
    validation: ValidationMode = "strict",
    batch_size: int = 10_000,
) -> None:
    """Incrementally import OCEL XML into staged native Parquet storage."""
    validation = check_validation_mode(validation)
    source_path = Path(source)
    if not source_path.exists():
        raise FileNotFoundError(f"File not found: {source_path}")
    if validation != "strict":
        read_xml(source_path, validation=validation).write(target, overwrite=overwrite)
        return
    event_types = _stream_type_declarations(source_path, "event-type", validation)
    object_types = _stream_type_declarations(source_path, "object-type", validation)
    object_lookup: dict[str, str] = {}
    for index, obj in enumerate(_iter_elements(source_path, "object")):
        context = f"objects/object[{index}]"
        object_id = _required_attr(obj, "id", validation, context)
        type_name = _required_attr(obj, "type", validation, context)
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
        _stream_xml_objects(source_path, sink, object_types, object_lookup, validation)
        _stream_xml_events(source_path, sink, event_types, object_lookup, validation)


def _stream_xml_events(
    source: Path,
    sink: NativeBatchSink,
    event_types: dict[str, dict[str, AttributeType]],
    object_lookup: dict[str, str],
    validation: ValidationMode,
) -> None:
    event_ids: set[str] = set()
    for index, event in enumerate(_iter_elements(source, "event")):
        context = f"events/event[{index}]"
        event_id = _required_attr(event, "id", validation, context)
        if event_id in event_ids:
            issue(validation, f"Duplicate event id {event_id!r}.")
        event_ids.add(event_id)
        type_name = _required_attr(event, "type", validation, context)
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
        _read_value_elements(event, row, declarations, validation, context)
        sink.add_events([row])
        relations: list[dict[str, Any]] = []
        for rel_index, relation in enumerate(_findall(event, "./objects/relationship")):
            rel_context = f"{context}/objects/relationship[{rel_index}]"
            object_id = relation.get("object-id") or relation.get("objectId") or ""
            if not object_id:
                issue(validation, f"{rel_context} requires object-id.")
            target_type = object_lookup.get(object_id)
            if target_type is None:
                issue(
                    validation,
                    f"{rel_context} references unknown object {object_id!r}.",
                )
                continue
            qualifier = relation.get("qualifier") or relation.get("relationship")
            if qualifier is None:
                issue(validation, f"{rel_context} requires qualifier.")
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


def _stream_xml_objects(
    source: Path,
    sink: NativeBatchSink,
    object_types: dict[str, dict[str, AttributeType]],
    object_lookup: dict[str, str],
    validation: ValidationMode,
) -> None:
    for index, obj in enumerate(_iter_elements(source, "object")):
        context = f"objects/object[{index}]"
        object_id = _required_attr(obj, "id", validation, context)
        type_name = _required_attr(obj, "type", validation, context)
        declarations = object_types[type_name]
        sink.add_objects([{s.OCEL_ID: object_id, s.OCEL_TYPE: type_name}])
        changes: list[dict[str, Any]] = []
        for attr_index, attribute in enumerate(_findall(obj, "./attributes/attribute")):
            attr_context = f"{context}/attributes/attribute[{attr_index}]"
            name = _required_attr(attribute, "name", validation, attr_context)
            raw_value = attribute.text if attribute.text is not None else ""
            attr_type = _resolve_attribute(
                name, raw_value, declarations, validation, attr_context
            )
            parsed_time = parse_datetime(
                attribute.get("time"),
                validation=validation,
                context=f"{attr_context}.time",
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
                        raw_value,
                        attr_type,
                        validation=validation,
                        context=f"{attr_context}.value",
                    ),
                }
            )
        sink.add_object_changes(changes)
        relations: list[dict[str, Any]] = []
        for rel_index, relation in enumerate(_findall(obj, "./objects/relationship")):
            rel_context = f"{context}/objects/relationship[{rel_index}]"
            target_id = relation.get("object-id") or relation.get("objectId") or ""
            if not target_id:
                issue(validation, f"{rel_context} requires object-id.")
            target_type = object_lookup.get(target_id)
            if target_type is None:
                issue(
                    validation,
                    f"{rel_context} references unknown object {target_id!r}.",
                )
                continue
            qualifier = relation.get("qualifier") or relation.get("relationship")
            if qualifier is None:
                issue(validation, f"{rel_context} requires qualifier.")
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


def _stream_type_declarations(
    source: Path, item_name: str, validation: ValidationMode
) -> dict[str, dict[str, AttributeType]]:
    result: dict[str, dict[str, AttributeType]] = {}
    for index, item in enumerate(_iter_elements(source, item_name)):
        context = f"{item_name}[{index}]"
        name = _required_attr(item, "name", validation, context)
        attributes: dict[str, AttributeType] = {}
        for attr_index, attribute in enumerate(
            _findall(item, "./attributes/attribute")
        ):
            attr_context = f"{context}/attribute[{attr_index}]"
            attr_name = _required_attr(attribute, "name", validation, attr_context)
            raw_type = _required_attr(attribute, "type", validation, attr_context)
            try:
                attributes[attr_name] = AttributeType.parse(raw_type)
            except ValueError as exc:
                issue(validation, f"{attr_context}: {exc}")
        if name in result:
            issue(validation, f"Duplicate type declaration {name!r}.")
        result[name] = attributes
    return result


def _iter_elements(source: Path, name: str) -> Iterator[ET.Element]:
    records = {"event", "object", "event-type", "object-type"}
    iterator = ET.iterparse(source, events=("start", "end"))
    _, root = next(iterator)
    for event, element in iterator:
        if event != "end":
            continue
        local_name = _local_name(element.tag)
        if local_name == name:
            yield element
            element.clear()
            root.clear()
        elif local_name in records:
            element.clear()
            root.clear()
