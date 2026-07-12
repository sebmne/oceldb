"""Read conforming OCEL 2.0 XML logs."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from oceldb import schema as s
from oceldb.io._values import coerce_attribute, parse_datetime
from oceldb.io.errors import ValidationMode, check_validation_mode, issue
from oceldb.io.exchange._common import EPOCH_DATETIME, build_ocel
from oceldb.schema import AttributeType, OCELSchema
from oceldb.ocel import OCEL


def read_xml(path: str | Path, *, validation: ValidationMode = "strict") -> OCEL:
    """Read OCEL 2.0 XML, including declared schemas and element-text values."""
    validation = check_validation_mode(validation)
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(f"File not found: {source}")
    root = ET.parse(source).getroot()
    if _local_name(root.tag) != "log":
        issue(validation, "OCEL XML root element must be <log>.")

    event_types = _type_declarations(root, "event-types", "event-type", validation)
    object_types = _type_declarations(root, "object-types", "object-type", validation)

    object_elements = _findall(root, "./objects/object")
    object_lookup: dict[str, str] = {}
    for index, obj in enumerate(object_elements):
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
            object_types.setdefault(type_name, {})

    event_rows: list[dict[str, Any]] = []
    e2o_rows: list[dict[str, Any]] = []
    event_ids: set[str] = set()
    for index, event in enumerate(_findall(root, "./events/event")):
        context = f"events/event[{index}]"
        event_id = _required_attr(event, "id", validation, context)
        if event_id in event_ids:
            issue(validation, f"Duplicate event id {event_id!r}.")
        event_ids.add(event_id)
        type_name = _required_attr(event, "type", validation, context)
        declarations = event_types.get(type_name)
        if declarations is None:
            issue(validation, f"Event {event_id!r} uses undeclared type {type_name!r}.")
            declarations = event_types.setdefault(type_name, {})
        row: dict[str, Any] = {
            s.OCEL_ID: event_id,
            s.OCEL_TYPE: type_name,
            s.OCEL_TIME: parse_datetime(
                event.get("time"), validation=validation, context=f"{context}.time"
            ),
        }
        _read_value_elements(event, row, declarations, validation, context)
        event_rows.append(row)
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
                target_type = ""
            qualifier = relation.get("qualifier")
            if qualifier is None:
                # Some early examples used ``relationship`` for this field.
                qualifier = relation.get("relationship")
            if qualifier is None:
                issue(validation, f"{rel_context} requires qualifier.")
            e2o_rows.append(
                {
                    s.OCEL_EVENT_ID: event_id,
                    s.OCEL_EVENT_TYPE: type_name,
                    s.OCEL_OBJECT_ID: object_id,
                    s.OCEL_OBJECT_TYPE: target_type,
                    s.OCEL_QUALIFIER: qualifier,
                }
            )

    object_rows: list[dict[str, Any]] = []
    change_rows: list[dict[str, Any]] = []
    o2o_rows: list[dict[str, Any]] = []
    for index, obj in enumerate(object_elements):
        context = f"objects/object[{index}]"
        object_id = _required_attr(obj, "id", validation, context)
        type_name = _required_attr(obj, "type", validation, context)
        declarations = object_types.setdefault(type_name, {})
        object_rows.append({s.OCEL_ID: object_id, s.OCEL_TYPE: type_name})
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
            change_rows.append(
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
                target_type = ""
            qualifier = relation.get("qualifier")
            if qualifier is None:
                qualifier = relation.get("relationship")
            if qualifier is None:
                issue(validation, f"{rel_context} requires qualifier.")
            o2o_rows.append(
                {
                    s.OCEL_SOURCE_ID: object_id,
                    s.OCEL_SOURCE_TYPE: type_name,
                    s.OCEL_TARGET_ID: target_id,
                    s.OCEL_TARGET_TYPE: target_type,
                    s.OCEL_QUALIFIER: qualifier,
                }
            )

    return build_ocel(
        event_rows=event_rows,
        object_rows=object_rows,
        object_change_rows=change_rows,
        e2o_rows=e2o_rows,
        o2o_rows=o2o_rows,
        schema=OCELSchema(event_types=event_types, object_types=object_types),
    )


def _type_declarations(
    root: ET.Element,
    collection: str,
    item_name: str,
    validation: ValidationMode,
) -> dict[str, dict[str, AttributeType]]:
    result: dict[str, dict[str, AttributeType]] = {}
    for index, item in enumerate(_findall(root, f"./{collection}/{item_name}")):
        context = f"{collection}/{item_name}[{index}]"
        name = _required_attr(item, "name", validation, context)
        attributes: dict[str, AttributeType] = {}
        for attr_index, attribute in enumerate(
            _findall(item, "./attributes/attribute")
        ):
            attr_context = f"{context}/attributes/attribute[{attr_index}]"
            attr_name = _required_attr(attribute, "name", validation, attr_context)
            raw_type = _required_attr(attribute, "type", validation, attr_context)
            try:
                attributes[attr_name] = AttributeType.parse(raw_type)
            except ValueError as exc:
                issue(validation, f"{attr_context}: {exc}")
                attributes[attr_name] = AttributeType.STRING
        if name in result:
            issue(validation, f"Duplicate type declaration {name!r}.")
        result[name] = attributes
    return result


def _read_value_elements(
    parent: ET.Element,
    row: dict[str, Any],
    declarations: dict[str, AttributeType],
    validation: ValidationMode,
    context: str,
) -> None:
    for index, attribute in enumerate(_findall(parent, "./attributes/attribute")):
        attr_context = f"{context}/attributes/attribute[{index}]"
        name = _required_attr(attribute, "name", validation, attr_context)
        raw_value = attribute.text if attribute.text is not None else ""
        attr_type = _resolve_attribute(
            name, raw_value, declarations, validation, attr_context
        )
        row[name] = coerce_attribute(
            raw_value,
            attr_type,
            validation=validation,
            context=f"{attr_context}.value",
        )


def _resolve_attribute(
    name: str,
    value: Any,
    declarations: dict[str, AttributeType],
    validation: ValidationMode,
    context: str,
) -> AttributeType:
    attr_type = declarations.get(name)
    if attr_type is not None:
        return attr_type
    issue(validation, f"{context}: attribute {name!r} is not declared.")
    declarations[name] = AttributeType.STRING
    return AttributeType.STRING


def _required_attr(
    element: ET.Element,
    name: str,
    validation: ValidationMode,
    context: str,
) -> str:
    value = element.get(name)
    if not value:
        issue(validation, f"{context} requires non-empty attribute {name!r}.")
        return ""
    return value


def _findall(element: ET.Element, path: str) -> list[ET.Element]:
    """Find path elements while tolerating a document namespace."""
    direct = element.findall(path)
    if direct:
        return direct
    parts = path.removeprefix("./").split("/")
    current = [element]
    for part in parts:
        current = [
            child
            for parent in current
            for child in list(parent)
            if _local_name(child.tag) == part
        ]
    return current


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]
