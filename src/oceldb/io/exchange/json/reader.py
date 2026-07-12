"""Read conforming OCEL 2.0 JSON logs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

from oceldb import schema as s
from oceldb.io._values import coerce_attribute, parse_datetime
from oceldb.io.errors import ValidationMode, check_validation_mode, issue
from oceldb.io.exchange._common import EPOCH, EPOCH_DATETIME, build_ocel
from oceldb.schema import AttributeType, OCELSchema
from oceldb.ocel import OCEL


def read_json(path: str | Path, *, validation: ValidationMode = "strict") -> OCEL:
    """Read an OCEL 2.0 JSON document with retained declared metadata.

    ``validation="strict"`` rejects missing declarations, malformed values,
    and dangling relationships. ``"warn"`` imports recoverable rows while
    emitting :class:`~oceldb.io.OCELIOWarning`; ``"none"`` is permissive.
    """
    validation = check_validation_mode(validation)
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(f"File not found: {source}")
    with source.open(encoding="utf-8") as file:
        data = json.load(file)
    if not isinstance(data, dict):
        issue(validation, "OCEL JSON root must be an object.")
        data = {}

    event_types = _read_type_declarations(data.get("eventTypes"), validation, "event")
    object_types = _read_type_declarations(
        data.get("objectTypes"), validation, "object"
    )
    schema = OCELSchema(event_types=event_types, object_types=object_types)

    objects_data = _array(data, "objects", validation)
    obj_type: dict[str, str] = {}
    for index, obj in enumerate(objects_data):
        if not isinstance(obj, dict):
            issue(validation, f"objects[{index}] must be an object.")
            continue
        obj_id = _required_string(obj, "id", validation, f"objects[{index}]")
        type_name = _required_string(obj, "type", validation, f"objects[{index}]")
        if obj_id:
            if obj_id in obj_type:
                issue(validation, f"Duplicate object id {obj_id!r}.")
            obj_type[obj_id] = type_name
        if type_name not in object_types:
            issue(validation, f"Object {obj_id!r} uses undeclared type {type_name!r}.")
            object_types.setdefault(type_name, {})

    event_rows: list[dict[str, Any]] = []
    e2o_rows: list[dict[str, Any]] = []
    event_ids: set[str] = set()
    for index, event in enumerate(_array(data, "events", validation)):
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
            declarations = event_types.setdefault(type_name, {})
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
        event_rows.append(row)
        for rel_index, relation in enumerate(
            _list_value(
                event.get("relationships", []),
                validation,
                f"{context}.relationships",
            )
        ):
            rel_context = f"{context}.relationships[{rel_index}]"
            if not isinstance(relation, dict):
                issue(validation, f"{rel_context} must be an object.")
                continue
            object_id = _required_string(relation, "objectId", validation, rel_context)
            target_type = obj_type.get(object_id)
            if target_type is None:
                issue(
                    validation,
                    f"{rel_context} references unknown object {object_id!r}.",
                )
                target_type = ""
            qualifier = relation.get("qualifier")
            if not isinstance(qualifier, str):
                issue(validation, f"{rel_context}.qualifier must be a string.")
                qualifier = None
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
    for index, obj in enumerate(objects_data):
        if not isinstance(obj, dict):
            continue
        context = f"objects[{index}]"
        object_id = _required_string(obj, "id", validation, context)
        type_name = _required_string(obj, "type", validation, context)
        declarations = object_types.setdefault(type_name, {})
        object_rows.append({s.OCEL_ID: object_id, s.OCEL_TYPE: type_name})
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
            change_rows.append(
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
        for rel_index, relation in enumerate(
            _list_value(
                obj.get("relationships", []),
                validation,
                f"{context}.relationships",
            )
        ):
            rel_context = f"{context}.relationships[{rel_index}]"
            if not isinstance(relation, dict):
                issue(validation, f"{rel_context} must be an object.")
                continue
            target_id = _required_string(relation, "objectId", validation, rel_context)
            target_type = obj_type.get(target_id)
            if target_type is None:
                issue(
                    validation,
                    f"{rel_context} references unknown object {target_id!r}.",
                )
                target_type = ""
            qualifier = relation.get("qualifier")
            if not isinstance(qualifier, str):
                issue(validation, f"{rel_context}.qualifier must be a string.")
                qualifier = None
            o2o_rows.append(
                {
                    s.OCEL_SOURCE_ID: object_id,
                    s.OCEL_SOURCE_TYPE: type_name,
                    s.OCEL_TARGET_ID: target_id,
                    s.OCEL_TARGET_TYPE: target_type,
                    s.OCEL_QUALIFIER: qualifier,
                }
            )

    # Declarations may have been inferred in permissive modes.
    schema = OCELSchema(event_types=event_types, object_types=object_types)
    return build_ocel(
        event_rows=event_rows,
        object_rows=object_rows,
        object_change_rows=change_rows,
        e2o_rows=e2o_rows,
        o2o_rows=o2o_rows,
        schema=schema,
    )


def _read_type_declarations(
    raw: Any, validation: ValidationMode, kind: str
) -> dict[str, dict[str, AttributeType]]:
    if not isinstance(raw, list):
        issue(validation, f"{kind}Types must be an array.")
        return {}
    result: dict[str, dict[str, AttributeType]] = {}
    for index, item in enumerate(cast(list[Any], raw)):
        context = f"{kind}Types[{index}]"
        if not isinstance(item, dict):
            issue(validation, f"{context} must be an object.")
            continue
        name = _required_string(item, "name", validation, context)
        attributes: dict[str, AttributeType] = {}
        for attr_index, attribute in enumerate(
            _list_value(item.get("attributes"), validation, f"{context}.attributes")
        ):
            attr_context = f"{context}.attributes[{attr_index}]"
            if not isinstance(attribute, dict):
                issue(validation, f"{attr_context} must be an object.")
                continue
            attr_name = _required_string(attribute, "name", validation, attr_context)
            type_name = _required_string(attribute, "type", validation, attr_context)
            try:
                attributes[attr_name] = AttributeType.parse(type_name)
            except ValueError as exc:
                issue(validation, f"{attr_context}: {exc}")
                attributes[attr_name] = AttributeType.STRING
        if name in result:
            issue(validation, f"Duplicate {kind} type declaration {name!r}.")
        result[name] = attributes
    return result


def _read_attributes(
    raw: Any,
    row: dict[str, Any],
    declarations: dict[str, AttributeType],
    validation: ValidationMode,
    parent: str,
) -> None:
    if not isinstance(raw, list):
        issue(validation, f"{parent}.attributes must be an array.")
        return
    for index, attribute in enumerate(cast(list[Any], raw)):
        context = f"{parent}.attributes[{index}]"
        if not isinstance(attribute, dict):
            issue(validation, f"{context} must be an object.")
            continue
        name = _required_string(attribute, "name", validation, context)
        attr_type = _attribute_type(
            name, attribute.get("value"), declarations, validation, context
        )
        row[name] = coerce_attribute(
            attribute.get("value"),
            attr_type,
            validation=validation,
            context=f"{context}.value",
        )


def _attribute_type(
    name: str,
    value: Any,
    declarations: dict[str, AttributeType],
    validation: ValidationMode,
    context: str,
) -> AttributeType:
    declared = declarations.get(name)
    if declared is not None:
        return declared
    issue(validation, f"{context}: attribute {name!r} is not declared.")
    inferred = _infer_value_type(value)
    declarations[name] = inferred
    return inferred


def _infer_value_type(value: Any) -> AttributeType:
    if isinstance(value, bool):
        return AttributeType.BOOLEAN
    if isinstance(value, int):
        return AttributeType.INTEGER
    if isinstance(value, float):
        return AttributeType.FLOAT
    return AttributeType.STRING


def _array(data: dict[str, Any], key: str, validation: ValidationMode) -> list[Any]:
    value = data.get(key)
    if not isinstance(value, list):
        issue(validation, f"{key} must be an array.")
        return []
    return cast(list[Any], value)


def _list_value(value: Any, validation: ValidationMode, context: str) -> list[Any]:
    if not isinstance(value, list):
        issue(validation, f"{context} must be an array.")
        return []
    return cast(list[Any], value)


def _required_string(
    data: dict[str, Any], key: str, validation: ValidationMode, context: str
) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        issue(validation, f"{context}.{key} must be a non-empty string.")
        return "" if value is None else str(value)
    return value
