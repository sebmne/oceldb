"""Stream and normalize OCEL 2.0 JSON documents."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import ijson

from oceldb import schema as s
from oceldb.io._values import coerce_attribute, parse_datetime
from oceldb.io.errors import ValidationMode, check_validation_mode, io_boundary, issue
from oceldb.io._schema import validate_dataset_for_io
from oceldb.io.exchange._common import (
    EPOCH,
    EPOCH_DATETIME,
    ParsedEvent,
    ParsedObject,
    collect_parsed,
)
from oceldb.schema import AttributeType, OCELSchema
from oceldb.ocel import OCEL

_ARRAYS = ("eventTypes", "objectTypes", "events", "objects")


def read_json(path: str | Path, *, validation: ValidationMode = "strict") -> OCEL:
    """Read an OCEL 2.0 JSON document into an in-memory OCEL."""
    validation = check_validation_mode(validation)
    with io_boundary("read JSON OCEL from", path):
        parser = JSONParser(path, validation=validation)
        ocel = collect_parsed(parser.schema, parser.objects(), parser.events())
        validate_dataset_for_io(ocel._dataset, validation)
        return ocel


class JSONParser:
    """Normalized, repeatable record stream shared by reading and importing."""

    def __init__(self, path: str | Path, *, validation: ValidationMode) -> None:
        self.source = Path(path)
        self.validation: ValidationMode = validation
        if not self.source.exists():
            raise FileNotFoundError(f"File not found: {self.source}")
        self._validate_shape()
        self.event_types = _read_type_declarations(
            list(self._items("eventTypes.item")), validation, "event"
        )
        self.object_types = _read_type_declarations(
            list(self._items("objectTypes.item")), validation, "object"
        )
        self.object_lookup: dict[str, str] = {}
        self._discover_objects()
        self._discover_events()
        self.schema = OCELSchema(
            event_types=self.event_types,
            object_types=self.object_types,
        )

    def objects(self) -> Iterator[ParsedObject]:
        seen: set[str] = set()
        for index, value in enumerate(self._items("objects.item")):
            if not isinstance(value, dict):
                issue(self.validation, f"objects[{index}] must be an object.")
                continue
            obj = cast(dict[str, Any], value)
            context = f"objects[{index}]"
            object_id = _required_string(obj, "id", self.validation, context)
            if object_id in seen:
                issue(self.validation, f"Duplicate object id {object_id!r}.")
            seen.add(object_id)
            type_name = _required_string(obj, "type", self.validation, context)
            declarations = self.object_types[type_name]
            changes = self._object_changes(
                obj, object_id, type_name, declarations, context
            )
            relations = self._object_relations(obj, object_id, type_name, context)
            yield ParsedObject(
                row={s.OCEL_ID: object_id, s.OCEL_TYPE: type_name},
                changes=tuple(changes),
                relations=tuple(relations),
            )

    def events(self) -> Iterator[ParsedEvent]:
        seen: set[str] = set()
        for index, value in enumerate(self._items("events.item")):
            if not isinstance(value, dict):
                issue(self.validation, f"events[{index}] must be an object.")
                continue
            event = cast(dict[str, Any], value)
            context = f"events[{index}]"
            event_id = _required_string(event, "id", self.validation, context)
            if event_id in seen:
                issue(self.validation, f"Duplicate event id {event_id!r}.")
            seen.add(event_id)
            type_name = _required_string(event, "type", self.validation, context)
            row: dict[str, Any] = {
                s.OCEL_ID: event_id,
                s.OCEL_TYPE: type_name,
                s.OCEL_TIME: parse_datetime(
                    event.get("time"),
                    validation=self.validation,
                    context=f"{context}.time",
                ),
            }
            _read_attributes(
                event.get("attributes", []),
                row,
                self.event_types[type_name],
                self.validation,
                context,
            )
            relations = self._event_relations(event, event_id, type_name, context)
            yield ParsedEvent(row=row, relations=tuple(relations))

    def _discover_objects(self) -> None:
        for index, value in enumerate(self._items("objects.item")):
            if not isinstance(value, dict):
                continue
            obj = cast(dict[str, Any], value)
            object_id = _raw_string(obj, "id")
            type_name = _raw_string(obj, "type")
            if object_id:
                self.object_lookup[object_id] = type_name
            declarations = self.object_types.get(type_name)
            if declarations is None:
                issue(
                    self.validation,
                    f"Object {object_id!r} uses undeclared type {type_name!r}.",
                )
                declarations = self.object_types.setdefault(type_name, {})
            _discover_attributes(
                obj.get("attributes"),
                declarations,
                self.validation,
                f"objects[{index}]",
            )

    def _discover_events(self) -> None:
        for index, value in enumerate(self._items("events.item")):
            if not isinstance(value, dict):
                continue
            event = cast(dict[str, Any], value)
            event_id = _raw_string(event, "id")
            type_name = _raw_string(event, "type")
            declarations = self.event_types.get(type_name)
            if declarations is None:
                issue(
                    self.validation,
                    f"Event {event_id!r} uses undeclared type {type_name!r}.",
                )
                declarations = self.event_types.setdefault(type_name, {})
            _discover_attributes(
                event.get("attributes"),
                declarations,
                self.validation,
                f"events[{index}]",
            )

    def _object_changes(
        self,
        obj: dict[str, Any],
        object_id: str,
        type_name: str,
        declarations: dict[str, AttributeType],
        context: str,
    ) -> list[dict[str, Any]]:
        changes: list[dict[str, Any]] = []
        for index, value in enumerate(
            _list_value(
                obj.get("attributes", []),
                self.validation,
                f"{context}.attributes",
            )
        ):
            attr_context = f"{context}.attributes[{index}]"
            if not isinstance(value, dict):
                issue(self.validation, f"{attr_context} must be an object.")
                continue
            attribute = cast(dict[str, Any], value)
            name = _required_string(attribute, "name", self.validation, attr_context)
            attr_type = declarations[name]
            timestamp = attribute.get("time", EPOCH)
            if "time" not in attribute:
                issue(
                    self.validation,
                    f"{attr_context}.time is required by OCEL 2.0.",
                )
            parsed_time = parse_datetime(
                timestamp,
                validation=self.validation,
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
                        attribute.get("value"),
                        attr_type,
                        validation=self.validation,
                        context=f"{attr_context}.value",
                    ),
                }
            )
        return changes

    def _event_relations(
        self,
        event: dict[str, Any],
        event_id: str,
        type_name: str,
        context: str,
    ) -> list[dict[str, Any]]:
        return [
            {
                s.OCEL_EVENT_ID: event_id,
                s.OCEL_EVENT_TYPE: type_name,
                s.OCEL_OBJECT_ID: target_id,
                s.OCEL_OBJECT_TYPE: target_type,
                s.OCEL_QUALIFIER: relation.get("qualifier"),
            }
            for relation, target_id, target_type in self._relations(event, context)
        ]

    def _object_relations(
        self,
        obj: dict[str, Any],
        object_id: str,
        type_name: str,
        context: str,
    ) -> list[dict[str, Any]]:
        return [
            {
                s.OCEL_SOURCE_ID: object_id,
                s.OCEL_SOURCE_TYPE: type_name,
                s.OCEL_TARGET_ID: target_id,
                s.OCEL_TARGET_TYPE: target_type,
                s.OCEL_QUALIFIER: relation.get("qualifier"),
            }
            for relation, target_id, target_type in self._relations(obj, context)
        ]

    def _relations(
        self, parent: dict[str, Any], context: str
    ) -> Iterator[tuple[dict[str, Any], str, str]]:
        for index, value in enumerate(
            _list_value(
                parent.get("relationships", []),
                self.validation,
                f"{context}.relationships",
            )
        ):
            rel_context = f"{context}.relationships[{index}]"
            if not isinstance(value, dict):
                issue(self.validation, f"{rel_context} must be an object.")
                continue
            relation = cast(dict[str, Any], value)
            target_id = _required_string(
                relation, "objectId", self.validation, rel_context
            )
            target_type = self.object_lookup.get(target_id)
            if target_type is None:
                issue(
                    self.validation,
                    f"{rel_context} references unknown object {target_id!r}.",
                )
                target_type = ""
            qualifier = relation.get("qualifier")
            if not isinstance(qualifier, str):
                issue(
                    self.validation,
                    f"{rel_context}.qualifier must be a string.",
                )
                relation["qualifier"] = None
            yield relation, target_id, target_type

    def _validate_shape(self) -> None:
        root_object = False
        arrays: set[str] = set()
        with self.source.open("rb") as file:
            for prefix, event, _ in ijson.parse(file, use_float=True):
                if prefix == "" and event == "start_map":
                    root_object = True
                if prefix in _ARRAYS and event == "start_array":
                    arrays.add(prefix)
        if not root_object:
            issue(self.validation, "OCEL JSON root must be an object.")
        for name in _ARRAYS:
            if name not in arrays:
                issue(self.validation, f"{name} must be an array.")

    def _items(self, prefix: str) -> Iterator[Any]:
        with self.source.open("rb") as file:
            yield from ijson.items(file, prefix, use_float=True)


def _read_type_declarations(
    raw: list[Any], validation: ValidationMode, kind: str
) -> dict[str, dict[str, AttributeType]]:
    result: dict[str, dict[str, AttributeType]] = {}
    for index, item in enumerate(raw):
        context = f"{kind}Types[{index}]"
        if not isinstance(item, dict):
            issue(validation, f"{context} must be an object.")
            continue
        declaration = cast(dict[str, Any], item)
        name = _required_string(declaration, "name", validation, context)
        attributes: dict[str, AttributeType] = {}
        for attr_index, value in enumerate(
            _list_value(
                declaration.get("attributes"),
                validation,
                f"{context}.attributes",
            )
        ):
            attr_context = f"{context}.attributes[{attr_index}]"
            if not isinstance(value, dict):
                issue(validation, f"{attr_context} must be an object.")
                continue
            attribute = cast(dict[str, Any], value)
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


def _discover_attributes(
    raw: Any,
    declarations: dict[str, AttributeType],
    validation: ValidationMode,
    parent: str,
) -> None:
    if not isinstance(raw, list):
        return
    for index, value in enumerate(cast(list[Any], raw)):
        if not isinstance(value, dict):
            continue
        attribute = cast(dict[str, Any], value)
        name = _raw_string(attribute, "name")
        if name not in declarations:
            issue(
                validation,
                f"{parent}.attributes[{index}]: attribute {name!r} is not declared.",
            )
            declarations[name] = _infer_value_type(attribute.get("value"))


def _read_attributes(
    raw: Any,
    row: dict[str, Any],
    declarations: dict[str, AttributeType],
    validation: ValidationMode,
    parent: str,
) -> None:
    for index, value in enumerate(_list_value(raw, validation, f"{parent}.attributes")):
        context = f"{parent}.attributes[{index}]"
        if not isinstance(value, dict):
            issue(validation, f"{context} must be an object.")
            continue
        attribute = cast(dict[str, Any], value)
        name = _required_string(attribute, "name", validation, context)
        row[name] = coerce_attribute(
            attribute.get("value"),
            declarations[name],
            validation=validation,
            context=f"{context}.value",
        )


def _infer_value_type(value: Any) -> AttributeType:
    if isinstance(value, bool):
        return AttributeType.BOOLEAN
    if isinstance(value, int):
        return AttributeType.INTEGER
    if isinstance(value, float):
        return AttributeType.FLOAT
    return AttributeType.STRING


def _list_value(value: Any, validation: ValidationMode, context: str) -> list[Any]:
    if not isinstance(value, list):
        issue(validation, f"{context} must be an array.")
        return []
    return cast(list[Any], value)


def _raw_string(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    return value if isinstance(value, str) else "" if value is None else str(value)


def _required_string(
    data: dict[str, Any], key: str, validation: ValidationMode, context: str
) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        issue(validation, f"{context}.{key} must be a non-empty string.")
        return "" if value is None else str(value)
    return value
