"""Stream and normalize OCEL 2.0 XML documents."""

import xml.etree.ElementTree as ET
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from oceldb import schema as s
from oceldb.io._values import coerce_attribute, parse_datetime
from oceldb.io.errors import ValidationMode, check_validation_mode, io_boundary, issue
from oceldb.io._schema import validate_dataset_for_io
from oceldb.io.exchange._common import (
    EPOCH_DATETIME,
    ParsedEvent,
    ParsedObject,
    collect_parsed,
)
from oceldb.schema import AttributeType, OCELSchema
from oceldb.ocel import OCEL


def read_xml(path: str | Path, *, validation: ValidationMode = "strict") -> OCEL:
    """Read an OCEL 2.0 XML document into an in-memory OCEL."""
    validation = check_validation_mode(validation)
    with io_boundary("read XML OCEL from", path):
        parser = XMLParser(path, validation=validation)
        ocel = collect_parsed(parser.schema, parser.objects(), parser.events())
        validate_dataset_for_io(ocel._dataset, validation)
        return ocel


class XMLParser:
    """Normalized, repeatable record stream shared by reading and importing."""

    def __init__(self, path: str | Path, *, validation: ValidationMode) -> None:
        self.source = Path(path)
        self.validation: ValidationMode = validation
        if not self.source.exists():
            raise FileNotFoundError(f"File not found: {self.source}")
        self._validate_root()
        self.event_types = _type_declarations(self.source, "event-type", validation)
        self.object_types = _type_declarations(self.source, "object-type", validation)
        self.object_lookup: dict[str, str] = {}
        self._discover_objects()
        self._discover_events()
        self.schema = OCELSchema(
            event_types=self.event_types,
            object_types=self.object_types,
        )

    def objects(self) -> Iterator[ParsedObject]:
        seen: set[str] = set()
        for index, obj in enumerate(_iter_elements(self.source, "object")):
            context = f"objects/object[{index}]"
            object_id = _required_attr(obj, "id", self.validation, context)
            if object_id in seen:
                issue(self.validation, f"Duplicate object id {object_id!r}.")
            seen.add(object_id)
            type_name = _required_attr(obj, "type", self.validation, context)
            changes = self._object_changes(
                obj,
                object_id,
                type_name,
                self.object_types[type_name],
                context,
            )
            relations = self._object_relations(obj, object_id, type_name, context)
            yield ParsedObject(
                row={s.OCEL_ID: object_id, s.OCEL_TYPE: type_name},
                changes=tuple(changes),
                relations=tuple(relations),
            )

    def events(self) -> Iterator[ParsedEvent]:
        seen: set[str] = set()
        for index, event in enumerate(_iter_elements(self.source, "event")):
            context = f"events/event[{index}]"
            event_id = _required_attr(event, "id", self.validation, context)
            if event_id in seen:
                issue(self.validation, f"Duplicate event id {event_id!r}.")
            seen.add(event_id)
            type_name = _required_attr(event, "type", self.validation, context)
            row: dict[str, Any] = {
                s.OCEL_ID: event_id,
                s.OCEL_TYPE: type_name,
                s.OCEL_TIME: parse_datetime(
                    event.get("time"),
                    validation=self.validation,
                    context=f"{context}.time",
                ),
            }
            _read_value_elements(
                event,
                row,
                self.event_types[type_name],
                self.validation,
                context,
            )
            relations = self._event_relations(event, event_id, type_name, context)
            yield ParsedEvent(row=row, relations=tuple(relations))

    def _discover_objects(self) -> None:
        for index, obj in enumerate(_iter_elements(self.source, "object")):
            object_id = obj.get("id") or ""
            type_name = obj.get("type") or ""
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
                obj,
                declarations,
                self.validation,
                f"objects/object[{index}]",
            )

    def _discover_events(self) -> None:
        for index, event in enumerate(_iter_elements(self.source, "event")):
            event_id = event.get("id") or ""
            type_name = event.get("type") or ""
            declarations = self.event_types.get(type_name)
            if declarations is None:
                issue(
                    self.validation,
                    f"Event {event_id!r} uses undeclared type {type_name!r}.",
                )
                declarations = self.event_types.setdefault(type_name, {})
            _discover_attributes(
                event,
                declarations,
                self.validation,
                f"events/event[{index}]",
            )

    def _object_changes(
        self,
        obj: ET.Element,
        object_id: str,
        type_name: str,
        declarations: dict[str, AttributeType],
        context: str,
    ) -> list[dict[str, Any]]:
        changes: list[dict[str, Any]] = []
        for index, attribute in enumerate(_findall(obj, "./attributes/attribute")):
            attr_context = f"{context}/attributes/attribute[{index}]"
            name = _required_attr(attribute, "name", self.validation, attr_context)
            raw_value = attribute.text if attribute.text is not None else ""
            parsed_time = parse_datetime(
                attribute.get("time"),
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
                        raw_value,
                        declarations[name],
                        validation=self.validation,
                        context=f"{attr_context}.value",
                    ),
                }
            )
        return changes

    def _event_relations(
        self,
        event: ET.Element,
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
                s.OCEL_QUALIFIER: qualifier,
            }
            for target_id, target_type, qualifier in self._relations(event, context)
        ]

    def _object_relations(
        self,
        obj: ET.Element,
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
                s.OCEL_QUALIFIER: qualifier,
            }
            for target_id, target_type, qualifier in self._relations(obj, context)
        ]

    def _relations(
        self, parent: ET.Element, context: str
    ) -> Iterator[tuple[str, str, str | None]]:
        for index, relation in enumerate(_findall(parent, "./objects/relationship")):
            rel_context = f"{context}/objects/relationship[{index}]"
            target_id = relation.get("object-id") or relation.get("objectId") or ""
            if not target_id:
                issue(self.validation, f"{rel_context} requires object-id.")
            target_type = self.object_lookup.get(target_id)
            if target_type is None:
                issue(
                    self.validation,
                    f"{rel_context} references unknown object {target_id!r}.",
                )
                target_type = ""
            qualifier = relation.get("qualifier") or relation.get("relationship")
            if qualifier is None:
                issue(self.validation, f"{rel_context} requires qualifier.")
            yield target_id, target_type, qualifier

    def _validate_root(self) -> None:
        iterator = ET.iterparse(self.source, events=("start",))
        try:
            _, root = next(iterator)
        except StopIteration:
            issue(self.validation, "OCEL XML document is empty.")
            return
        if _local_name(root.tag) != "log":
            issue(self.validation, "OCEL XML root element must be <log>.")


def _type_declarations(
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
                attributes[attr_name] = AttributeType.STRING
        if name in result:
            issue(validation, f"Duplicate type declaration {name!r}.")
        result[name] = attributes
    return result


def _discover_attributes(
    parent: ET.Element,
    declarations: dict[str, AttributeType],
    validation: ValidationMode,
    context: str,
) -> None:
    for index, attribute in enumerate(_findall(parent, "./attributes/attribute")):
        name = attribute.get("name") or ""
        if name not in declarations:
            issue(
                validation,
                f"{context}/attributes/attribute[{index}]: "
                f"attribute {name!r} is not declared.",
            )
            declarations[name] = AttributeType.STRING


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
        row[name] = coerce_attribute(
            raw_value,
            declarations[name],
            validation=validation,
            context=f"{attr_context}.value",
        )


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


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]
