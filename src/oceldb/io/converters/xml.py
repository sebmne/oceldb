"""Streaming OCEL 2.0 XML-to-native conversion."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any
import xml.etree.ElementTree as ElementTree

from defusedxml import ElementTree as SafeElementTree
from defusedxml.common import DefusedXmlException

from oceldb import OCEL
from oceldb.core import schema as s
from oceldb.io._errors import OCELConversionError, conversion_error
from oceldb.io._schema import (
    AttributeType,
    ExchangeSchema,
    TypeDeclarations,
    require_declared_type,
    required_string,
)
from oceldb.io._relations import RelationIndex
from oceldb.io._sink import TableSink
from oceldb.io._values import EPOCH, attribute_value, timestamp


def convert(
    source: Path,
    target: Path,
    staging: Path,
    overwrite: bool,
    validate: bool,
    batch_size: int,
) -> OCEL:
    """Convert one XML exchange file using secure incremental parsing."""
    try:
        event_types, object_types = _type_declarations(source)
        schema = ExchangeSchema.build(event_types, object_types)
        sink = TableSink(staging / "tables", schema, batch_size=batch_size)
        relations = RelationIndex(staging / "relations.sqlite", batch_size=batch_size)
        try:
            _convert_objects(source, sink, schema, relations)
            _convert_events(source, sink, schema, relations)
            relations.resolve_into(sink)
        finally:
            relations.close()
        return sink.finish(
            target,
            overwrite=overwrite,
            validate=validate,
        )
    except OCELConversionError:
        raise
    except (ElementTree.ParseError, DefusedXmlException) as exc:
        raise OCELConversionError(f"Invalid OCEL XML document: {exc}") from exc


def _type_declarations(source: Path) -> tuple[TypeDeclarations, TypeDeclarations]:
    event_types: TypeDeclarations = {}
    object_types: TypeDeclarations = {}
    iterator = SafeElementTree.iterparse(source, events=("start", "end"))
    first_event, root = next(iterator)
    root_seen = first_event == "start"
    if not root_seen or _local_name(root.tag) != "log":
        raise conversion_error("XML root", "expected <log>")
    records = {"event", "object", "event-type", "object-type"}
    required_collections = {"event-types", "object-types", "events", "objects"}
    collections: set[str] = set()
    depth = 0
    for event, element in iterator:
        name = _local_name(element.tag)
        if event == "start":
            if depth == 0 and name in required_collections:
                collections.add(name)
            depth += 1
            continue
        if name != "log":
            depth -= 1
        if event != "end":
            continue
        if name not in {"event-type", "object-type"}:
            if name in records:
                element.clear()
                root.clear()
            continue
        context = name
        type_name = required_string(element.get("name"), f"{context}.name")
        target = event_types if name == "event-type" else object_types
        if type_name in target:
            raise conversion_error(context, f"duplicate type declaration {type_name!r}")
        attributes: dict[str, AttributeType] = {}
        for index, attribute in enumerate(
            _section_items(element, "attributes", "attribute")
        ):
            attribute_context = f"{context}[{type_name!r}].attributes[{index}]"
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
        target[type_name] = attributes
        element.clear()
        root.clear()
    if not root_seen:
        raise conversion_error("XML document", "is empty")
    missing = sorted(required_collections - collections)
    if missing:
        raise conversion_error(
            "XML root",
            f"missing required collections: {missing}",
        )
    return event_types, object_types


def _convert_events(
    source: Path,
    sink: TableSink,
    schema: ExchangeSchema,
    relations: RelationIndex,
) -> None:
    for index, event in enumerate(_elements(source, "event")):
        context = f"events/event[{index}]"
        event_id = required_string(event.get("id"), f"{context}.id")
        type_name = required_string(event.get("type"), f"{context}.type")
        declarations = require_declared_type(
            schema.event_types,
            type_name,
            context=f"{context}.type",
        )
        row: dict[str, Any] = {
            s.OCEL_ID: event_id,
            s.OCEL_TIME: timestamp(event.get("time"), context=f"{context}.time"),
            s.OCEL_TYPE: type_name,
        }
        for attribute_index, attribute in enumerate(
            _section_items(event, "attributes", "attribute")
        ):
            attribute_context = f"{context}/attributes/attribute[{attribute_index}]"
            name = required_string(
                attribute.get("name"),
                f"{attribute_context}.name",
            )
            if name in row:
                raise conversion_error(
                    attribute_context,
                    f"duplicate attribute value {name!r}",
                )
            declared = _declared_attribute(
                declarations,
                name,
                context=attribute_context,
            )
            row[name] = attribute_value(
                attribute.text if attribute.text is not None else "",
                declared,
                schema.event_attributes[name],
                style="xml",
                context=f"{attribute_context}.value",
            )
        sink.add("events", row)
        for relation_index, relation in enumerate(
            _section_items(event, "objects", "relationship")
        ):
            relation_context = f"{context}/objects/relationship[{relation_index}]"
            relations.add_e2o(
                event_id,
                type_name,
                _relationship_target(
                    relation,
                    relation_context,
                ),
                _relationship_qualifier(
                    relation,
                    relation_context,
                ),
            )


def _convert_objects(
    source: Path,
    sink: TableSink,
    schema: ExchangeSchema,
    relations: RelationIndex,
) -> None:
    for index, obj in enumerate(_elements(source, "object")):
        context = f"objects/object[{index}]"
        object_id = required_string(obj.get("id"), f"{context}.id")
        type_name = required_string(obj.get("type"), f"{context}.type")
        declarations = require_declared_type(
            schema.object_types,
            type_name,
            context=f"{context}.type",
        )
        sink.add(
            "objects",
            {
                s.OCEL_ID: object_id,
                s.OCEL_TYPE: type_name,
            },
        )
        relations.add_object(object_id, type_name)
        initial: dict[str, Any] = {}
        for attribute_index, attribute in enumerate(
            _section_items(obj, "attributes", "attribute")
        ):
            attribute_context = f"{context}/attributes/attribute[{attribute_index}]"
            name = required_string(
                attribute.get("name"),
                f"{attribute_context}.name",
            )
            declared = _declared_attribute(
                declarations,
                name,
                context=attribute_context,
            )
            changed_at = timestamp(
                attribute.get("time"),
                context=f"{attribute_context}.time",
            )
            value = attribute_value(
                attribute.text if attribute.text is not None else "",
                declared,
                schema.object_attributes[name],
                style="xml",
                context=f"{attribute_context}.value",
            )
            if changed_at == EPOCH:
                if name in initial:
                    raise conversion_error(
                        attribute_context,
                        f"duplicate initial value for attribute {name!r}",
                    )
                initial[name] = value
                continue
            sink.add(
                "object_changes",
                {
                    s.OCEL_ID: object_id,
                    s.OCEL_TIME: changed_at,
                    s.OCEL_CHANGED_FIELD: name,
                    s.OCEL_IS_INITIAL: False,
                    s.OCEL_TYPE: type_name,
                    name: value,
                },
            )
        if initial:
            sink.add(
                "object_changes",
                {
                    s.OCEL_ID: object_id,
                    s.OCEL_TIME: EPOCH,
                    s.OCEL_CHANGED_FIELD: None,
                    s.OCEL_IS_INITIAL: True,
                    s.OCEL_TYPE: type_name,
                    **initial,
                },
            )
        for relation_index, relation in enumerate(
            _section_items(obj, "objects", "relationship")
        ):
            relation_context = f"{context}/objects/relationship[{relation_index}]"
            relations.add_o2o(
                object_id,
                type_name,
                _relationship_target(
                    relation,
                    relation_context,
                ),
                _relationship_qualifier(
                    relation,
                    relation_context,
                ),
            )


def _elements(source: Path, wanted: str) -> Iterator[ElementTree.Element]:
    iterator = SafeElementTree.iterparse(source, events=("start", "end"))
    _, root = next(iterator)
    if _local_name(root.tag) != "log":
        raise conversion_error("XML root", "expected <log>")
    records = {"event", "object", "event-type", "object-type"}
    for event, element in iterator:
        if event != "end":
            continue
        name = _local_name(element.tag)
        if name == wanted:
            yield element
            element.clear()
            root.clear()
        elif name in records:
            element.clear()
            root.clear()


def _section_items(
    parent: ElementTree.Element,
    section_name: str,
    item_name: str,
) -> list[ElementTree.Element]:
    for child in parent:
        if _local_name(child.tag) != section_name:
            continue
        return [item for item in child if _local_name(item.tag) == item_name]
    return []


def _declared_attribute(
    declarations: Mapping[str, AttributeType],
    name: str,
    *,
    context: str,
) -> AttributeType:
    declared = declarations.get(name)
    if declared is None:
        raise conversion_error(context, f"undeclared attribute {name!r}")
    return declared


def _relationship_target(
    relation: ElementTree.Element,
    context: str,
) -> str:
    value = relation.get("object-id")
    if value is None:
        value = relation.get("objectId")
    return required_string(value, f"{context}.object-id")


def _relationship_qualifier(
    relation: ElementTree.Element,
    context: str,
) -> str:
    qualifier = relation.get("qualifier")
    relationship = relation.get("relationship")
    if qualifier is not None and relationship is not None and qualifier != relationship:
        raise conversion_error(
            context,
            "qualifier and relationship attributes disagree",
        )
    value = qualifier if qualifier is not None else relationship
    if not isinstance(value, str):
        raise conversion_error(
            f"{context}.qualifier",
            "must be a string",
        )
    return value


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]
