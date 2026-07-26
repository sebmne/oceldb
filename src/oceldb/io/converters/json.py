"""Streaming OCEL 2.0 JSON-to-native conversion."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import ijson

from oceldb import OCEL
from oceldb.core import schema as s
from oceldb.io._errors import OCELConversionError, conversion_error
from oceldb.io._schema import (
    AttributeType,
    ExchangeSchema,
    declarations_from_json,
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
    """Convert one JSON exchange file using bounded row buffers."""
    try:
        declaration_items: dict[str, list[object]] = {
            "eventTypes": [],
            "objectTypes": [],
        }
        for section, item in _selected_top_level_items(
            source,
            {"eventTypes", "objectTypes"},
        ):
            declaration_items[section].append(item)
        event_types = declarations_from_json(
            declaration_items["eventTypes"],
            kind="event",
        )
        object_types = declarations_from_json(
            declaration_items["objectTypes"],
            kind="object",
        )
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
    except ijson.JSONError as exc:
        raise OCELConversionError(f"Invalid OCEL JSON document: {exc}") from exc


def _convert_events(
    source: Path,
    sink: TableSink,
    schema: ExchangeSchema,
    relations: RelationIndex,
) -> None:
    for index, raw in enumerate(_items(source, "events.item")):
        context = f"events[{index}]"
        event = _object(raw, context)
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
        _event_attributes(
            event.get("attributes", []),
            row,
            declarations,
            schema,
            context,
        )
        sink.add("events", row)
        for relation_index, raw_relation in enumerate(
            _array(event.get("relationships", []), f"{context}.relationships")
        ):
            relation_context = f"{context}.relationships[{relation_index}]"
            relation = _object(raw_relation, relation_context)
            qualifier = relation.get("qualifier")
            if not isinstance(qualifier, str):
                raise conversion_error(
                    f"{relation_context}.qualifier",
                    "must be a string",
                )
            relations.add_e2o(
                event_id,
                type_name,
                required_string(
                    relation.get("objectId"),
                    f"{relation_context}.objectId",
                ),
                qualifier,
            )


def _convert_objects(
    source: Path,
    sink: TableSink,
    schema: ExchangeSchema,
    relations: RelationIndex,
) -> None:
    for index, raw in enumerate(_items(source, "objects.item")):
        context = f"objects[{index}]"
        obj = _object(raw, context)
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
        for attribute_index, raw_attribute in enumerate(
            _array(obj.get("attributes", []), f"{context}.attributes")
        ):
            attribute_context = f"{context}.attributes[{attribute_index}]"
            attribute = _object(raw_attribute, attribute_context)
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
                attribute.get("value"),
                declared,
                schema.object_attributes[name],
                style="json",
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
        for relation_index, raw_relation in enumerate(
            _array(obj.get("relationships", []), f"{context}.relationships")
        ):
            relation_context = f"{context}.relationships[{relation_index}]"
            relation = _object(raw_relation, relation_context)
            qualifier = relation.get("qualifier")
            if not isinstance(qualifier, str):
                raise conversion_error(
                    f"{relation_context}.qualifier",
                    "must be a string",
                )
            relations.add_o2o(
                object_id,
                type_name,
                required_string(
                    relation.get("objectId"),
                    f"{relation_context}.objectId",
                ),
                qualifier,
            )


def _event_attributes(
    raw: object,
    row: dict[str, Any],
    declarations: Mapping[str, AttributeType],
    schema: ExchangeSchema,
    parent: str,
) -> None:
    for index, raw_attribute in enumerate(_array(raw, f"{parent}.attributes")):
        context = f"{parent}.attributes[{index}]"
        attribute = _object(raw_attribute, context)
        name = required_string(attribute.get("name"), f"{context}.name")
        if name in row:
            raise conversion_error(context, f"duplicate attribute value {name!r}")
        declared = _declared_attribute(declarations, name, context=context)
        row[name] = attribute_value(
            attribute.get("value"),
            declared,
            schema.event_attributes[name],
            style="json",
            context=f"{context}.value",
        )


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


def _items(path: Path, prefix: str) -> Iterator[object]:
    with path.open("rb") as stream:
        yield from ijson.items(stream, prefix, use_float=True)


def _selected_top_level_items(
    path: Path,
    selected: set[str],
) -> Iterator[tuple[str, object]]:
    required = {"eventTypes", "objectTypes", "events", "objects"}
    keys: set[str] = set()
    arrays: set[str] = set()
    builder: ijson.ObjectBuilder | None = None
    section: str | None = None
    depth = 0
    root_started = False
    with path.open("rb") as stream:
        for prefix, event, value in ijson.parse(stream, use_float=True):
            if not root_started:
                if prefix != "" or event != "start_map":
                    raise conversion_error("JSON root", "must be an object")
                root_started = True
                continue
            if prefix == "" and event == "map_key":
                if not isinstance(value, str):
                    raise conversion_error(
                        "JSON root", "property name must be a string"
                    )
                if value in keys:
                    raise conversion_error(
                        "JSON root",
                        f"duplicate top-level property {value!r}",
                    )
                keys.add(value)
                continue
            if prefix in required and event == "start_array":
                arrays.add(prefix)
                continue
            if prefix in required and event not in {"end_array", "start_array"}:
                if not prefix.endswith(".item"):
                    raise conversion_error(prefix, "must be an array")
            matching = next(
                (
                    name
                    for name in selected
                    if prefix == f"{name}.item" or prefix.startswith(f"{name}.item.")
                ),
                None,
            )
            if matching is None:
                continue
            base = f"{matching}.item"
            if builder is None:
                if prefix != base:
                    continue
                if event in {"string", "number", "boolean", "null"}:
                    yield matching, value
                    continue
                if event not in {"start_map", "start_array"}:
                    continue
                builder = ijson.ObjectBuilder()
                section = matching
                depth = 1
                builder.event(event, value)
                continue
            builder.event(event, value)
            if event in {"start_map", "start_array"}:
                depth += 1
            elif event in {"end_map", "end_array"}:
                depth -= 1
                if depth == 0:
                    assert section is not None
                    yield section, builder.value
                    builder = None
                    section = None
    missing = sorted(required - keys)
    if missing:
        raise conversion_error("JSON root", f"missing required properties: {missing}")
    non_arrays = sorted(required - arrays)
    if non_arrays:
        raise conversion_error(
            "JSON root",
            f"required properties are not arrays: {non_arrays}",
        )


def _object(value: object, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise conversion_error(context, "must be an object")
    return value


def _array(value: object, context: str) -> list[object]:
    if not isinstance(value, list):
        raise conversion_error(context, "must be an array")
    return value
