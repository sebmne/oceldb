"""Write OCEL 2.0 XML documents."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from oceldb import schema as s
from oceldb.io._paths import atomic_file
from oceldb.io._schema import materialize, validate_for_exchange
from oceldb.io._values import encode_attribute, format_datetime, qualifier
from oceldb.io.errors import ValidationMode, check_validation_mode
from oceldb.schema import AttributeType, TypeAttributes
from oceldb.ocel import OCEL


def write_xml(
    ocel: OCEL,
    path: str | Path,
    *,
    overwrite: bool = False,
    validation: ValidationMode = "strict",
) -> None:
    """Write a schema-complete OCEL 2.0 XML document."""
    validation = check_validation_mode(validation)
    data = materialize(ocel)
    validate_for_exchange(data, validation)
    root = ET.Element("log")
    _write_type_declarations(root, "event-types", "event-type", data.schema.event_types)
    _write_type_declarations(
        root, "object-types", "object-type", data.schema.object_types
    )

    e2o: dict[str, list[dict[str, Any]]] = {}
    for relation in data.e2o.iter_rows(named=True):
        e2o.setdefault(str(relation[s.OCEL_EVENT_ID]), []).append(relation)
    events_element = ET.SubElement(root, "events")
    for row in data.events.sort(s.OCEL_TIME, s.OCEL_ID).iter_rows(named=True):
        event_id = str(row[s.OCEL_ID])
        type_name = str(row[s.OCEL_TYPE])
        event_element = ET.SubElement(
            events_element,
            "event",
            {
                "id": event_id,
                "type": type_name,
                "time": format_datetime(row[s.OCEL_TIME]),
            },
        )
        attributes_element = ET.SubElement(event_element, "attributes")
        for name, attr_type in data.schema.event_types.get(type_name, {}).items():
            value = row.get(name)
            if value is None:
                continue
            encoded = encode_attribute(
                value,
                attr_type,
                style="xml",
                validation=validation,
                context=f"event {event_id!r} attribute {name!r}",
            )
            if encoded is None:
                continue
            attr_element = ET.SubElement(
                attributes_element, "attribute", {"name": name}
            )
            attr_element.text = str(encoded)
        objects_element = ET.SubElement(event_element, "objects")
        for relation in e2o.get(event_id, []):
            ET.SubElement(
                objects_element,
                "relationship",
                {
                    "object-id": str(relation[s.OCEL_OBJECT_ID]),
                    "qualifier": qualifier(
                        relation[s.OCEL_QUALIFIER],
                        validation=validation,
                        context=f"E2O {event_id!r}",
                    ),
                },
            )

    changes: dict[str, list[dict[str, Any]]] = {}
    for row in data.object_changes.sort(
        s.OCEL_ID, s.OCEL_TIME, s.OCEL_CHANGED_FIELD
    ).iter_rows(named=True):
        changes.setdefault(str(row[s.OCEL_ID]), []).append(row)
    o2o: dict[str, list[dict[str, Any]]] = {}
    for relation in data.o2o.iter_rows(named=True):
        o2o.setdefault(str(relation[s.OCEL_SOURCE_ID]), []).append(relation)

    objects_element = ET.SubElement(root, "objects")
    for row in data.objects.sort(s.OCEL_ID).iter_rows(named=True):
        object_id = str(row[s.OCEL_ID])
        type_name = str(row[s.OCEL_TYPE])
        object_element = ET.SubElement(
            objects_element, "object", {"id": object_id, "type": type_name}
        )
        attributes_element = ET.SubElement(object_element, "attributes")
        declarations = data.schema.object_types.get(type_name, {})
        for change in changes.get(object_id, []):
            changed = change.get(s.OCEL_CHANGED_FIELD)
            names = [str(changed)] if changed else list(declarations)
            for name in names:
                value = change.get(name)
                if value is None:
                    continue
                attr_type = declarations.get(name, AttributeType.STRING)
                encoded = encode_attribute(
                    value,
                    attr_type,
                    style="xml",
                    validation=validation,
                    context=f"object {object_id!r} attribute {name!r}",
                )
                if encoded is None:
                    continue
                attr_element = ET.SubElement(
                    attributes_element,
                    "attribute",
                    {"name": name, "time": format_datetime(change[s.OCEL_TIME])},
                )
                attr_element.text = str(encoded)
        related_element = ET.SubElement(object_element, "objects")
        for relation in o2o.get(object_id, []):
            ET.SubElement(
                related_element,
                "relationship",
                {
                    "object-id": str(relation[s.OCEL_TARGET_ID]),
                    "qualifier": qualifier(
                        relation[s.OCEL_QUALIFIER],
                        validation=validation,
                        context=f"O2O {object_id!r}",
                    ),
                },
            )

    ET.indent(root, space="  ")
    tree = ET.ElementTree(root)
    with atomic_file(path, overwrite=overwrite) as (_, staging):
        tree.write(staging, encoding="utf-8", xml_declaration=True)


def _write_type_declarations(
    root: ET.Element,
    collection: str,
    item_name: str,
    types: Mapping[str, TypeAttributes],
) -> None:
    collection_element = ET.SubElement(root, collection)
    for type_name, attributes in sorted(types.items()):
        type_element = ET.SubElement(collection_element, item_name, {"name": type_name})
        attributes_element = ET.SubElement(type_element, "attributes")
        for name, attr_type in sorted(attributes.items()):
            ET.SubElement(
                attributes_element,
                "attribute",
                {"name": name, "type": attr_type.value},
            )
