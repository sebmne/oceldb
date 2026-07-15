"""Write OCEL 2.0 XML documents."""

import xml.etree.ElementTree as ET
from collections.abc import Mapping
from pathlib import Path

from oceldb.io._paths import atomic_file
from oceldb.io.errors import ValidationMode, check_validation_mode
from oceldb.io.exchange._common import prepare_exchange
from oceldb.schema import TypeAttributes
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
    data = prepare_exchange(ocel, style="xml", validation=validation)
    root = ET.Element("log")
    _write_type_declarations(root, "event-types", "event-type", data.schema.event_types)
    _write_type_declarations(
        root, "object-types", "object-type", data.schema.object_types
    )

    events_element = ET.SubElement(root, "events")
    for event in data.events:
        event_element = ET.SubElement(
            events_element,
            "event",
            {
                "id": event.id,
                "type": event.type,
                "time": event.time,
            },
        )
        attributes_element = ET.SubElement(event_element, "attributes")
        for attribute in event.attributes:
            attr_element = ET.SubElement(
                attributes_element, "attribute", {"name": attribute.name}
            )
            attr_element.text = str(attribute.value)
        objects_element = ET.SubElement(event_element, "objects")
        for relation in event.relationships:
            ET.SubElement(
                objects_element,
                "relationship",
                {
                    "object-id": relation.object_id,
                    "qualifier": relation.qualifier,
                },
            )

    objects_element = ET.SubElement(root, "objects")
    for obj in data.objects:
        object_element = ET.SubElement(
            objects_element, "object", {"id": obj.id, "type": obj.type}
        )
        attributes_element = ET.SubElement(object_element, "attributes")
        for attribute in obj.attributes:
            if attribute.time is None:
                continue
            attr_element = ET.SubElement(
                attributes_element,
                "attribute",
                {"name": attribute.name, "time": attribute.time},
            )
            attr_element.text = str(attribute.value)
        related_element = ET.SubElement(object_element, "objects")
        for relation in obj.relationships:
            ET.SubElement(
                related_element,
                "relationship",
                {
                    "object-id": relation.object_id,
                    "qualifier": relation.qualifier,
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
