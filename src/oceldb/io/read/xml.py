"""Read OCEL 2.0 XML logs into an in-memory OCEL."""

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from oceldb import schema as s
from oceldb.io.read._common import _EPOCH, build_ocel
from oceldb.ocel import OCEL

# Maps OCEL 2.0 declared attribute types to Python callables for value casting.
_CASTERS: dict[str, Any] = {
    "float": float,
    "integer": int,
    "boolean": lambda v: v.strip().lower() in ("true", "1", "yes"),
}


def read_xml(path: str | Path) -> OCEL:
    """Read an OCEL 2.0 XML file into an in-memory :class:`~oceldb.OCEL`.

    Parses the standard OCEL 2.0 XML format (``<log>`` root with
    ``<object-types>``, ``<event-types>``, ``<objects>``, and ``<events>``
    sections). Attribute types declared in the schema are used to cast string
    values to their proper Python types.

    The entire file is parsed eagerly and held **in memory**. For very large
    logs, prefer the SQLite format via :func:`read_sqlite`, which produces
    file-backed lazy frames instead.

    Args:
        path: Path to an ``.xml`` / ``.xmlocel`` file.

    Returns:
        An ``OCEL`` backed by in-memory Polars lazy frames.

    Raises:
        FileNotFoundError: If ``path`` does not exist.
        ET.ParseError: If the file is not valid XML.

    Examples:
        >>> from oceldb.io import read_xml
        >>> ocel = read_xml("log.xmlocel")
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    root = ET.parse(path).getroot()

    # Build per-type attribute type maps from the schema sections
    obj_attr_types: dict[str, dict[str, str]] = {}
    for ot in root.findall("./object-types/object-type"):
        name = ot.get("name", "")
        obj_attr_types[name] = {
            attr.get("name", ""): attr.get("type", "string")
            for attr in ot.findall("./attributes/attribute")
        }

    evt_attr_types: dict[str, dict[str, str]] = {}
    for et in root.findall("./event-types/event-type"):
        name = et.get("name", "")
        evt_attr_types[name] = {
            attr.get("name", ""): attr.get("type", "string")
            for attr in et.findall("./attributes/attribute")
        }

    # First pass over objects to build id→type lookup for relationship targets
    obj_type: dict[str, str] = {
        obj.get("id", ""): obj.get("type", "")
        for obj in root.findall("./objects/object")
    }

    event_rows: list[dict[str, Any]] = []
    e2o_rows: list[dict[str, Any]] = []
    for ev in root.findall("./events/event"):
        ev_id = ev.get("id", "")
        ev_type = ev.get("type", "")
        row: dict[str, Any] = {
            s.OCEL_ID: ev_id,
            s.OCEL_TYPE: ev_type,
            s.OCEL_TIME: ev.get("time", ""),
        }
        type_schema = evt_attr_types.get(ev_type, {})
        for attr in ev.findall("./attributes/attribute"):
            name = attr.get("name", "")
            row[name] = _cast(attr.get("value", ""), type_schema.get(name, "string"))
        event_rows.append(row)
        for rel in ev.findall("./objects/relationship"):
            obj_id = rel.get("objectId", "")
            e2o_rows.append(
                {
                    s.OCEL_EVENT_ID: ev_id,
                    s.OCEL_EVENT_TYPE: ev_type,
                    s.OCEL_OBJECT_ID: obj_id,
                    s.OCEL_OBJECT_TYPE: obj_type.get(obj_id, ""),
                    s.OCEL_QUALIFIER: rel.get("qualifier", ""),
                }
            )

    object_rows: list[dict[str, Any]] = []
    oc_rows: list[dict[str, Any]] = []
    o2o_rows: list[dict[str, Any]] = []
    for obj in root.findall("./objects/object"):
        obj_id = obj.get("id", "")
        obj_t = obj.get("type", "")
        object_rows.append({s.OCEL_ID: obj_id, s.OCEL_TYPE: obj_t})
        type_schema = obj_attr_types.get(obj_t, {})
        for attr in obj.findall("./attributes/attribute"):
            name = attr.get("name", "")
            oc_rows.append(
                {
                    s.OCEL_ID: obj_id,
                    s.OCEL_TYPE: obj_t,
                    s.OCEL_TIME: attr.get("time", _EPOCH),
                    s.OCEL_CHANGED_FIELD: name,
                    name: _cast(attr.get("value", ""), type_schema.get(name, "string")),
                }
            )
        for rel in obj.findall("./objects/relationship"):
            target = rel.get("objectId", "")
            o2o_rows.append(
                {
                    s.OCEL_SOURCE_ID: obj_id,
                    s.OCEL_SOURCE_TYPE: obj_t,
                    s.OCEL_TARGET_ID: target,
                    s.OCEL_TARGET_TYPE: obj_type.get(target, ""),
                    s.OCEL_QUALIFIER: rel.get("qualifier", ""),
                }
            )

    return build_ocel(
        event_rows=event_rows,
        object_rows=object_rows,
        object_change_rows=oc_rows,
        e2o_rows=e2o_rows,
        o2o_rows=o2o_rows,
    )


def _cast(value: str, attr_type: str) -> Any:
    caster = _CASTERS.get(attr_type)
    if caster is None:
        return value
    try:
        return caster(value)
    except (ValueError, AttributeError):
        return value
