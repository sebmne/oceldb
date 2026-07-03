"""Read OCEL 2.0 JSON logs into an in-memory OCEL."""

import json
from pathlib import Path
from typing import Any

from oceldb import schema as s
from oceldb.io.read._common import _EPOCH, build_ocel
from oceldb.ocel import OCEL


def read_json(path: str | Path) -> OCEL:
    """Read an OCEL 2.0 JSON file into an in-memory :class:`~oceldb.OCEL`.

    Parses the standard OCEL 2.0 JSON format produced by pm4py, ocelescope,
    and the reference implementation. Object attribute histories are preserved
    as time-stamped ``object_changes`` rows; object-to-object relationships are
    read from each object's ``relationships`` array.

    The entire file is parsed eagerly and held **in memory**. For very large
    logs, prefer the SQLite format via :func:`read_sqlite`, which produces
    file-backed lazy frames instead.

    Args:
        path: Path to an ``.json`` / ``.jsonocel`` file.

    Returns:
        An ``OCEL`` backed by in-memory Polars lazy frames.

    Raises:
        FileNotFoundError: If ``path`` does not exist.
        json.JSONDecodeError: If the file is not valid JSON.

    Examples:
        >>> from oceldb.io import read_json
        >>> ocel = read_json("log.jsonocel")
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    with open(path) as f:
        data = json.load(f)

    obj_type: dict[str, str] = {
        obj["id"]: obj["type"] for obj in data.get("objects", [])
    }

    event_rows: list[dict[str, Any]] = []
    e2o_rows: list[dict[str, Any]] = []
    for ev in data.get("events", []):
        row: dict[str, Any] = {
            s.OCEL_ID: ev["id"],
            s.OCEL_TYPE: ev["type"],
            s.OCEL_TIME: ev["time"],
        }
        for attr in ev.get("attributes", []):
            row[attr["name"]] = attr["value"]
        event_rows.append(row)
        for rel in ev.get("relationships", []):
            obj_id = rel["objectId"]
            e2o_rows.append(
                {
                    s.OCEL_EVENT_ID: ev["id"],
                    s.OCEL_EVENT_TYPE: ev["type"],
                    s.OCEL_OBJECT_ID: obj_id,
                    s.OCEL_OBJECT_TYPE: obj_type.get(obj_id, ""),
                    s.OCEL_QUALIFIER: rel.get("qualifier", ""),
                }
            )

    object_rows: list[dict[str, Any]] = []
    oc_rows: list[dict[str, Any]] = []
    o2o_rows: list[dict[str, Any]] = []
    for obj in data.get("objects", []):
        object_rows.append({s.OCEL_ID: obj["id"], s.OCEL_TYPE: obj["type"]})
        for attr in obj.get("attributes", []):
            oc_rows.append(
                {
                    s.OCEL_ID: obj["id"],
                    s.OCEL_TYPE: obj["type"],
                    s.OCEL_TIME: attr.get("time", _EPOCH),
                    s.OCEL_CHANGED_FIELD: attr["name"],
                    attr["name"]: attr["value"],
                }
            )
        for rel in obj.get("relationships", []):
            target = rel["objectId"]
            o2o_rows.append(
                {
                    s.OCEL_SOURCE_ID: obj["id"],
                    s.OCEL_SOURCE_TYPE: obj["type"],
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
