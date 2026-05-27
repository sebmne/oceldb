"""OCEL 2.0 XML source."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from xml.etree.ElementTree import Element, iterparse

from oceldb.io.registry import ConverterSpec
from oceldb.io.source import Canonical, ocel_to_duckdb, relation_if_nonempty
from oceldb.io.sql import quote_identifier, sql_string

if TYPE_CHECKING:
    import duckdb


class XmlSource:
    """Attach a streaming XML parser to DuckDB."""

    BATCH_SIZE = 5000

    def __init__(self, source: str | Path) -> None:
        path = Path(source)
        if not path.exists():
            raise FileNotFoundError(f"Source not found: {path}")
        self.path = path

    def attach(self, con: "duckdb.DuckDBPyConnection") -> Canonical:
        _create_tables(con)
        event_types, object_types = _parse(self.path, con, self.BATCH_SIZE)
        _validate_relationships(con)
        return _build_canonical(con, event_types, object_types)

    def release(self, con: "duckdb.DuckDBPyConnection") -> None:
        for name in (
            "_xml_events_base",
            "_xml_objects_base",
            "_xml_event_attrs",
            "_xml_object_attr_changes",
            "_xml_eo_raw",
            "_xml_oo_raw",
        ):
            con.execute(f"DROP TABLE IF EXISTS {name}")


def _create_tables(con: "duckdb.DuckDBPyConnection") -> None:
    con.execute(
        "CREATE TEMP TABLE _xml_events_base "
        "(ocel_id VARCHAR, ocel_type VARCHAR, ocel_time VARCHAR)"
    )
    con.execute(
        "CREATE TEMP TABLE _xml_objects_base (ocel_id VARCHAR, ocel_type VARCHAR)"
    )
    con.execute(
        "CREATE TEMP TABLE _xml_event_attrs "
        "(ocel_event_id VARCHAR, attr_name VARCHAR, attr_value VARCHAR)"
    )
    con.execute(
        "CREATE TEMP TABLE _xml_object_attr_changes "
        "(ocel_id VARCHAR, ocel_type VARCHAR, attr_time VARCHAR, "
        " attr_name VARCHAR, attr_value VARCHAR)"
    )
    con.execute(
        "CREATE TEMP TABLE _xml_eo_raw "
        "(ocel_event_id VARCHAR, ocel_object_id VARCHAR, ocel_qualifier VARCHAR)"
    )
    con.execute(
        "CREATE TEMP TABLE _xml_oo_raw "
        "(ocel_source_id VARCHAR, ocel_target_id VARCHAR, ocel_qualifier VARCHAR)"
    )


def _parse(
    path: Path, con: "duckdb.DuckDBPyConnection", batch_size: int
) -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]]]:
    event_types: dict[str, dict[str, str]] = {}
    object_types: dict[str, dict[str, str]] = {}

    buffers: dict[str, list[tuple[object, ...]]] = {
        "events_base": [],
        "objects_base": [],
        "event_attrs": [],
        "object_attr_changes": [],
        "eo_raw": [],
        "oo_raw": [],
    }

    def maybe_flush() -> None:
        if any(len(b) >= batch_size for b in buffers.values()):
            _flush(con, buffers)

    # Depth distinguishes top-level entries from nested relationships.
    events_depth = 0
    objects_depth = 0

    for ev, elem in iterparse(str(path), events=("start", "end")):
        tag = _local(elem.tag)

        if ev == "start":
            if events_depth > 0:
                events_depth += 1
            elif tag == "events":
                events_depth = 1
            elif objects_depth > 0:
                objects_depth += 1
            elif tag == "objects":
                objects_depth = 1
            continue

        if tag == "event-type":
            _read_type(elem, event_types, kind="event")
            elem.clear()
            continue
        if tag == "object-type":
            _read_type(elem, object_types, kind="object")
            elem.clear()
            continue

        if events_depth > 0:
            if events_depth == 2 and tag == "event":
                _emit_event(elem, buffers)
                elem.clear()
                maybe_flush()
            events_depth -= 1
            continue

        if objects_depth > 0:
            if objects_depth == 2 and tag == "object":
                _emit_object(elem, buffers)
                elem.clear()
                maybe_flush()
            objects_depth -= 1
            continue

    _flush(con, buffers)
    return event_types, object_types


def _read_type(elem: Element, dest: dict[str, dict[str, str]], *, kind: str) -> None:
    name = elem.attrib.get("name")
    if not name:
        raise ValueError(f"Missing name on {kind}-type element.")
    attrs: dict[str, str] = {}
    attrs_node = _find_child(elem, "attributes")
    if attrs_node is not None:
        for attr in attrs_node:
            if _local(attr.tag) != "attribute":
                continue
            attr_name = attr.attrib.get("name")
            attr_type = attr.attrib.get("type")
            if not attr_name or not attr_type:
                raise ValueError(f"Malformed attribute on {kind} type {name!r}.")
            attrs[attr_name] = ocel_to_duckdb(attr_type)
    dest[name] = attrs


def _emit_event(elem: Element, buffers: dict[str, list[tuple[object, ...]]]) -> None:
    event_id = elem.attrib.get("id")
    event_type = elem.attrib.get("type")
    event_time = elem.attrib.get("time")
    if not event_id or not event_type or not event_time:
        raise ValueError("Event missing id/type/time.")
    buffers["events_base"].append((event_id, event_type, event_time))

    attrs_node = _find_child(elem, "attributes")
    if attrs_node is not None:
        for attr in attrs_node:
            if _local(attr.tag) != "attribute":
                continue
            attr_name = attr.attrib.get("name")
            if not attr_name:
                continue
            buffers["event_attrs"].append((event_id, attr_name, attr.text or ""))

    rels_node = _find_child(elem, "objects")
    if rels_node is not None:
        for child in rels_node:
            local = _local(child.tag)
            if local not in ("object", "relationship"):
                continue
            object_id = child.attrib.get("object-id")
            if not object_id:
                continue
            qualifier = child.attrib.get("qualifier", child.attrib.get("relationship"))
            buffers["eo_raw"].append((event_id, object_id, qualifier))


def _emit_object(elem: Element, buffers: dict[str, list[tuple[object, ...]]]) -> None:
    object_id = elem.attrib.get("id")
    object_type = elem.attrib.get("type")
    if not object_id or not object_type:
        raise ValueError("Object missing id/type.")
    buffers["objects_base"].append((object_id, object_type))

    attrs_node = _find_child(elem, "attributes")
    if attrs_node is not None:
        for attr in attrs_node:
            if _local(attr.tag) != "attribute":
                continue
            attr_name = attr.attrib.get("name")
            attr_time = attr.attrib.get("time")
            if not attr_name or not attr_time:
                continue
            buffers["object_attr_changes"].append(
                (object_id, object_type, attr_time, attr_name, attr.text or "")
            )

    rels_node = _find_child(elem, "objects")
    if rels_node is not None:
        for child in rels_node:
            local = _local(child.tag)
            if local not in ("object", "relationship"):
                continue
            target_id = child.attrib.get("object-id")
            if not target_id:
                continue
            qualifier = child.attrib.get("qualifier", child.attrib.get("relationship"))
            buffers["oo_raw"].append((object_id, target_id, qualifier))


def _flush(
    con: "duckdb.DuckDBPyConnection",
    buffers: dict[str, list[tuple[object, ...]]],
) -> None:
    inserts = {
        "events_base": "INSERT INTO _xml_events_base VALUES (?, ?, ?)",
        "objects_base": "INSERT INTO _xml_objects_base VALUES (?, ?)",
        "event_attrs": "INSERT INTO _xml_event_attrs VALUES (?, ?, ?)",
        "object_attr_changes": (
            "INSERT INTO _xml_object_attr_changes VALUES (?, ?, ?, ?, ?)"
        ),
        "eo_raw": "INSERT INTO _xml_eo_raw VALUES (?, ?, ?)",
        "oo_raw": "INSERT INTO _xml_oo_raw VALUES (?, ?, ?)",
    }
    for key, sql in inserts.items():
        rows = buffers[key]
        if rows:
            con.executemany(sql, rows)
            rows.clear()


def _validate_relationships(con: "duckdb.DuckDBPyConnection") -> None:
    row = con.execute("""
        SELECT eo.ocel_event_id, NULL
        FROM _xml_eo_raw eo
        WHERE eo.ocel_object_id NOT IN (SELECT ocel_id FROM _xml_objects_base)
        UNION ALL
        SELECT NULL, oo.ocel_source_id
        FROM _xml_oo_raw oo
        WHERE oo.ocel_target_id NOT IN (SELECT ocel_id FROM _xml_objects_base)
        LIMIT 1
    """).fetchone()
    if row is not None:
        event_id, source_id = row
        if event_id is not None:
            raise ValueError(f"Event {event_id!r} references unknown object.")
        raise ValueError(f"Object {source_id!r} references unknown object.")


def _build_canonical(
    con: "duckdb.DuckDBPyConnection",
    event_types: dict[str, dict[str, str]],
    object_types: dict[str, dict[str, str]],
) -> Canonical:
    def events_for(t: str) -> "duckdb.DuckDBPyRelation":
        attrs = event_types[t]
        if not attrs:
            return con.sql(f"""
                SELECT ocel_id, CAST(ocel_time AS TIMESTAMP) AS ocel_time
                FROM _xml_events_base
                WHERE ocel_type = {sql_string(t)}
            """)
        proj = "e.ocel_id AS ocel_id, CAST(e.ocel_time AS TIMESTAMP) AS ocel_time"
        for name, dtype in attrs.items():
            proj += (
                f", CAST(MAX(CASE WHEN a.attr_name = {sql_string(name)} "
                f"THEN a.attr_value END) AS {dtype}) AS {quote_identifier(name)}"
            )
        return con.sql(f"""
            SELECT {proj}
            FROM _xml_events_base e
            LEFT JOIN _xml_event_attrs a ON a.ocel_event_id = e.ocel_id
            WHERE e.ocel_type = {sql_string(t)}
            GROUP BY e.ocel_id, e.ocel_time
        """)

    def objects_for(t: str) -> "duckdb.DuckDBPyRelation":
        return con.sql(
            f"SELECT ocel_id FROM _xml_objects_base WHERE ocel_type = {sql_string(t)}"
        )

    def object_changes_for(t: str) -> "duckdb.DuckDBPyRelation":
        attrs = object_types[t]
        if not attrs:
            return con.sql(f"""
                SELECT
                    ocel_id,
                    TIMESTAMP '1970-01-01 00:00:00' AS ocel_time,
                    CAST(NULL AS VARCHAR) AS ocel_changed_field
                FROM _xml_objects_base
                WHERE ocel_type = {sql_string(t)}
            """)
        epoch_cols = ", ".join(
            f"CAST(MAX(CASE WHEN a.attr_name = {sql_string(name)} "
            f"THEN a.attr_value END) AS {dtype}) AS {quote_identifier(name)}"
            for name, dtype in attrs.items()
        )
        non_epoch_cols = ", ".join(
            f"CAST(CASE WHEN a.attr_name = {sql_string(name)} "
            f"THEN a.attr_value END AS {dtype}) AS {quote_identifier(name)}"
            for name, dtype in attrs.items()
        )
        return con.sql(f"""
            WITH all_attrs AS (
                SELECT
                    ocel_id,
                    CAST(attr_time AS TIMESTAMP) AS attr_time,
                    attr_name,
                    attr_value
                FROM _xml_object_attr_changes
                WHERE ocel_type = {sql_string(t)}
            ),
            epoch_rows AS (
                SELECT
                    ocel_id,
                    TIMESTAMP '1970-01-01 00:00:00' AS ocel_time,
                    CAST(NULL AS VARCHAR) AS ocel_changed_field,
                    {epoch_cols}
                FROM all_attrs a
                WHERE attr_time = TIMESTAMP '1970-01-01 00:00:00'
                GROUP BY ocel_id
            ),
            non_epoch_rows AS (
                SELECT
                    ocel_id,
                    attr_time AS ocel_time,
                    attr_name AS ocel_changed_field,
                    {non_epoch_cols}
                FROM all_attrs a
                WHERE attr_time <> TIMESTAMP '1970-01-01 00:00:00'
            )
            SELECT * FROM epoch_rows
            UNION ALL
            SELECT * FROM non_epoch_rows
        """)

    event_object = con.sql("""
        SELECT
            eo.ocel_event_id,
            e.ocel_type AS ocel_event_type,
            eo.ocel_object_id,
            o.ocel_type AS ocel_object_type,
            eo.ocel_qualifier
        FROM _xml_eo_raw eo
        JOIN _xml_events_base e ON e.ocel_id = eo.ocel_event_id
        JOIN _xml_objects_base o ON o.ocel_id = eo.ocel_object_id
    """)

    object_object = relation_if_nonempty(
        con,
        """
        SELECT
            oo.ocel_source_id,
            s.ocel_type AS ocel_source_type,
            oo.ocel_target_id,
            t.ocel_type AS ocel_target_type,
            oo.ocel_qualifier
        FROM _xml_oo_raw oo
        JOIN _xml_objects_base s ON s.ocel_id = oo.ocel_source_id
        JOIN _xml_objects_base t ON t.ocel_id = oo.ocel_target_id
    """,
    )

    return Canonical(
        event_types=event_types,
        object_types=object_types,
        events_for=events_for,
        objects_for=objects_for,
        object_changes_for=object_changes_for,
        event_object=event_object,
        object_object=object_object,
    )


def _local(tag: str) -> str:
    return tag.rsplit("}", maxsplit=1)[-1]


def _find_child(elem: Element, name: str) -> Element | None:
    for child in elem:
        if _local(child.tag) == name:
            return child
    return None


SPEC = ConverterSpec(
    format="xml",
    source_factory=XmlSource,
    extensions=(".xml", ".xmlocel"),
)
