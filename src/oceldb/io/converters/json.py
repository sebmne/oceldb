"""OCEL 2.0 JSON source."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from oceldb.io.registry import ConverterSpec
from oceldb.io.source import Canonical, ocel_to_duckdb, relation_if_nonempty
from oceldb.io.sql import quote_identifier, sql_string

if TYPE_CHECKING:
    import duckdb


class JsonSource:
    """Attach an OCEL 2.0 JSON file as Canonical relations on DuckDB."""

    def __init__(self, source: str | Path) -> None:
        path = Path(source)
        if not path.exists():
            raise FileNotFoundError(f"Source not found: {path}")
        self.path = path

    def attach(self, con: "duckdb.DuckDBPyConnection") -> Canonical:
        # Keep values as JSON so declared attribute types control casting.
        path_literal = sql_string(str(self.path))
        con.execute(f"""
            CREATE TEMP TABLE _doc AS
            SELECT * FROM read_json(
                {path_literal},
                columns = {{
                    eventTypes: 'STRUCT(
                        name VARCHAR,
                        attributes STRUCT(name VARCHAR, "type" VARCHAR)[]
                    )[]',
                    objectTypes: 'STRUCT(
                        name VARCHAR,
                        attributes STRUCT(name VARCHAR, "type" VARCHAR)[]
                    )[]',
                    events: 'STRUCT(
                        id VARCHAR,
                        "type" VARCHAR,
                        "time" VARCHAR,
                        attributes STRUCT(name VARCHAR, value JSON)[],
                        relationships STRUCT("objectId" VARCHAR, qualifier VARCHAR)[]
                    )[]',
                    objects: 'STRUCT(
                        id VARCHAR,
                        "type" VARCHAR,
                        attributes STRUCT(name VARCHAR, "time" VARCHAR, value JSON)[],
                        relationships STRUCT("objectId" VARCHAR, qualifier VARCHAR)[]
                    )[]'
                }},
                maximum_depth = -1
            )
        """)

        event_types = _read_declared_types(con, "eventTypes")
        object_types = _read_declared_types(con, "objectTypes")

        _validate_relationships(con)

        def events_for(t: str) -> "duckdb.DuckDBPyRelation":
            attrs = event_types[t]
            return con.sql(_events_sql(t, attrs))

        def objects_for(t: str) -> "duckdb.DuckDBPyRelation":
            return con.sql(_objects_sql(t))

        def object_changes_for(t: str) -> "duckdb.DuckDBPyRelation":
            attrs = object_types[t]
            return con.sql(_object_changes_sql(t, attrs))

        event_object = con.sql(_event_object_sql())
        object_object = relation_if_nonempty(con, _object_object_sql())

        return Canonical(
            event_types=event_types,
            object_types=object_types,
            events_for=events_for,
            objects_for=objects_for,
            object_changes_for=object_changes_for,
            event_object=event_object,
            object_object=object_object,
        )

    def release(self, con: "duckdb.DuckDBPyConnection") -> None:
        con.execute("DROP TABLE IF EXISTS _doc")


def _read_declared_types(
    con: "duckdb.DuckDBPyConnection", key: str
) -> dict[str, dict[str, str]]:
    rows = con.execute(f"""
        SELECT
            t.name,
            COALESCE(
                list_transform(t.attributes, x -> {{name: x.name, type: x.type}}),
                []
            ) AS attrs
        FROM _doc, UNNEST(_doc.{quote_identifier(key)}) u(t)
    """).fetchall()

    types: dict[str, dict[str, str]] = {}
    for type_name, attrs in rows:
        type_attrs: dict[str, str] = {}
        for attr in attrs or []:  # pyright: ignore[reportUnknownVariableType]
            type_attrs[attr["name"]] = ocel_to_duckdb(attr["type"])  # pyright: ignore[reportUnknownArgumentType]
        types[type_name] = type_attrs
    return types


def _validate_relationships(con: "duckdb.DuckDBPyConnection") -> None:
    row = con.execute("""
        WITH all_obj_ids AS (
            SELECT o.id AS id
            FROM _doc, UNNEST(_doc.objects) u(o)
        ),
        ev_refs AS (
            SELECT r.objectId AS id, e.id AS event_id
            FROM _doc, UNNEST(_doc.events) ue(e), UNNEST(e.relationships) ur(r)
        ),
        obj_refs AS (
            SELECT r.objectId AS id, o.id AS source_id
            FROM _doc, UNNEST(_doc.objects) uo(o), UNNEST(o.relationships) ur(r)
        )
        SELECT event_id, NULL FROM ev_refs WHERE id NOT IN (SELECT id FROM all_obj_ids)
        UNION ALL
        SELECT NULL, source_id FROM obj_refs WHERE id NOT IN (SELECT id FROM all_obj_ids)
        LIMIT 1
    """).fetchone()
    if row is not None:
        event_id, source_id = row
        if event_id is not None:
            raise ValueError(f"Event {event_id!r} references unknown object.")
        raise ValueError(f"Object {source_id!r} references unknown object.")


def _cast_from_json(value_expr: str, duckdb_type: str) -> str:
    if duckdb_type == "VARCHAR":
        return f"json_extract_string({value_expr}, '$')"
    if duckdb_type == "TIMESTAMP":
        return f"CAST(json_extract_string({value_expr}, '$') AS TIMESTAMP)"
    return f"CAST({value_expr} AS {duckdb_type})"


def _attr_lookup(name: str, duckdb_type: str, attrs_expr: str) -> str:
    base = f"list_filter({attrs_expr}, x -> x.name = {sql_string(name)})[1].value"
    return _cast_from_json(base, duckdb_type)


def _events_sql(type_name: str, attrs: dict[str, str]) -> str:
    cols = [
        "e.id AS ocel_id",
        "CAST(e.time AS TIMESTAMP) AS ocel_time",
    ]
    for name, dtype in attrs.items():
        cols.append(
            f"{_attr_lookup(name, dtype, 'e.attributes')} AS {quote_identifier(name)}"
        )
    return f"""
        SELECT {", ".join(cols)}
        FROM _doc, UNNEST(_doc.events) u(e)
        WHERE e.type = {sql_string(type_name)}
    """


def _objects_sql(type_name: str) -> str:
    return f"""
        SELECT o.id AS ocel_id
        FROM _doc, UNNEST(_doc.objects) u(o)
        WHERE o.type = {sql_string(type_name)}
    """


def _object_changes_sql(type_name: str, attrs: dict[str, str]) -> str:
    if not attrs:
        return f"""
            SELECT
                o.id AS ocel_id,
                TIMESTAMP '1970-01-01 00:00:00' AS ocel_time,
                CAST(NULL AS VARCHAR) AS ocel_changed_field
            FROM _doc, UNNEST(_doc.objects) u(o)
            WHERE o.type = {sql_string(type_name)}
        """

    epoch_cols = ", ".join(
        f"{_cast_from_json(f'MAX(CASE WHEN a.name = {sql_string(name)} THEN a.value END)', dtype)} "
        f"AS {quote_identifier(name)}"
        for name, dtype in attrs.items()
    )
    non_epoch_cols = ", ".join(
        f"{_cast_from_json(f'CASE WHEN a.name = {sql_string(name)} THEN a.value END', dtype)} "
        f"AS {quote_identifier(name)}"
        for name, dtype in attrs.items()
    )
    return f"""
        WITH all_attrs AS (
            SELECT
                o.id AS ocel_id,
                a.name AS name,
                CAST(a.time AS TIMESTAMP) AS attr_time,
                a.value AS value
            FROM _doc,
                 UNNEST(_doc.objects) uo(o),
                 UNNEST(o.attributes) ua(a)
            WHERE o.type = {sql_string(type_name)}
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
                name AS ocel_changed_field,
                {non_epoch_cols}
            FROM all_attrs a
            WHERE attr_time <> TIMESTAMP '1970-01-01 00:00:00'
        )
        SELECT * FROM epoch_rows
        UNION ALL
        SELECT * FROM non_epoch_rows
    """


def _event_object_sql() -> str:
    return """
        WITH obj_types AS (
            SELECT o.id AS id, o.type AS type
            FROM _doc, UNNEST(_doc.objects) u(o)
        )
        SELECT
            e.id AS ocel_event_id,
            e.type AS ocel_event_type,
            r.objectId AS ocel_object_id,
            ot.type AS ocel_object_type,
            r.qualifier AS ocel_qualifier
        FROM _doc,
             UNNEST(_doc.events) ue(e),
             UNNEST(e.relationships) ur(r)
        JOIN obj_types ot ON ot.id = r.objectId
    """


def _object_object_sql() -> str:
    return """
        WITH obj_types AS (
            SELECT o.id AS id, o.type AS type
            FROM _doc, UNNEST(_doc.objects) u(o)
        )
        SELECT
            o.id AS ocel_source_id,
            o.type AS ocel_source_type,
            r.objectId AS ocel_target_id,
            ot.type AS ocel_target_type,
            r.qualifier AS ocel_qualifier
        FROM _doc,
             UNNEST(_doc.objects) uo(o),
             UNNEST(o.relationships) ur(r)
        JOIN obj_types ot ON ot.id = r.objectId
    """


SPEC = ConverterSpec(
    format="json",
    source_factory=JsonSource,
    extensions=(".json", ".jsonocel"),
)
