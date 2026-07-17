"""Round-trips through native storage and the exchange formats."""

import json
import sqlite3

import polars as pl

from conftest import assert_same_log
from oceldb import OCEL
from oceldb.io import export_ocel, import_ocel, read_json, read_xml


def test_native_write_open_roundtrip(ocel: OCEL, native_ocel: OCEL) -> None:
    assert_same_log(ocel, native_ocel)


def test_json_roundtrip_is_stable(ocel: OCEL, tmp_path) -> None:
    first = tmp_path / "log.json"
    second = tmp_path / "log2.json"
    export_ocel(ocel, first)
    export_ocel(read_json(first), second)
    assert json.loads(first.read_text()) == json.loads(second.read_text())


def test_xml_roundtrip(ocel: OCEL, tmp_path) -> None:
    path = tmp_path / "log.xml"
    export_ocel(ocel, path)
    assert_same_log(ocel, read_xml(path))


def test_sqlite_roundtrip(ocel: OCEL, tmp_path) -> None:
    path = tmp_path / "log.sqlite"
    export_ocel(ocel, path)
    imported = import_ocel(path, tmp_path / "imported")
    assert_same_log(ocel, imported)


def test_declared_but_empty_types_survive(tmp_path) -> None:
    """Unused declarations only IO boundaries can know about are preserved."""
    document = {
        "eventTypes": [
            {"name": "Load", "attributes": [{"name": "weight", "type": "float"}]},
            {"name": "Ghost", "attributes": [{"name": "phantom", "type": "integer"}]},
        ],
        "objectTypes": [{"name": "Container", "attributes": []}],
        "events": [
            {
                "id": "e1",
                "type": "Load",
                "time": "2024-01-01T00:00:00+00:00",
                "attributes": [{"name": "weight", "value": 1.5}],
                "relationships": [{"objectId": "c1", "qualifier": "loads"}],
            }
        ],
        "objects": [{"id": "c1", "type": "Container", "attributes": []}],
    }
    source = tmp_path / "declared.json"
    source.write_text(json.dumps(document))

    # In-memory read -> export keeps the unused declaration.
    exported = tmp_path / "exported.json"
    export_ocel(read_json(source), exported)
    result = json.loads(exported.read_text())
    assert {
        "name": "Ghost",
        "attributes": [{"name": "phantom", "type": "integer"}],
    } in (result["eventTypes"])

    # Import -> native keeps it in the manifest, and the typed accessor
    # returns the declared (empty, correctly typed) frame.
    opened = import_ocel(source, tmp_path / "native")
    ghost = opened.events("Ghost").collect()
    assert ghost.height == 0
    assert ghost.schema["phantom"] == pl.Int64


def test_transformed_export_infers_from_data(ocel: OCEL, tmp_path) -> None:
    from oceldb.operations.filters import filter_events_by_type

    sub = filter_events_by_type(ocel, "Load")
    path = tmp_path / "sub.json"
    export_ocel(sub, path)
    result = json.loads(path.read_text())
    assert [t["name"] for t in result["eventTypes"]] == ["Load"]


def test_json_import_widens_shared_attributes(tmp_path) -> None:
    """A column reused by two types with different declarations gets one dtype."""
    document = {
        "eventTypes": [
            {"name": "A", "attributes": [{"name": "x", "type": "integer"}]},
            {"name": "B", "attributes": [{"name": "x", "type": "string"}]},
        ],
        "objectTypes": [{"name": "Thing", "attributes": []}],
        "events": [
            {
                "id": "e1",
                "type": "A",
                "time": "2024-01-01T00:00:00+00:00",
                "attributes": [{"name": "x", "value": 1}],
                "relationships": [{"objectId": "t1", "qualifier": "q"}],
            },
            {
                "id": "e2",
                "type": "B",
                "time": "2024-01-02T00:00:00+00:00",
                "attributes": [{"name": "x", "value": "high"}],
                "relationships": [{"objectId": "t1", "qualifier": "q"}],
            },
        ],
        "objects": [{"id": "t1", "type": "Thing", "attributes": []}],
    }
    source = tmp_path / "conflict.json"
    source.write_text(json.dumps(document))
    opened = import_ocel(source, tmp_path / "native")
    assert opened.events().collect().schema["x"] == pl.String
    assert opened.events("A").collect().get_column("x").to_list() == ["1"]


def test_sqlite_import_widens_shared_attributes(tmp_path) -> None:
    source = tmp_path / "conflict.sqlite"
    con = sqlite3.connect(source)
    con.executescript(
        """
        CREATE TABLE event (ocel_id TEXT, ocel_type TEXT);
        CREATE TABLE event_map_type (ocel_type TEXT, ocel_type_map TEXT);
        CREATE TABLE "event_A" (ocel_id TEXT, ocel_time TIMESTAMP, x INTEGER);
        CREATE TABLE "event_B" (ocel_id TEXT, ocel_time TIMESTAMP, x TEXT);
        CREATE TABLE object (ocel_id TEXT, ocel_type TEXT);
        CREATE TABLE object_map_type (ocel_type TEXT, ocel_type_map TEXT);
        CREATE TABLE "object_Thing" (
            ocel_id TEXT, ocel_time TIMESTAMP, ocel_changed_field TEXT
        );
        CREATE TABLE event_object (
            ocel_event_id TEXT, ocel_object_id TEXT, ocel_qualifier TEXT
        );

        INSERT INTO event VALUES ('e1', 'A'), ('e2', 'B');
        INSERT INTO event_map_type VALUES ('A', 'A'), ('B', 'B');
        INSERT INTO "event_A" VALUES ('e1', '2024-01-01 00:00:00+00:00', 1);
        INSERT INTO "event_B" VALUES ('e2', '2024-01-02 00:00:00+00:00', 'high');
        INSERT INTO object VALUES ('t1', 'Thing');
        INSERT INTO object_map_type VALUES ('Thing', 'Thing');
        INSERT INTO "object_Thing" VALUES ('t1', NULL, NULL);
        INSERT INTO event_object VALUES ('e1', 't1', 'q'), ('e2', 't1', 'q');
        """
    )
    con.commit()
    con.close()

    opened = import_ocel(source, tmp_path / "native")
    assert opened.events().collect().schema["x"] == pl.String
    assert set(opened.describe().event_types) == {"A", "B"}
