from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from oceldb import OCEL
from oceldb.core.manifest import OCELManifest, TableSchema
from oceldb.dsl import col
from oceldb.io import convert_sqlite
from oceldb.io.convert import _attach_sqlite_source


def _create_sqlite_source(source: str) -> None:
    with sqlite3.connect(source) as con:
        con.execute("""
            CREATE TABLE event_map_type (
                ocel_type TEXT,
                ocel_type_map TEXT
            )
        """)
        con.execute("""
            CREATE TABLE object_map_type (
                ocel_type TEXT,
                ocel_type_map TEXT
            )
        """)
        con.execute("""
            CREATE TABLE event (
                ocel_id TEXT,
                ocel_type TEXT
            )
        """)
        con.execute("""
            CREATE TABLE object (
                ocel_id TEXT,
                ocel_type TEXT
            )
        """)
        con.execute("""
            CREATE TABLE event_object (
                ocel_event_id TEXT,
                ocel_object_id TEXT
            )
        """)
        con.execute("""
            CREATE TABLE object_object (
                ocel_source_id TEXT,
                ocel_target_id TEXT
            )
        """)
        con.execute("""
            CREATE TABLE event_create_order (
                ocel_id TEXT,
                ocel_time TEXT,
                total_price REAL
            )
        """)
        con.execute("""
            CREATE TABLE event_pay_order (
                ocel_id TEXT,
                ocel_time TEXT,
                method TEXT
            )
        """)
        con.execute("""
            CREATE TABLE object_order (
                ocel_id TEXT,
                ocel_time TEXT,
                ocel_changed_field TEXT,
                status TEXT
            )
        """)
        con.execute("""
            CREATE TABLE object_customer (
                ocel_id TEXT,
                ocel_time TEXT,
                ocel_changed_field TEXT,
                name TEXT
            )
        """)

        con.executemany(
            "INSERT INTO event_map_type VALUES (?, ?)",
            [("Create Order", "create_order"), ("Pay Order", "pay_order")],
        )
        con.executemany(
            "INSERT INTO object_map_type VALUES (?, ?)",
            [("customer", "customer"), ("order", "order")],
        )
        con.executemany(
            "INSERT INTO event VALUES (?, ?)",
            [
                ("e1", "Create Order"),
                ("e2", "Create Order"),
                ("e3", "Pay Order"),
            ],
        )
        con.executemany(
            "INSERT INTO object VALUES (?, ?)",
            [
                ("o1", "order"),
                ("o2", "order"),
                ("o3", "customer"),
            ],
        )
        con.executemany(
            "INSERT INTO event_create_order VALUES (?, ?, ?)",
            [
                ("e1", "2022-01-01 10:00:00", 100.0),
                ("e2", "2022-01-01 11:00:00", 250.0),
            ],
        )
        con.executemany(
            "INSERT INTO event_pay_order VALUES (?, ?, ?)",
            [("e3", "2022-01-01 12:00:00", "credit_card")],
        )
        con.executemany(
            "INSERT INTO object_order VALUES (?, ?, ?, ?)",
            [
                ("o1", "2022-01-01 10:00:00", None, "open"),
                ("o2", "2022-01-01 11:00:00", None, "closed"),
            ],
        )
        con.executemany(
            "INSERT INTO object_customer VALUES (?, ?, ?, ?)",
            [("o3", "2022-01-01 09:00:00", None, "Alice")],
        )
        con.executemany(
            "INSERT INTO event_object VALUES (?, ?)",
            [("e1", "o1"), ("e1", "o3"), ("e2", "o2"), ("e3", "o1")],
        )
        con.executemany(
            "INSERT INTO object_object VALUES (?, ?)",
            [("o1", "o3"), ("o2", "o3")],
        )


def _manifest_with_stale_columns(manifest: OCELManifest) -> OCELManifest:
    event = manifest.table("event")
    object_change = manifest.table("object_change")

    tables = dict(manifest.tables)
    tables["event"] = TableSchema(
        name="event",
        core_columns=event.core_columns,
        custom_columns={
            **dict(event.custom_columns),
            "ghost_event": "VARCHAR",
        },
    )
    tables["object_change"] = TableSchema(
        name="object_change",
        core_columns=object_change.core_columns,
        custom_columns={
            **dict(object_change.custom_columns),
            "ghost_state": "VARCHAR",
        },
    )

    return OCELManifest(
        oceldb_version=manifest.oceldb_version,
        storage_version=manifest.storage_version,
        source=manifest.source,
        created_at=manifest.created_at,
        tables=tables,
    )


def test_write_and_read_directory_roundtrip(ocel, tmp_path):
    target = tmp_path / "roundtrip"
    written = ocel.write(target)

    with OCEL.read(written) as reloaded:
        assert reloaded.query.events().count() == 5
        assert reloaded.query.objects().count() == 3


def test_write_and_read_directory(ocel, tmp_path):
    target = tmp_path / "single"
    written = ocel.write(target)

    with OCEL.read(written) as reloaded:
        assert reloaded.query.events("Create Order").count() == 3
        assert reloaded.query.object_states("order").latest().where(col("status") == "open").ids() == ["o1"]


def test_write_sublog_directory(ocel, tmp_path):
    target = tmp_path / "open-orders"
    written = (
        ocel.query
        .object_states("order")
        .latest()
        .where(col("status") == "open")
        .write(target)
    )

    with OCEL.read(written) as sublog:
        assert sorted(sublog.query.events().ids()) == ["e1", "e3", "e5"]
        assert sorted(sublog.query.objects().ids()) == ["o1", "o3"]


def test_write_rebuilds_manifest_from_actual_tables(ocel, tmp_path):
    stale = OCEL(
        path=ocel.path,
        con=ocel._con,
        manifest=_manifest_with_stale_columns(ocel.manifest),
    )

    written = stale.write(tmp_path / "stale-copy")

    with OCEL.read(written) as reloaded:
        assert "ghost_event" not in reloaded.manifest.table("event").columns
        assert "ghost_state" not in reloaded.manifest.table("object_change").columns
        assert reloaded.query.events().count() == 5


def test_to_ocel_returns_independent_handle(ocel):
    derived = (
        ocel.query
        .object_states("order")
        .latest()
        .where(col("status") == "open")
        .to_ocel()
    )

    try:
        ocel.close()

        assert sorted(derived.query.events().ids()) == ["e1", "e3", "e5"]
        assert sorted(derived.query.objects().ids()) == ["o1", "o3"]
    finally:
        derived.close()


def test_object_rooted_sublog_keeps_selected_orphan_objects(ocel_with_orphan_object):
    derived = (
        ocel_with_orphan_object.query
        .object_states("order")
        .latest()
        .where(col("status") == "open")
        .to_ocel()
    )

    try:
        assert derived.query.events().ids() == ["e1"]
        assert sorted(derived.query.objects().ids()) == ["o1", "o2"]
    finally:
        derived.close()


def test_to_ocel_rebuilds_manifest_from_materialized_tables(ocel):
    stale = OCEL(
        path=ocel.path,
        con=ocel._con,
        manifest=_manifest_with_stale_columns(ocel.manifest),
    )

    derived = stale.query.events("Create Order").to_ocel()

    try:
        assert "ghost_event" not in derived.manifest.table("event").columns
        assert "ghost_state" not in derived.manifest.table("object_change").columns
        assert derived.query.events().count() == 3
    finally:
        derived.close()


def test_to_ocel_prunes_dead_event_custom_columns(ocel):
    derived = ocel.query.events("Create Order").to_ocel()

    try:
        custom_columns = derived.manifest.table("event").custom_columns
        assert set(custom_columns) == {"total_price"}
        assert derived.query.events().count() == 3
    finally:
        derived.close()


def test_to_ocel_prunes_dead_object_history_columns(ocel_with_orphan_object):
    derived = (
        ocel_with_orphan_object.query
        .objects("order")
        .where(col("ocel_id") == "o2")
        .to_ocel()
    )

    try:
        assert derived.manifest.table("object_change").custom_columns == {
            "status": "VARCHAR",
        }
        assert derived.query.objects().ids() == ["o2"]
    finally:
        derived.close()


def test_convert_sqlite_roundtrip(tmp_path):
    source = tmp_path / "source.sqlite"
    target = tmp_path / "converted"

    _create_sqlite_source(str(source))
    written = convert_sqlite(source, target)

    with OCEL.read(written) as ocel:
        assert ocel.query.events().count() == 3
        assert ocel.query.object_states("order").latest().where(col("status") == "open").ids() == ["o1"]
        assert (
            ocel.query
            .events("Create Order")
            .select("total_price")
            .collect()
            .fetchall()
        ) == [(100.0,), (250.0,)]


def test_attach_sqlite_source_reads_columns_as_varchar():
    class FakeConnection:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def execute(self, sql: str) -> None:
            self.calls.append(sql)

    con = FakeConnection()

    attached = _attach_sqlite_source(con, Path("/tmp/source.sqlite"))

    assert attached is True
    assert con.calls == [
        "LOAD sqlite",
        "SET GLOBAL sqlite_all_varchar = true",
        "ATTACH '/tmp/source.sqlite' AS ocel_source (TYPE sqlite, READ_ONLY)",
    ]


def test_read_rejects_unsupported_storage_version(tmp_path):
    legacy_dir = tmp_path / "legacy"
    legacy_dir.mkdir()
    (legacy_dir / "manifest.json").write_text(
        json.dumps(
            {
                "format": "oceldb",
                "storage_version": "1",
                "oceldb_version": "0.1.0",
                "source": "legacy.sqlite",
                "created_at": "2026-01-01T00:00:00+00:00",
                "tables": {},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unsupported oceldb storage version"):
        OCEL.read(legacy_dir)


def test_read_rejects_file_sources(tmp_path):
    file_path = tmp_path / "not-a-directory.txt"
    file_path.write_text("nope", encoding="utf-8")

    with pytest.raises(ValueError, match="must be a directory"):
        OCEL.read(file_path)
