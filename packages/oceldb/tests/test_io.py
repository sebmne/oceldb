from __future__ import annotations

import sqlite3

from oceldb.dsl import col
from oceldb.io import convert_sqlite, read_ocel


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


def test_write_and_read_directory_roundtrip(ocel, tmp_path):
    target = tmp_path / "roundtrip.oceldb"
    written = ocel.write(target)

    with read_ocel(written) as reloaded:
        assert reloaded.metadata.packaging == "directory"
        assert reloaded.query().events().count() == 5
        assert reloaded.query().objects().count() == 3


def test_write_and_read_packaged_archive(ocel, tmp_path):
    target = tmp_path / "single.oceldb"
    written = ocel.write(target, packaged=True)

    with read_ocel(written) as reloaded:
        assert reloaded.metadata.packaging == "archive"
        assert reloaded.query().events("Create Order").count() == 3
        assert reloaded.query().objects("order").filter(col("status") == "open").ids() == ["o1"]


def test_write_sublog_archive(ocel, tmp_path):
    target = tmp_path / "open-orders.oceldb"
    written = (
        ocel.query()
        .objects("order")
        .filter(col("status") == "open")
        .write(target, packaged=True)
    )

    with read_ocel(written) as sublog:
        assert sorted(sublog.query().events().ids()) == ["e1", "e3", "e5"]
        assert sorted(sublog.query().objects().ids()) == ["o1", "o3"]


def test_write_packaged_adds_oceldb_suffix(ocel, tmp_path):
    written = ocel.write(tmp_path / "single", packaged=True)

    assert written.name == "single.oceldb"
    assert written.is_file()


def test_convert_sqlite_roundtrip(tmp_path):
    source = tmp_path / "source.sqlite"
    target = tmp_path / "converted.oceldb"

    _create_sqlite_source(str(source))
    written = convert_sqlite(source, target)

    with read_ocel(written) as ocel:
        assert ocel.metadata.packaging == "directory"
        assert ocel.query().events().count() == 3
        assert ocel.query().objects("order").filter(col("status") == "open").ids() == ["o1"]
        assert (
            ocel.query()
            .events("Create Order")
            .select("total_price")
            .collect()
            .fetchall()
        ) == [(100.0,), (250.0,)]


def test_convert_sqlite_packaged_adds_oceldb_suffix(tmp_path):
    source = tmp_path / "source.sqlite"
    _create_sqlite_source(str(source))

    written = convert_sqlite(source, tmp_path / "single", packaged=True)

    assert written.name == "single.oceldb"
    assert written.is_file()
