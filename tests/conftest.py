"""Shared fixtures for oceldb tests."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest


@pytest.fixture()
def ocel_db(tmp_path: Path) -> Path:
    """Create a minimal OCEL 2.0 SQLite database and return its path.

    Schema:
        Event types: "Create Order" (attributes: total_price REAL),
                     "Pay Order"    (attributes: payment_method TEXT)
        Object types: "order"    (attributes: status TEXT),
                      "customer" (attributes: name TEXT)
        Events: e1 (Create Order, 10:00), e2 (Create Order, 11:00),
                e3 (Pay Order, 12:00), e4 (Pay Order, 13:00),
                e5 (Create Order, 14:00)
        Objects: o1 (order), o2 (order), o3 (customer)
        E2O relations: e1→o1, e1→o3, e2→o2, e3→o1, e4→o2, e5→o1
        O2O relations: o1→o3, o2→o3

    Timelines per object:
        o1: e1 (Create Order 10:00) → e3 (Pay Order 12:00) → e5 (Create Order 14:00)
        o2: e2 (Create Order 11:00) → e4 (Pay Order 13:00)
        o3: e1 (Create Order 10:00)
    """
    db_path = tmp_path / "test.sqlite"
    con = sqlite3.connect(str(db_path))
    cur = con.cursor()

    # Core tables
    cur.execute("CREATE TABLE event (ocel_id TEXT PRIMARY KEY, ocel_type TEXT)")
    cur.executemany(
        "INSERT INTO event VALUES (?, ?)",
        [
            ("e1", "Create Order"),
            ("e2", "Create Order"),
            ("e3", "Pay Order"),
            ("e4", "Pay Order"),
            ("e5", "Create Order"),
        ],
    )

    cur.execute("CREATE TABLE object (ocel_id TEXT PRIMARY KEY, ocel_type TEXT)")
    cur.executemany(
        "INSERT INTO object VALUES (?, ?)",
        [("o1", "order"), ("o2", "order"), ("o3", "customer")],
    )

    # Relationship tables
    cur.execute(
        "CREATE TABLE event_object ("
        "  ocel_event_id TEXT, ocel_object_id TEXT, ocel_qualifier TEXT"
        ")"
    )
    cur.executemany(
        "INSERT INTO event_object VALUES (?, ?, ?)",
        [
            ("e1", "o1", "order"),
            ("e1", "o3", "customer"),
            ("e2", "o2", "order"),
            ("e3", "o1", "order"),
            ("e4", "o2", "order"),
            ("e5", "o1", "order"),
        ],
    )

    cur.execute(
        "CREATE TABLE object_object ("
        "  ocel_source_id TEXT, ocel_target_id TEXT, ocel_qualifier TEXT"
        ")"
    )
    cur.executemany(
        "INSERT INTO object_object VALUES (?, ?, ?)",
        [("o1", "o3", "belongs_to"), ("o2", "o3", "belongs_to")],
    )

    # Map tables
    cur.execute("CREATE TABLE event_map_type (ocel_type TEXT, ocel_type_map TEXT)")
    cur.executemany(
        "INSERT INTO event_map_type VALUES (?, ?)",
        [("Create Order", "CreateOrder"), ("Pay Order", "PayOrder")],
    )

    cur.execute("CREATE TABLE object_map_type (ocel_type TEXT, ocel_type_map TEXT)")
    cur.executemany(
        "INSERT INTO object_map_type VALUES (?, ?)",
        [("order", "order"), ("customer", "customer")],
    )

    # Per-type event tables
    cur.execute(
        "CREATE TABLE event_CreateOrder ("
        "  ocel_id TEXT PRIMARY KEY, ocel_time TEXT, total_price REAL"
        ")"
    )
    cur.executemany(
        "INSERT INTO event_CreateOrder VALUES (?, ?, ?)",
        [
            ("e1", "2022-01-01T10:00:00", 100.0),
            ("e2", "2022-01-01T11:00:00", 250.0),
            ("e5", "2022-01-01T14:00:00", 300.0),
        ],
    )

    cur.execute(
        "CREATE TABLE event_PayOrder ("
        "  ocel_id TEXT PRIMARY KEY, ocel_time TEXT, payment_method TEXT"
        ")"
    )
    cur.executemany(
        "INSERT INTO event_PayOrder VALUES (?, ?, ?)",
        [
            ("e3", "2022-01-01T12:00:00", "credit_card"),
            ("e4", "2022-01-01T13:00:00", "debit_card"),
        ],
    )

    # Per-type object tables
    cur.execute(
        "CREATE TABLE object_order ("
        "  ocel_id TEXT PRIMARY KEY, ocel_time TEXT, status TEXT"
        ")"
    )
    cur.executemany(
        "INSERT INTO object_order VALUES (?, ?, ?)",
        [("o1", "2022-01-01T10:00:00", "open"), ("o2", "2022-01-02T11:00:00", "open")],
    )

    cur.execute(
        "CREATE TABLE object_customer ("
        "  ocel_id TEXT PRIMARY KEY, ocel_time TEXT, name TEXT"
        ")"
    )
    cur.execute(
        "INSERT INTO object_customer VALUES (?, ?, ?)",
        ("o3", "2022-01-01T09:00:00", "Alice"),
    )

    con.commit()
    con.close()
    return db_path
