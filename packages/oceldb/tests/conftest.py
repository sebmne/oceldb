"""Shared fixtures for oceldb tests."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb
import pytest

from oceldb.core.metadata import OCELMetadata
from oceldb.core.ocel import OCEL
from oceldb.sql.context import CompileContext


@pytest.fixture()
def ctx() -> CompileContext:
    """A minimal compile context for expression rendering tests."""
    return CompileContext(alias="o", schema="test_schema", kind="object")


@pytest.fixture()
def event_ctx() -> CompileContext:
    """A compile context rooted on events."""
    return CompileContext(alias="e", schema="test_schema", kind="event")


@pytest.fixture()
def ocel(tmp_path: Path) -> OCEL:
    """
    Create a minimal in-memory OCEL backed by DuckDB tables.

    Events:
        e1  Create Order  2022-01-01 10:00  {total_price: 100}
        e2  Create Order  2022-01-01 11:00  {total_price: 250}
        e3  Pay Order     2022-01-01 12:00  {method: credit_card}
        e4  Pay Order     2022-01-01 13:00  {method: debit_card}
        e5  Create Order  2022-01-01 14:00  {total_price: 300}

    Objects:
        o1  order     {status: open}
        o2  order     {status: closed}
        o3  customer  {name: Alice}

    Event-Object relations:
        e1 -> o1, e1 -> o3, e2 -> o2, e3 -> o1, e4 -> o2, e5 -> o1

    Object-Object relations:
        o1 -> o3 (belongs_to), o2 -> o3 (belongs_to)
    """
    con = duckdb.connect()
    schema = "test_ocel"
    con.execute(f"CREATE SCHEMA {schema}")

    con.execute(f"""
        CREATE TABLE {schema}.event AS
        SELECT * FROM (VALUES
            ('e1', 'Create Order', TIMESTAMP '2022-01-01 10:00:00', '{{"total_price": "100"}}'),
            ('e2', 'Create Order', TIMESTAMP '2022-01-01 11:00:00', '{{"total_price": "250"}}'),
            ('e3', 'Pay Order',    TIMESTAMP '2022-01-01 12:00:00', '{{"method": "credit_card"}}'),
            ('e4', 'Pay Order',    TIMESTAMP '2022-01-01 13:00:00', '{{"method": "debit_card"}}'),
            ('e5', 'Create Order', TIMESTAMP '2022-01-01 14:00:00', '{{"total_price": "300"}}')
        ) AS t(ocel_id, ocel_type, ocel_time, attributes)
    """)

    con.execute(f"""
        CREATE TABLE {schema}.object AS
        SELECT * FROM (VALUES
            ('o1', 'order',    TIMESTAMP '2022-01-01 10:00:00', NULL, '{{"status": "open"}}'),
            ('o2', 'order',    TIMESTAMP '2022-01-01 11:00:00', NULL, '{{"status": "closed"}}'),
            ('o3', 'customer', TIMESTAMP '2022-01-01 09:00:00', NULL, '{{"name": "Alice"}}')
        ) AS t(ocel_id, ocel_type, ocel_time, ocel_changed_field, attributes)
    """)

    con.execute(f"""
        CREATE TABLE {schema}.event_object AS
        SELECT * FROM (VALUES
            ('e1', 'o1'),
            ('e1', 'o3'),
            ('e2', 'o2'),
            ('e3', 'o1'),
            ('e4', 'o2'),
            ('e5', 'o1')
        ) AS t(ocel_event_id, ocel_object_id)
    """)

    con.execute(f"""
        CREATE TABLE {schema}.object_object AS
        SELECT * FROM (VALUES
            ('o1', 'o3'),
            ('o2', 'o3')
        ) AS t(ocel_source_id, ocel_target_id)
    """)

    metadata = OCELMetadata(
        oceldb_version="0.2.0",
        source="test",
        converted_at=datetime(2022, 1, 1),
    )

    return OCEL(
        path=tmp_path,
        con=con,
        metadata=metadata,
        schema=schema,
        owns_connection=True,
    )
