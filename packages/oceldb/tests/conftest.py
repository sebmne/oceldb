from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb
import pytest

from oceldb.core.metadata import OCELManifest, TableSchema
from oceldb.core.ocel import OCEL
from oceldb.sql.context import CompileContext


@pytest.fixture()
def ctx() -> CompileContext:
    return CompileContext(alias="o", schema="test_schema", kind="object")


@pytest.fixture()
def event_ctx() -> CompileContext:
    return CompileContext(alias="e", schema="test_schema", kind="event")


@pytest.fixture()
def ocel(tmp_path: Path) -> OCEL:
    con = duckdb.connect()
    schema = "test_ocel"
    con.execute(f'CREATE SCHEMA "{schema}"')

    con.execute(f"""
        CREATE TABLE "{schema}"."event" AS
        SELECT * FROM (VALUES
            ('e1', 'Create Order', TIMESTAMP '2022-01-01 10:00:00', 100.0, NULL),
            ('e2', 'Create Order', TIMESTAMP '2022-01-01 11:00:00', 250.0, NULL),
            ('e3', 'Pay Order',    TIMESTAMP '2022-01-01 12:00:00', NULL, 'credit_card'),
            ('e4', 'Pay Order',    TIMESTAMP '2022-01-01 13:00:00', NULL, 'debit_card'),
            ('e5', 'Create Order', TIMESTAMP '2022-01-01 14:00:00', 300.0, NULL)
        ) AS t(ocel_id, ocel_type, ocel_time, total_price, method)
    """)

    con.execute(f"""
        CREATE TABLE "{schema}"."object" AS
        SELECT * FROM (VALUES
            ('o1', 'order',    TIMESTAMP '2022-01-01 10:00:00', NULL, 'open',   NULL),
            ('o2', 'order',    TIMESTAMP '2022-01-01 11:00:00', NULL, 'closed', NULL),
            ('o3', 'customer', TIMESTAMP '2022-01-01 09:00:00', NULL, NULL,     'Alice')
        ) AS t(ocel_id, ocel_type, ocel_time, ocel_changed_field, status, name)
    """)

    con.execute(f"""
        CREATE TABLE "{schema}"."event_object" AS
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
        CREATE TABLE "{schema}"."object_object" AS
        SELECT * FROM (VALUES
            ('o1', 'o3'),
            ('o2', 'o3')
        ) AS t(ocel_source_id, ocel_target_id)
    """)

    manifest = OCELManifest(
        oceldb_version="0.3.0",
        storage_version="1",
        source="test.sqlite",
        created_at=datetime(2022, 1, 1),
        packaging="directory",
        tables={
            "event": TableSchema(
                name="event",
                core_columns={
                    "ocel_id": "VARCHAR",
                    "ocel_type": "VARCHAR",
                    "ocel_time": "TIMESTAMP",
                },
                custom_columns={
                    "total_price": "DOUBLE",
                    "method": "VARCHAR",
                },
            ),
            "object": TableSchema(
                name="object",
                core_columns={
                    "ocel_id": "VARCHAR",
                    "ocel_type": "VARCHAR",
                    "ocel_time": "TIMESTAMP",
                    "ocel_changed_field": "VARCHAR",
                },
                custom_columns={
                    "status": "VARCHAR",
                    "name": "VARCHAR",
                },
            ),
            "event_object": TableSchema(
                name="event_object",
                core_columns={
                    "ocel_event_id": "VARCHAR",
                    "ocel_object_id": "VARCHAR",
                },
            ),
            "object_object": TableSchema(
                name="object_object",
                core_columns={
                    "ocel_source_id": "VARCHAR",
                    "ocel_target_id": "VARCHAR",
                },
            ),
        },
    )

    return OCEL(
        path=tmp_path / "test.oceldb",
        data_path=tmp_path / "test.oceldb",
        con=con,
        manifest=manifest,
        schema=schema,
        owns_connection=True,
    )
