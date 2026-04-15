from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb
import pytest
from oceldb.core.manifest import OCELManifest, TableSchema
from oceldb.core.ocel import OCEL
from oceldb.sql.context import CompileContext

EVENT_OBJECT_CORE_COLUMNS = {
    "ocel_event_id": "VARCHAR",
    "ocel_object_id": "VARCHAR",
    "ocel_qualifier": "VARCHAR",
}
OBJECT_OBJECT_CORE_COLUMNS = {
    "ocel_source_id": "VARCHAR",
    "ocel_target_id": "VARCHAR",
    "ocel_qualifier": "VARCHAR",
}


def _add_relation_qualifier_columns(con: duckdb.DuckDBPyConnection) -> None:
    con.execute('ALTER TABLE "event_object" ADD COLUMN ocel_qualifier VARCHAR')
    con.execute('ALTER TABLE "object_object" ADD COLUMN ocel_qualifier VARCHAR')


@pytest.fixture()
def ctx() -> CompileContext:
    return CompileContext(
        alias="o",
        kind="object",
        object_change_columns=("ocel_id", "ocel_type", "ocel_time", "ocel_changed_field", "status", "name"),
    )


@pytest.fixture()
def event_ctx() -> CompileContext:
    return CompileContext(
        alias="e",
        kind="event",
        object_change_columns=("ocel_id", "ocel_type", "ocel_time", "ocel_changed_field", "status", "name"),
    )


@pytest.fixture()
def ocel(tmp_path: Path) -> OCEL:
    con = duckdb.connect()

    con.execute("""
        CREATE TABLE "event" AS
        SELECT * FROM (VALUES
            ('e1', 'Create Order', TIMESTAMP '2022-01-01 10:00:00', 100.0, NULL),
            ('e2', 'Create Order', TIMESTAMP '2022-01-01 11:00:00', 250.0, NULL),
            ('e3', 'Pay Order',    TIMESTAMP '2022-01-01 12:00:00', NULL, 'credit_card'),
            ('e4', 'Pay Order',    TIMESTAMP '2022-01-01 13:00:00', NULL, 'debit_card'),
            ('e5', 'Create Order', TIMESTAMP '2022-01-01 14:00:00', 300.0, NULL)
        ) AS t(ocel_id, ocel_type, ocel_time, total_price, method)
    """)

    con.execute("""
        CREATE TABLE "object" AS
        SELECT * FROM (VALUES
            ('o1', 'order'),
            ('o2', 'order'),
            ('o3', 'customer')
        ) AS t(ocel_id, ocel_type)
    """)

    con.execute("""
        CREATE TABLE "object_change" AS
        SELECT * FROM (VALUES
            ('o1', 'order',    TIMESTAMP '2022-01-01 10:00:00', NULL, 'open',   NULL),
            ('o2', 'order',    TIMESTAMP '2022-01-01 11:00:00', NULL, 'closed', NULL),
            ('o3', 'customer', TIMESTAMP '2022-01-01 09:00:00', NULL, NULL,     'Alice')
        ) AS t(ocel_id, ocel_type, ocel_time, ocel_changed_field, status, name)
    """)

    con.execute("""
        CREATE TABLE "event_object" AS
        SELECT * FROM (VALUES
            ('e1', 'o1'),
            ('e1', 'o3'),
            ('e2', 'o2'),
            ('e3', 'o1'),
            ('e4', 'o2'),
            ('e5', 'o1')
        ) AS t(ocel_event_id, ocel_object_id)
    """)

    con.execute("""
        CREATE TABLE "object_object" AS
        SELECT * FROM (VALUES
            ('o1', 'o3'),
            ('o2', 'o3')
        ) AS t(ocel_source_id, ocel_target_id)
    """)
    _add_relation_qualifier_columns(con)

    manifest = OCELManifest(
        oceldb_version="0.3.0",
        storage_version="3",
        source="test.sqlite",
        created_at=datetime(2022, 1, 1),
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
                type_attributes={
                    "Create Order": ("total_price",),
                    "Pay Order": ("method",),
                },
            ),
            "object": TableSchema(
                name="object",
                core_columns={
                    "ocel_id": "VARCHAR",
                    "ocel_type": "VARCHAR",
                },
            ),
            "object_change": TableSchema(
                name="object_change",
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
                type_attributes={
                    "customer": ("name",),
                    "order": ("status",),
                },
            ),
            "event_object": TableSchema(
                name="event_object",
                core_columns=EVENT_OBJECT_CORE_COLUMNS,
            ),
            "object_object": TableSchema(
                name="object_object",
                core_columns=OBJECT_OBJECT_CORE_COLUMNS,
            ),
        },
    )

    return OCEL(
        path=tmp_path / "test",
        con=con,
        manifest=manifest,
    )


@pytest.fixture()
def ocel_with_object_changes(tmp_path: Path) -> OCEL:
    con = duckdb.connect()

    con.execute("""
        CREATE TABLE "event" AS
        SELECT * FROM (VALUES
            ('e1', 'Create Order', TIMESTAMP '2022-01-01 10:00:00', 100.0, NULL),
            ('e2', 'Pay Order',    TIMESTAMP '2022-01-03 09:00:00', NULL, 'wire')
        ) AS t(ocel_id, ocel_type, ocel_time, total_price, method)
    """)

    con.execute("""
        CREATE TABLE "object" AS
        SELECT * FROM (VALUES
            ('o1', 'order'),
            ('o2', 'customer')
        ) AS t(ocel_id, ocel_type)
    """)

    con.execute("""
        CREATE TABLE "object_change" AS
        SELECT * FROM (VALUES
            ('o1', 'order',    TIMESTAMP '2022-01-01 10:00:00', NULL,      'open',   NULL),
            ('o1', 'order',    TIMESTAMP '2022-01-02 12:00:00', 'status',  'closed', NULL),
            ('o2', 'customer', TIMESTAMP '2022-01-01 09:00:00', NULL,      NULL,     'Alice'),
            ('o2', 'customer', TIMESTAMP '2022-01-04 08:00:00', 'name',    NULL,     'Alice B.')
        ) AS t(ocel_id, ocel_type, ocel_time, ocel_changed_field, status, name)
    """)

    con.execute("""
        CREATE TABLE "event_object" AS
        SELECT * FROM (VALUES
            ('e1', 'o1'),
            ('e1', 'o2'),
            ('e2', 'o1')
        ) AS t(ocel_event_id, ocel_object_id)
    """)

    con.execute("""
        CREATE TABLE "object_object" AS
        SELECT * FROM (VALUES
            ('o1', 'o2')
        ) AS t(ocel_source_id, ocel_target_id)
    """)
    _add_relation_qualifier_columns(con)

    manifest = OCELManifest(
        oceldb_version="0.3.0",
        storage_version="3",
        source="test.sqlite",
        created_at=datetime(2022, 1, 1),
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
                type_attributes={
                    "Create Order": ("total_price",),
                    "Pay Order": ("method",),
                },
            ),
            "object": TableSchema(
                name="object",
                core_columns={
                    "ocel_id": "VARCHAR",
                    "ocel_type": "VARCHAR",
                },
            ),
            "object_change": TableSchema(
                name="object_change",
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
                type_attributes={
                    "customer": ("name",),
                    "order": ("status",),
                },
            ),
            "event_object": TableSchema(
                name="event_object",
                core_columns=EVENT_OBJECT_CORE_COLUMNS,
            ),
            "object_object": TableSchema(
                name="object_object",
                core_columns=OBJECT_OBJECT_CORE_COLUMNS,
            ),
        },
    )

    return OCEL(
        path=tmp_path / "test-changes",
        con=con,
        manifest=manifest,
    )


@pytest.fixture()
def ocel_with_orphan_object(tmp_path: Path) -> OCEL:
    con = duckdb.connect()

    con.execute("""
        CREATE TABLE "event" AS
        SELECT * FROM (VALUES
            ('e1', 'Create Order', TIMESTAMP '2022-01-01 10:00:00', 100.0, NULL)
        ) AS t(ocel_id, ocel_type, ocel_time, total_price, method)
    """)

    con.execute("""
        CREATE TABLE "object" AS
        SELECT * FROM (VALUES
            ('o1', 'order'),
            ('o2', 'order')
        ) AS t(ocel_id, ocel_type)
    """)

    con.execute("""
        CREATE TABLE "object_change" AS
        SELECT * FROM (VALUES
            ('o1', 'order', TIMESTAMP '2022-01-01 09:30:00', NULL, 'open', NULL),
            ('o2', 'order', TIMESTAMP '2022-01-01 11:00:00', NULL, 'open', NULL)
        ) AS t(ocel_id, ocel_type, ocel_time, ocel_changed_field, status, name)
    """)

    con.execute("""
        CREATE TABLE "event_object" AS
        SELECT * FROM (VALUES
            ('e1', 'o1')
        ) AS t(ocel_event_id, ocel_object_id)
    """)

    con.execute("""
        CREATE TABLE "object_object" AS
        SELECT * FROM (VALUES
            ('o1', 'o2')
        ) AS t(ocel_source_id, ocel_target_id)
        WHERE FALSE
    """)
    _add_relation_qualifier_columns(con)

    manifest = OCELManifest(
        oceldb_version="0.3.0",
        storage_version="3",
        source="test.sqlite",
        created_at=datetime(2022, 1, 1),
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
                type_attributes={
                    "Create Order": ("total_price",),
                },
            ),
            "object": TableSchema(
                name="object",
                core_columns={
                    "ocel_id": "VARCHAR",
                    "ocel_type": "VARCHAR",
                },
            ),
            "object_change": TableSchema(
                name="object_change",
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
                type_attributes={
                    "order": ("status",),
                },
            ),
            "event_object": TableSchema(
                name="event_object",
                core_columns=EVENT_OBJECT_CORE_COLUMNS,
            ),
            "object_object": TableSchema(
                name="object_object",
                core_columns=OBJECT_OBJECT_CORE_COLUMNS,
            ),
        },
    )

    return OCEL(
        path=tmp_path / "test-orphan",
        con=con,
        manifest=manifest,
    )


@pytest.fixture()
def ocel_with_stateless_object(tmp_path: Path) -> OCEL:
    con = duckdb.connect()

    con.execute("""
        CREATE TABLE "event" AS
        SELECT * FROM (VALUES
            ('e1', 'Create Order', TIMESTAMP '2022-01-01 10:00:00', 100.0, NULL)
        ) AS t(ocel_id, ocel_type, ocel_time, total_price, method)
    """)

    con.execute("""
        CREATE TABLE "object" AS
        SELECT * FROM (VALUES
            ('o1', 'order'),
            ('o2', 'order')
        ) AS t(ocel_id, ocel_type)
    """)

    con.execute("""
        CREATE TABLE "object_change" AS
        SELECT * FROM (VALUES
            ('o1', 'order', TIMESTAMP '2022-01-01 10:00:00', NULL, 'open', NULL)
        ) AS t(ocel_id, ocel_type, ocel_time, ocel_changed_field, status, name)
    """)

    con.execute("""
        CREATE TABLE "event_object" AS
        SELECT * FROM (VALUES
            ('e1', 'o1')
        ) AS t(ocel_event_id, ocel_object_id)
    """)

    con.execute("""
        CREATE TABLE "object_object" AS
        SELECT * FROM (VALUES
            ('o1', 'o2')
        ) AS t(ocel_source_id, ocel_target_id)
        WHERE FALSE
    """)
    _add_relation_qualifier_columns(con)

    manifest = OCELManifest(
        oceldb_version="0.3.0",
        storage_version="3",
        source="test.sqlite",
        created_at=datetime(2022, 1, 1),
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
                type_attributes={
                    "Create Order": ("total_price",),
                },
            ),
            "object": TableSchema(
                name="object",
                core_columns={
                    "ocel_id": "VARCHAR",
                    "ocel_type": "VARCHAR",
                },
            ),
            "object_change": TableSchema(
                name="object_change",
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
                type_attributes={
                    "order": ("status",),
                },
            ),
            "event_object": TableSchema(
                name="event_object",
                core_columns=EVENT_OBJECT_CORE_COLUMNS,
            ),
            "object_object": TableSchema(
                name="object_object",
                core_columns=OBJECT_OBJECT_CORE_COLUMNS,
            ),
        },
    )

    return OCEL(
        path=tmp_path / "test-stateless",
        con=con,
        manifest=manifest,
    )


@pytest.fixture()
def ocel_with_link_graph(tmp_path: Path) -> OCEL:
    con = duckdb.connect()

    con.execute("""
        CREATE TABLE "event" AS
        SELECT * FROM (VALUES
            ('e0', 'Init', TIMESTAMP '2022-01-01 00:00:00', NULL, NULL)
        ) AS t(ocel_id, ocel_type, ocel_time, total_price, method)
        WHERE FALSE
    """)

    con.execute("""
        CREATE TABLE "object" AS
        SELECT * FROM (VALUES
            ('o1', 'order'),
            ('o2', 'package'),
            ('o3', 'shipment'),
            ('o4', 'customer'),
            ('o5', 'order')
        ) AS t(ocel_id, ocel_type)
    """)

    con.execute("""
        CREATE TABLE "object_change" AS
        SELECT * FROM (VALUES
            ('o1', 'order',    TIMESTAMP '2022-01-01 09:00:00', NULL, NULL,        NULL),
            ('o2', 'package',  TIMESTAMP '2022-01-01 09:05:00', NULL, NULL,        'PK-1'),
            ('o3', 'shipment', TIMESTAMP '2022-01-01 09:10:00', NULL, NULL,        'SH-1'),
            ('o4', 'customer', TIMESTAMP '2022-01-01 09:15:00', NULL, NULL,        'Alice'),
            ('o5', 'order',    TIMESTAMP '2022-01-01 09:20:00', NULL, NULL,        NULL)
        ) AS t(ocel_id, ocel_type, ocel_time, ocel_changed_field, status, name)
    """)

    con.execute("""
        CREATE TABLE "event_object" AS
        SELECT * FROM (VALUES
            ('e0', 'o1')
        ) AS t(ocel_event_id, ocel_object_id)
        WHERE FALSE
    """)

    con.execute("""
        CREATE TABLE "object_object" AS
        SELECT * FROM (VALUES
            ('o1', 'o2'),
            ('o2', 'o3'),
            ('o3', 'o2'),
            ('o3', 'o4')
        ) AS t(ocel_source_id, ocel_target_id)
    """)
    _add_relation_qualifier_columns(con)

    manifest = OCELManifest(
        oceldb_version="0.3.0",
        storage_version="3",
        source="test-link-graph.sqlite",
        created_at=datetime(2022, 1, 1),
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
                type_attributes={},
            ),
            "object": TableSchema(
                name="object",
                core_columns={
                    "ocel_id": "VARCHAR",
                    "ocel_type": "VARCHAR",
                },
            ),
            "object_change": TableSchema(
                name="object_change",
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
                type_attributes={
                    "customer": ("name",),
                    "order": (),
                    "package": ("name",),
                    "shipment": ("name",),
                },
            ),
            "event_object": TableSchema(
                name="event_object",
                core_columns=EVENT_OBJECT_CORE_COLUMNS,
            ),
            "object_object": TableSchema(
                name="object_object",
                core_columns=OBJECT_OBJECT_CORE_COLUMNS,
            ),
        },
    )

    return OCEL(
        path=tmp_path / "test-link-graph",
        con=con,
        manifest=manifest,
    )


@pytest.fixture()
def ocel_with_object_lifecycle(tmp_path: Path) -> OCEL:
    con = duckdb.connect()

    con.execute("""
        CREATE TABLE "event" AS
        SELECT * FROM (VALUES
            ('e0', 'Init', TIMESTAMP '2022-01-01 00:00:00', NULL, NULL)
        ) AS t(ocel_id, ocel_type, ocel_time, total_price, method)
        WHERE FALSE
    """)

    con.execute("""
        CREATE TABLE "object" AS
        SELECT * FROM (VALUES
            ('o1', 'order'),
            ('o2', 'order'),
            ('o3', 'order'),
            ('o4', 'order'),
            ('c1', 'customer')
        ) AS t(ocel_id, ocel_type)
    """)

    con.execute("""
        CREATE TABLE "object_change" AS
        SELECT * FROM (VALUES
            ('o1', 'order',    TIMESTAMP '2022-01-01 09:00:00', NULL,       'open',    'high', NULL),
            ('o1', 'order',    TIMESTAMP '2022-01-01 10:00:00', 'priority', NULL,      'low',  NULL),
            ('o1', 'order',    TIMESTAMP '2022-01-01 11:00:00', 'status',   'packed',  NULL,   NULL),
            ('o2', 'order',    TIMESTAMP '2022-01-01 09:30:00', NULL,       'open',    'low',  NULL),
            ('o2', 'order',    TIMESTAMP '2022-01-01 11:30:00', 'status',   'shipped', NULL,   NULL),
            ('o4', 'order',    TIMESTAMP '2022-01-01 08:00:00', NULL,       NULL,      NULL,   NULL),
            ('c1', 'customer', TIMESTAMP '2022-01-01 08:30:00', NULL,       NULL,      NULL,   'Alice')
        ) AS t(ocel_id, ocel_type, ocel_time, ocel_changed_field, status, priority, name)
    """)

    con.execute("""
        CREATE TABLE "event_object" AS
        SELECT * FROM (VALUES
            ('e0', 'o1')
        ) AS t(ocel_event_id, ocel_object_id)
        WHERE FALSE
    """)

    con.execute("""
        CREATE TABLE "object_object" AS
        SELECT * FROM (VALUES
            ('o1', 'o2')
        ) AS t(ocel_source_id, ocel_target_id)
        WHERE FALSE
    """)
    _add_relation_qualifier_columns(con)

    manifest = OCELManifest(
        oceldb_version="0.3.0",
        storage_version="3",
        source="test-lifecycle.sqlite",
        created_at=datetime(2022, 1, 1),
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
                type_attributes={},
            ),
            "object": TableSchema(
                name="object",
                core_columns={
                    "ocel_id": "VARCHAR",
                    "ocel_type": "VARCHAR",
                },
            ),
            "object_change": TableSchema(
                name="object_change",
                core_columns={
                    "ocel_id": "VARCHAR",
                    "ocel_type": "VARCHAR",
                    "ocel_time": "TIMESTAMP",
                    "ocel_changed_field": "VARCHAR",
                },
                custom_columns={
                    "status": "VARCHAR",
                    "priority": "VARCHAR",
                    "name": "VARCHAR",
                },
                type_attributes={
                    "customer": ("name",),
                    "order": ("status", "priority"),
                },
            ),
            "event_object": TableSchema(
                name="event_object",
                core_columns=EVENT_OBJECT_CORE_COLUMNS,
            ),
            "object_object": TableSchema(
                name="object_object",
                core_columns=OBJECT_OBJECT_CORE_COLUMNS,
            ),
        },
    )

    return OCEL(
        path=tmp_path / "test-lifecycle",
        con=con,
        manifest=manifest,
    )


@pytest.fixture()
def ocel_with_conflicting_lifecycle_changes(tmp_path: Path) -> OCEL:
    con = duckdb.connect()

    con.execute("""
        CREATE TABLE "event" AS
        SELECT * FROM (VALUES
            ('e0', 'Init', TIMESTAMP '2022-01-01 00:00:00', NULL, NULL)
        ) AS t(ocel_id, ocel_type, ocel_time, total_price, method)
        WHERE FALSE
    """)

    con.execute("""
        CREATE TABLE "object" AS
        SELECT * FROM (VALUES
            ('o1', 'order')
        ) AS t(ocel_id, ocel_type)
    """)

    con.execute("""
        CREATE TABLE "object_change" AS
        SELECT * FROM (VALUES
            ('o1', 'order', TIMESTAMP '2022-01-01 09:00:00', NULL,     'open'),
            ('o1', 'order', TIMESTAMP '2022-01-01 09:00:00', 'status', 'packed')
        ) AS t(ocel_id, ocel_type, ocel_time, ocel_changed_field, status)
    """)

    con.execute("""
        CREATE TABLE "event_object" AS
        SELECT * FROM (VALUES
            ('e0', 'o1')
        ) AS t(ocel_event_id, ocel_object_id)
        WHERE FALSE
    """)

    con.execute("""
        CREATE TABLE "object_object" AS
        SELECT * FROM (VALUES
            ('o1', 'o1')
        ) AS t(ocel_source_id, ocel_target_id)
        WHERE FALSE
    """)
    _add_relation_qualifier_columns(con)

    manifest = OCELManifest(
        oceldb_version="0.3.0",
        storage_version="3",
        source="test-conflicting-lifecycle.sqlite",
        created_at=datetime(2022, 1, 1),
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
                type_attributes={},
            ),
            "object": TableSchema(
                name="object",
                core_columns={
                    "ocel_id": "VARCHAR",
                    "ocel_type": "VARCHAR",
                },
            ),
            "object_change": TableSchema(
                name="object_change",
                core_columns={
                    "ocel_id": "VARCHAR",
                    "ocel_type": "VARCHAR",
                    "ocel_time": "TIMESTAMP",
                    "ocel_changed_field": "VARCHAR",
                },
                custom_columns={
                    "status": "VARCHAR",
                },
                type_attributes={
                    "order": ("status",),
                },
            ),
            "event_object": TableSchema(
                name="event_object",
                core_columns=EVENT_OBJECT_CORE_COLUMNS,
            ),
            "object_object": TableSchema(
                name="object_object",
                core_columns=OBJECT_OBJECT_CORE_COLUMNS,
            ),
        },
    )

    return OCEL(
        path=tmp_path / "test-conflicting-lifecycle",
        con=con,
        manifest=manifest,
    )


@pytest.fixture()
def ocel_with_simultaneous_object_updates(tmp_path: Path) -> OCEL:
    con = duckdb.connect()

    con.execute("""
        CREATE TABLE "event" AS
        SELECT * FROM (VALUES
            ('e0', 'Init', TIMESTAMP '2022-01-01 00:00:00', NULL, NULL)
        ) AS t(ocel_id, ocel_type, ocel_time, total_price, method)
        WHERE FALSE
    """)

    con.execute("""
        CREATE TABLE "object" AS
        SELECT * FROM (VALUES
            ('o1', 'order')
        ) AS t(ocel_id, ocel_type)
    """)

    con.execute("""
        CREATE TABLE "object_change" AS
        SELECT * FROM (VALUES
            ('o1', 'order', TIMESTAMP '2022-01-01 09:00:00', 'status',   'open', NULL),
            ('o1', 'order', TIMESTAMP '2022-01-01 09:00:00', 'priority', NULL,   'high'),
            ('o1', 'order', TIMESTAMP '2022-01-01 11:00:00', 'status',   'done', NULL)
        ) AS t(ocel_id, ocel_type, ocel_time, ocel_changed_field, status, priority)
    """)

    con.execute("""
        CREATE TABLE "event_object" AS
        SELECT * FROM (VALUES
            ('e0', 'o1')
        ) AS t(ocel_event_id, ocel_object_id)
        WHERE FALSE
    """)

    con.execute("""
        CREATE TABLE "object_object" AS
        SELECT * FROM (VALUES
            ('o1', 'o1')
        ) AS t(ocel_source_id, ocel_target_id)
        WHERE FALSE
    """)
    _add_relation_qualifier_columns(con)

    manifest = OCELManifest(
        oceldb_version="0.3.0",
        storage_version="3",
        source="test-simultaneous-updates.sqlite",
        created_at=datetime(2022, 1, 1),
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
                type_attributes={},
            ),
            "object": TableSchema(
                name="object",
                core_columns={
                    "ocel_id": "VARCHAR",
                    "ocel_type": "VARCHAR",
                },
            ),
            "object_change": TableSchema(
                name="object_change",
                core_columns={
                    "ocel_id": "VARCHAR",
                    "ocel_type": "VARCHAR",
                    "ocel_time": "TIMESTAMP",
                    "ocel_changed_field": "VARCHAR",
                },
                custom_columns={
                    "status": "VARCHAR",
                    "priority": "VARCHAR",
                },
                type_attributes={
                    "order": ("status", "priority"),
                },
            ),
            "event_object": TableSchema(
                name="event_object",
                core_columns=EVENT_OBJECT_CORE_COLUMNS,
            ),
            "object_object": TableSchema(
                name="object_object",
                core_columns=OBJECT_OBJECT_CORE_COLUMNS,
            ),
        },
    )

    return OCEL(
        path=tmp_path / "test-simultaneous-updates",
        con=con,
        manifest=manifest,
    )
