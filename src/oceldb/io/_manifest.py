from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import cast

import duckdb

from oceldb.core.manifest import LogicalTableName, OCELManifest, TableSchema

MANIFEST_FILE = "manifest.json"
STORAGE_VERSION = "2"
LOGICAL_TABLES: tuple[LogicalTableName, ...] = (
    "event",
    "object",
    "object_change",
    "event_object",
    "object_object",
)
CORE_COLUMNS: dict[LogicalTableName, dict[str, str]] = {
    "event": {
        "ocel_id": "VARCHAR",
        "ocel_type": "VARCHAR",
        "ocel_time": "TIMESTAMP",
    },
    "object": {
        "ocel_id": "VARCHAR",
        "ocel_type": "VARCHAR",
    },
    "object_change": {
        "ocel_id": "VARCHAR",
        "ocel_type": "VARCHAR",
        "ocel_time": "TIMESTAMP",
        "ocel_changed_field": "VARCHAR",
    },
    "event_object": {
        "ocel_event_id": "VARCHAR",
        "ocel_object_id": "VARCHAR",
    },
    "object_object": {
        "ocel_source_id": "VARCHAR",
        "ocel_target_id": "VARCHAR",
    },
}


def load_manifest(path: Path) -> OCELManifest:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid manifest.json in '{path.parent}': {e}") from e

    raw_manifest = _expect_object(raw, path="manifest")

    required = {
        "format",
        "storage_version",
        "oceldb_version",
        "source",
        "created_at",
        "tables",
    }
    missing = required - raw_manifest.keys()
    if missing:
        raise ValueError(
            f"manifest.json in '{path.parent}' is missing required keys: "
            f"{', '.join(sorted(missing))}"
        )

    format_name = _expect_str(raw_manifest["format"], path="manifest.format")
    if format_name != "oceldb":
        raise ValueError(
            f"Unsupported manifest format in '{path.parent}': {format_name!r}"
        )

    storage_version = _expect_str(
        raw_manifest["storage_version"],
        path="manifest.storage_version",
    )
    if storage_version != STORAGE_VERSION:
        raise ValueError(
            f"Unsupported oceldb storage version in '{path.parent}': "
            f"{storage_version!r}. Expected {STORAGE_VERSION!r}."
        )

    try:
        created_at = datetime.fromisoformat(
            _expect_str(raw_manifest["created_at"], path="manifest.created_at")
        )
    except Exception as e:
        raise ValueError(
            f"Invalid created_at value in manifest.json: {raw_manifest['created_at']!r}"
        ) from e

    raw_tables = _expect_object(raw_manifest["tables"], path="manifest.tables")

    tables: dict[LogicalTableName, TableSchema] = {}
    for name in LOGICAL_TABLES:
        custom_columns = _load_custom_columns(raw_tables, table_name=name)
        tables[name] = TableSchema(
            name=name,
            core_columns=CORE_COLUMNS[name],
            custom_columns=custom_columns,
        )

    return OCELManifest(
        oceldb_version=_expect_str(
            raw_manifest["oceldb_version"],
            path="manifest.oceldb_version",
        ),
        storage_version=storage_version,
        source=_expect_str(raw_manifest["source"], path="manifest.source"),
        created_at=created_at,
        tables=tables,
    )


def build_manifest_from_tables(
    con: duckdb.DuckDBPyConnection,
    *,
    oceldb_version: str,
    source: str,
    created_at: datetime,
    schema: str | None = None,
    drop_empty_custom_columns: bool = False,
) -> OCELManifest:
    tables: dict[LogicalTableName, TableSchema] = {}
    for table_name in LOGICAL_TABLES:
        tables[table_name] = _build_table_schema(
            con,
            table_name=table_name,
            schema=schema,
            drop_empty_custom_columns=drop_empty_custom_columns,
        )

    return OCELManifest(
        oceldb_version=oceldb_version,
        storage_version=STORAGE_VERSION,
        source=source,
        created_at=created_at,
        tables=tables,
    )


def write_manifest(path: Path, manifest: OCELManifest) -> None:
    raw = {
        "format": "oceldb",
        "storage_version": manifest.storage_version,
        "oceldb_version": manifest.oceldb_version,
        "source": manifest.source,
        "created_at": manifest.created_at.isoformat(),
        "tables": {
            name: {
                "custom_columns": dict(schema.custom_columns),
            }
            for name, schema in manifest.tables.items()
        },
    }

    path.write_text(json.dumps(raw, indent=2), encoding="utf-8")


def _build_table_schema(
    con: duckdb.DuckDBPyConnection,
    *,
    table_name: LogicalTableName,
    schema: str | None,
    drop_empty_custom_columns: bool,
) -> TableSchema:
    location = _qualified_table_name(table_name, schema=schema)
    rows = con.execute(
        f"DESCRIBE {location}"
    ).fetchall()
    actual_columns = {
        str(row[0]): str(row[1])
        for row in rows
    }
    core_columns = CORE_COLUMNS[table_name]
    missing = set(core_columns) - set(actual_columns)
    if missing:
        raise ValueError(
            f"Cannot build manifest for {location}: missing required columns "
            f"{', '.join(sorted(missing))}"
        )

    present_custom_columns = set(actual_columns) - set(core_columns)
    if drop_empty_custom_columns and present_custom_columns:
        present_custom_columns = _non_null_custom_columns(
            con,
            location=location,
            column_names=tuple(sorted(present_custom_columns)),
        )

    custom_columns = {
        name: sql_type
        for name, sql_type in actual_columns.items()
        if name in present_custom_columns
    }
    return TableSchema(
        name=table_name,
        core_columns=core_columns,
        custom_columns=custom_columns,
    )


def _load_custom_columns(
    raw_tables: dict[str, object],
    *,
    table_name: LogicalTableName,
) -> dict[str, str]:
    raw_table = raw_tables.get(table_name)
    if raw_table is None:
        return {}
    if not isinstance(raw_table, dict):
        raise ValueError(
            f"Invalid tables.{table_name!s} section in manifest.json: expected an object"
        )
    raw_table_dict = cast(dict[str, object], raw_table)

    raw_custom_columns = raw_table_dict.get("custom_columns", {})
    if not isinstance(raw_custom_columns, dict):
        raise ValueError(
            f"Invalid tables.{table_name!s}.custom_columns section in manifest.json: expected an object"
        )
    raw_custom_columns_dict = cast(dict[str, object], raw_custom_columns)

    return {
        str(name): str(sql_type)
        for name, sql_type in raw_custom_columns_dict.items()
    }


def _expect_object(value: object, *, path: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"Invalid {path} section in manifest.json: expected an object")
    return cast(dict[str, object], value)


def _expect_str(value: object, *, path: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"Invalid {path} value in manifest.json: expected a string")
    return value


def _qualified_table_name(table_name: str, *, schema: str | None) -> str:
    quoted_table = _quote_ident(table_name)
    if schema is None:
        return quoted_table
    return f'{_quote_ident(schema)}.{quoted_table}'


def _quote_ident(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _non_null_custom_columns(
    con: duckdb.DuckDBPyConnection,
    *,
    location: str,
    column_names: tuple[str, ...],
) -> set[str]:
    if not column_names:
        return set()

    select_sql = ", ".join(
        f'COUNT(*) FILTER (WHERE {_quote_ident(name)} IS NOT NULL) > 0 AS {_quote_ident(name)}'
        for name in column_names
    )
    row = con.execute(f"""
        SELECT {select_sql}
        FROM {location}
    """).fetchone()
    if row is None:
        return set()

    return {
        name
        for name, has_value in zip(column_names, row, strict=False)
        if bool(has_value)
    }
