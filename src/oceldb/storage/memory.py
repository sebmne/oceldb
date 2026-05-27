"""Materialize canonical OCEL relations into an ephemeral DuckDB database."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from uuid import uuid4

import duckdb
import ibis
import ibis.backends.duckdb

from oceldb.io.source import Canonical, Source, release_source, temporary_view
from oceldb.io.sql import quote_identifier, sql_string
from oceldb.storage.manifest import EventTypeInfo, Manifest, ObjectTypeInfo
from oceldb.storage.metadata import build_manifest, count_rows, event_stats
from oceldb.storage.views import build_derived_views


def materialize_memory(
    source: Source,
    *,
    source_kind: str,
    source_path: Path | None,
    progress: Callable[[str], None] | None = None,
) -> tuple[ibis.backends.duckdb.Backend, Manifest]:
    """Load *source* into temporary DuckDB tables suitable for :class:`OCEL`."""
    backend = ibis.duckdb.connect()
    con = backend.con
    try:
        canonical = source.attach(con)
        try:
            say: Callable[[str], None] = progress or (lambda _message: None)

            say("Loading events")
            event_infos = _materialize_events(con, canonical)

            say("Loading objects")
            object_infos = _materialize_objects(con, canonical)

            say("Loading event_object")
            _materialize_relation(con, "event_object", canonical.event_object)
            e2o_count = count_rows(con, "event_object")

            say("Loading object_object")
            o2o_count = 0
            if canonical.object_object is not None:
                _materialize_relation(con, "object_object", canonical.object_object)
                o2o_count = count_rows(con, "object_object")

            manifest = build_manifest(
                source_kind=source_kind,
                source_path=source_path,
                event_types=event_infos,
                object_types=object_infos,
                e2o_count=e2o_count,
                o2o_count=o2o_count,
            )
        finally:
            release_source(source, con)

        build_derived_views(backend, manifest)
        return backend, manifest
    except BaseException:
        backend.disconnect()
        raise


def _materialize_events(
    con: duckdb.DuckDBPyConnection, canonical: Canonical
) -> dict[str, EventTypeInfo]:
    staging: list[str] = []
    infos: dict[str, EventTypeInfo] = {}
    for type_name, attributes in canonical.event_types.items():
        table = _staging_name("event")
        columns = (
            quote_identifier("ocel_id"),
            f"{sql_string(type_name)} AS {quote_identifier('ocel_type')}",
            quote_identifier("ocel_time"),
            *(quote_identifier(name) for name in attributes),
        )
        _materialize_relation(
            con, table, canonical.events_for(type_name), columns=columns
        )
        count, lo, hi = event_stats(con, table)
        infos[type_name] = EventTypeInfo(
            count=count,
            time_range=(lo, hi),
            attributes=dict(attributes),
        )
        staging.append(table)

    _union_staging(
        con,
        "events",
        staging,
        empty_schema="ocel_id VARCHAR, ocel_type VARCHAR, ocel_time TIMESTAMP",
    )
    return infos


def _materialize_objects(
    con: duckdb.DuckDBPyConnection, canonical: Canonical
) -> dict[str, ObjectTypeInfo]:
    object_staging: list[str] = []
    object_counts: dict[str, int] = {}
    for type_name in canonical.object_types:
        table = _staging_name("object")
        columns = (
            quote_identifier("ocel_id"),
            f"{sql_string(type_name)} AS {quote_identifier('ocel_type')}",
        )
        _materialize_relation(
            con, table, canonical.objects_for(type_name), columns=columns
        )
        object_counts[type_name] = count_rows(con, table)
        object_staging.append(table)

    _union_staging(
        con,
        "objects",
        object_staging,
        empty_schema="ocel_id VARCHAR, ocel_type VARCHAR",
    )

    infos: dict[str, ObjectTypeInfo] = {}
    for type_name, attributes in canonical.object_types.items():
        table = f"object_changes_{type_name}"
        columns = (
            quote_identifier("ocel_id"),
            quote_identifier("ocel_time"),
            quote_identifier("ocel_changed_field"),
            *(quote_identifier(name) for name in attributes),
        )
        _materialize_relation(
            con, table, canonical.object_changes_for(type_name), columns=columns
        )
        infos[type_name] = ObjectTypeInfo(
            object_count=object_counts[type_name],
            change_count=count_rows(con, table),
            attributes=dict(attributes),
        )
    return infos


def _materialize_relation(
    con: duckdb.DuckDBPyConnection,
    name: str,
    relation: duckdb.DuckDBPyRelation,
    *,
    columns: tuple[str, ...] | None = None,
) -> None:
    projection = "*" if columns is None else ", ".join(columns)
    with temporary_view(con, relation) as view:
        con.execute(
            f"CREATE TEMP TABLE {quote_identifier(name)} AS "
            f"SELECT {projection} FROM {quote_identifier(view)}"
        )


def _union_staging(
    con: duckdb.DuckDBPyConnection,
    name: str,
    tables: list[str],
    *,
    empty_schema: str,
) -> None:
    if not tables:
        con.execute(f"CREATE TEMP TABLE {quote_identifier(name)} ({empty_schema})")
        return
    selects = " UNION ALL BY NAME ".join(
        f"SELECT * FROM {quote_identifier(table)}" for table in tables
    )
    con.execute(f"CREATE TEMP TABLE {quote_identifier(name)} AS {selects}")
    for table in tables:
        con.execute(f"DROP TABLE {quote_identifier(table)}")


def _staging_name(kind: str) -> str:
    return f"_oceldb_memory_{kind}_{uuid4().hex}"
