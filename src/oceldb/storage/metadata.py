"""Build and measure storage-independent OCEL manifest metadata."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from oceldb.io.sql import quote_identifier
from oceldb.storage.manifest import EventTypeInfo, Manifest, ObjectTypeInfo

if TYPE_CHECKING:
    import duckdb


def build_manifest(
    *,
    source_kind: str,
    source_path: Path | None,
    event_types: dict[str, EventTypeInfo],
    object_types: dict[str, ObjectTypeInfo],
    e2o_count: int,
    o2o_count: int,
) -> Manifest:
    starts = [
        info.time_range[0]
        for info in event_types.values()
        if info.time_range[0] is not None
    ]
    ends = [
        info.time_range[1]
        for info in event_types.values()
        if info.time_range[1] is not None
    ]
    source: dict[str, object] = {"kind": source_kind}
    if source_path is not None:
        source["path"] = str(source_path.resolve())
    return Manifest(
        oceldb_format_version="1",
        ocel_version="2.0",
        created_at=datetime.now(timezone.utc).isoformat(),
        source=source,
        layout="type",
        totals={
            "event_count": sum(info.count for info in event_types.values()),
            "object_count": sum(info.object_count for info in object_types.values()),
            "e2o_count": e2o_count,
            "o2o_count": o2o_count,
            "time_range": [
                min(starts) if starts else None,
                max(ends) if ends else None,
            ],
        },
        event_types=event_types,
        object_types=object_types,
    )


def count_rows(con: duckdb.DuckDBPyConnection, relation_name: str) -> int:
    row = con.execute(
        f"SELECT COUNT(*) FROM {quote_identifier(relation_name)}"
    ).fetchone()
    return int(row[0]) if row else 0


def event_stats(
    con: duckdb.DuckDBPyConnection, relation_name: str
) -> tuple[int, str | None, str | None]:
    row = con.execute(
        f"SELECT COUNT(*), MIN(ocel_time)::TEXT, MAX(ocel_time)::TEXT "
        f"FROM {quote_identifier(relation_name)}"
    ).fetchone()
    assert row is not None
    return int(row[0]), row[1], row[2]
