from __future__ import annotations

from oceldb import OCEL

from oceldb_ui.json_utils import sanitize_rows


def get_table_preview(
    ocel: OCEL, source: str, limit: int = 100, offset: int = 0
) -> dict:
    if source == "event":
        base = ocel.tables.events()
    elif source == "object":
        base = ocel.tables.objects()
    elif source == "event_object":
        base = ocel.tables.event_objects()
    elif source == "object_object":
        base = ocel.tables.object_objects()
    else:
        raise ValueError(f"Unsupported table source: {source!r}")

    total_count = base.count()

    query_sql = base.to_sql()
    page_sql = f"{query_sql} LIMIT {limit} OFFSET {offset}"
    rel = ocel.sql(page_sql)

    rows = rel.fetchall()
    columns = [c[0] for c in rel.description]

    return {
        "columns": columns,
        "rows": sanitize_rows(rows),
        "total_count": total_count,
    }
