from __future__ import annotations

from oceldb import OCEL

from oceldb_ui.builders.compile_table_spec import compile_table_spec
from oceldb_ui.json_utils import sanitize_rows


def preview_table_spec(ocel: OCEL, spec: dict) -> dict:
    query = compile_table_spec(ocel, spec).limit(spec.get("limit") or 100)
    rel = query.relation()

    rows = rel.fetchall()

    return {
        "sql": query.to_sql(),
        "columns": [c[0] for c in rel.description],
        "rows": sanitize_rows(rows),
        "row_count": len(rows),
    }
