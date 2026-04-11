from __future__ import annotations

from oceldb import OCEL

from oceldb_ui.builders.compile_view_spec import compile_view_spec
from oceldb_ui.json_utils import sanitize_rows


def preview_view_spec(ocel: OCEL, spec: dict, limit: int = 100) -> dict:
    view = compile_view_spec(ocel, spec)

    count = view.count()
    sql = view.to_sql()

    page_sql = f"{sql} LIMIT {limit}"
    rel = ocel.sql(page_sql)
    rows = rel.fetchall()
    columns = [c[0] for c in rel.description]

    return {
        "count": count,
        "sql": sql,
        "columns": columns,
        "rows": sanitize_rows(rows),
    }


def materialize_view_spec(ocel: OCEL, spec: dict) -> OCEL:
    view = compile_view_spec(ocel, spec)
    return view.to_ocel()
