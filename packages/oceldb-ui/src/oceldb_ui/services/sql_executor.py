from __future__ import annotations

import time

from oceldb import OCEL

from oceldb_ui.json_utils import sanitize_rows


def execute_sql(ocel: OCEL, query: str, limit: int = 1000) -> dict:
    start = time.perf_counter()
    rel = ocel.sql(query)
    elapsed_ms = (time.perf_counter() - start) * 1000

    columns = [c[0] for c in rel.description]
    rows = rel.fetchmany(limit)

    return {
        "columns": columns,
        "rows": sanitize_rows(rows),
        "row_count": len(rows),
        "execution_time_ms": round(elapsed_ms, 2),
    }
