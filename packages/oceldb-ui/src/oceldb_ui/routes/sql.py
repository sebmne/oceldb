from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from oceldb import OCEL

from oceldb_ui.deps import get_ocel
from oceldb_ui.models.sql import SqlRequest, SqlResponse
from oceldb_ui.services.sql_executor import execute_sql

router = APIRouter(prefix="/api")


@router.post("/sql/execute")
def run_sql(
    req: SqlRequest,
    ocel: OCEL = Depends(get_ocel),
) -> SqlResponse:
    try:
        result = execute_sql(ocel, req.query, limit=req.limit)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    return SqlResponse(**result)
