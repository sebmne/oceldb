from __future__ import annotations

from fastapi import APIRouter, Depends

from oceldb import OCEL

from oceldb_ui.deps import get_ocel
from oceldb_ui.models.table_spec import TablePreviewResponse, TableSpecModel
from oceldb_ui.services.table_builder import preview_table_spec

router = APIRouter(prefix="/api")


@router.post("/table/preview")
def preview_table(
    spec: TableSpecModel,
    ocel: OCEL = Depends(get_ocel),
) -> TablePreviewResponse:
    result = preview_table_spec(ocel, spec.model_dump())
    return TablePreviewResponse(**result)
