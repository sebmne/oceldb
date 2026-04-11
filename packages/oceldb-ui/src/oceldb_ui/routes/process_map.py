from __future__ import annotations

from fastapi import APIRouter, Depends

from oceldb import OCEL

from oceldb_ui.deps import get_ocel
from oceldb_ui.models.process_map import DfgResponse, VariantsResponse
from oceldb_ui.services.process_map import compute_dfg, compute_variants

router = APIRouter(prefix="/api")


@router.get("/process-map/dfg/{object_type}")
def get_dfg(
    object_type: str,
    ocel: OCEL = Depends(get_ocel),
) -> DfgResponse:
    result = compute_dfg(ocel, object_type)
    return DfgResponse(**result)


@router.get("/process-map/variants/{object_type}")
def get_variants(
    object_type: str,
    ocel: OCEL = Depends(get_ocel),
) -> VariantsResponse:
    variants = compute_variants(ocel, object_type)
    return VariantsResponse(variants=variants)
