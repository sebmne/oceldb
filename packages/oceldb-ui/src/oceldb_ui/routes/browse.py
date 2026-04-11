from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends

from oceldb import OCEL

from oceldb_ui.deps import get_ocel
from oceldb_ui.models.browse import BrowseResponse
from oceldb_ui.services.browser import get_table_preview

router = APIRouter(prefix="/api")


@router.get("/browse/{source}")
def browse_table(
    source: Literal["event", "object", "event_object", "object_object"],
    limit: int = 100,
    offset: int = 0,
    ocel: OCEL = Depends(get_ocel),
) -> BrowseResponse:
    result = get_table_preview(ocel, source, limit=limit, offset=offset)
    return BrowseResponse(**result)
