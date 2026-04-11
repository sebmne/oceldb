from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException

from oceldb import OCEL

from oceldb_ui.deps import get_ocel, get_session
from oceldb_ui.models.browse import BrowseResponse
from oceldb_ui.models.view_spec import (
    SubLogListResponse,
    SubLogResponse,
    ViewPreviewResponse,
    ViewSpecModel,
)
from oceldb_ui.services.browser import get_table_preview
from oceldb_ui.services.view_builder import materialize_view_spec, preview_view_spec
from oceldb_ui.session import SubLog, UISession

router = APIRouter(prefix="/api")


@router.post("/view/preview")
def preview_view(
    spec: ViewSpecModel,
    ocel: OCEL = Depends(get_ocel),
) -> ViewPreviewResponse:
    result = preview_view_spec(ocel, spec.model_dump())
    return ViewPreviewResponse(**result)


@router.post("/view/materialize")
def materialize_view(
    spec: ViewSpecModel,
    session: UISession = Depends(get_session),
) -> SubLogResponse:
    sub_ocel = materialize_view_spec(session.ocel, spec.model_dump())
    sub_id = uuid.uuid4().hex[:8]

    sublog = SubLog(
        id=sub_id,
        ocel=sub_ocel,
        root=spec.root,
        types=spec.types,
        filter_count=len(spec.filters),
    )
    session.sublogs[sub_id] = sublog

    return SubLogResponse(
        id=sub_id,
        root=sublog.root,
        types=sublog.types,
        filter_count=sublog.filter_count,
    )


@router.get("/sublogs")
def list_sublogs(
    session: UISession = Depends(get_session),
) -> SubLogListResponse:
    return SubLogListResponse(
        sublogs=[
            SubLogResponse(
                id=s.id,
                root=s.root,
                types=s.types,
                filter_count=s.filter_count,
            )
            for s in session.sublogs.values()
        ]
    )


@router.get("/sublogs/{sublog_id}/browse/{source}")
def browse_sublog(
    sublog_id: str,
    source: str,
    limit: int = 100,
    offset: int = 0,
    session: UISession = Depends(get_session),
) -> BrowseResponse:
    sublog = session.sublogs.get(sublog_id)
    if sublog is None:
        raise HTTPException(status_code=404, detail=f"Sub-log {sublog_id!r} not found")

    result = get_table_preview(sublog.ocel, source, limit=limit, offset=offset)
    return BrowseResponse(**result)


@router.delete("/sublogs/{sublog_id}")
def delete_sublog(
    sublog_id: str,
    session: UISession = Depends(get_session),
) -> dict:
    sublog = session.sublogs.pop(sublog_id, None)
    if sublog is None:
        raise HTTPException(status_code=404, detail=f"Sub-log {sublog_id!r} not found")

    sublog.ocel.close()
    return {"ok": True}
