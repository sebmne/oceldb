from __future__ import annotations

from dataclasses import asdict
from typing import Literal

from fastapi import APIRouter, Depends

from oceldb import OCEL
from oceldb.inspect.attributes import attributes, event_attributes, object_attributes

from oceldb_ui.deps import get_ocel, get_session
from oceldb_ui.json_utils import sanitize_value
from oceldb_ui.models.overview import (
    AttributesResponse,
    EventObjectStats,
    MetadataResponse,
    OverviewResponse,
    OverviewStats,
    SchemaColumn,
    SchemaResponse,
    TypesResponse,
)
from oceldb_ui.session import UISession

router = APIRouter(prefix="/api")


@router.get("/overview")
def get_overview(ocel: OCEL = Depends(get_ocel)) -> OverviewResponse:
    ov = ocel.inspect.overview()
    eo = ocel.inspect.event_object_stats()

    return OverviewResponse(
        overview=OverviewStats(
            event_count=ov.event_count,
            object_count=ov.object_count,
            event_type_count=ov.event_type_count,
            object_type_count=ov.object_type_count,
            earliest_event_time=sanitize_value(ov.earliest_event_time),
            latest_event_time=sanitize_value(ov.latest_event_time),
        ),
        event_type_counts=ocel.inspect.event_type_counts(),
        object_type_counts=ocel.inspect.object_type_counts(),
        event_object_stats=EventObjectStats(**asdict(eo)),
    )


@router.get("/types")
def get_types(ocel: OCEL = Depends(get_ocel)) -> TypesResponse:
    return TypesResponse(
        event_types=ocel.inspect.event_types(),
        object_types=ocel.inspect.object_types(),
    )


@router.get("/attributes/{kind}/{type_name}")
def get_attributes(
    kind: Literal["event", "object"],
    type_name: str,
    ocel: OCEL = Depends(get_ocel),
) -> AttributesResponse:
    if kind == "event":
        attrs = event_attributes(ocel, type_name)
    else:
        attrs = object_attributes(ocel, type_name)
    return AttributesResponse(attributes=attrs)


@router.get("/schema/{table}")
def get_schema(
    table: Literal["event", "object", "event_object", "object_object"],
    ocel: OCEL = Depends(get_ocel),
) -> SchemaResponse:
    rel = ocel.sql(f"DESCRIBE {ocel.schema}.{table}")
    rows = rel.fetchall()
    return SchemaResponse(
        columns=[SchemaColumn(name=row[0], type=row[1]) for row in rows]
    )


@router.get("/metadata")
def get_metadata(ocel: OCEL = Depends(get_ocel)) -> MetadataResponse:
    m = ocel.metadata
    return MetadataResponse(
        source=m.source,
        oceldb_version=m.oceldb_version,
        converted_at=sanitize_value(m.converted_at),
    )
