from __future__ import annotations

from pydantic import BaseModel


class OverviewStats(BaseModel):
    event_count: int
    object_count: int
    event_type_count: int
    object_type_count: int
    earliest_event_time: str | None
    latest_event_time: str | None


class EventObjectStats(BaseModel):
    avg_objects_per_event: float | None
    min_objects_per_event: int | None
    max_objects_per_event: int | None
    avg_events_per_object: float | None
    min_events_per_object: int | None
    max_events_per_object: int | None


class OverviewResponse(BaseModel):
    overview: OverviewStats
    event_type_counts: dict[str, int]
    object_type_counts: dict[str, int]
    event_object_stats: EventObjectStats


class TypesResponse(BaseModel):
    event_types: list[str]
    object_types: list[str]


class AttributesResponse(BaseModel):
    attributes: list[str]


class SchemaColumn(BaseModel):
    name: str
    type: str


class SchemaResponse(BaseModel):
    columns: list[SchemaColumn]


class MetadataResponse(BaseModel):
    source: str
    oceldb_version: str
    converted_at: str | None
