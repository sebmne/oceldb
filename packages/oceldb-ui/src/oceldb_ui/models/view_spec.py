from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel


CastName = Literal["int", "float", "str", "bool", "datetime"]
ViewRoot = Literal["event", "object"]


class FieldRefModel(BaseModel):
    kind: Literal["field"]
    name: str
    cast: CastName | None = None


class AttrRefModel(BaseModel):
    kind: Literal["attr"]
    name: str
    cast: CastName | None = None


class ComparisonFilterModel(BaseModel):
    kind: Literal["comparison"]
    left: FieldRefModel | AttrRefModel
    op: Literal["==", "!=", ">", ">=", "<", "<="]
    right: Any


class NullFilterModel(BaseModel):
    kind: Literal["null_check"]
    expr: FieldRefModel | AttrRefModel
    op: Literal["is_null", "is_not_null"]


class RelatedExistsFilterModel(BaseModel):
    kind: Literal["related_exists"]
    object_type: str


class LinkedExistsFilterModel(BaseModel):
    kind: Literal["linked_exists"]
    object_type: str


class HasEventExistsFilterModel(BaseModel):
    kind: Literal["has_event_exists"]
    event_type: str


ViewFilterModel = (
    ComparisonFilterModel
    | NullFilterModel
    | RelatedExistsFilterModel
    | LinkedExistsFilterModel
    | HasEventExistsFilterModel
)


class ViewSpecModel(BaseModel):
    root: ViewRoot
    types: list[str] = []
    filters: list[ViewFilterModel] = []


class ViewPreviewResponse(BaseModel):
    count: int
    sql: str
    columns: list[str]
    rows: list[list[Any]]


class SubLogResponse(BaseModel):
    id: str
    root: str
    types: list[str]
    filter_count: int


class SubLogListResponse(BaseModel):
    sublogs: list[SubLogResponse]
