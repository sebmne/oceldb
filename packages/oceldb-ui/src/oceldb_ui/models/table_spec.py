from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel


CastName = Literal["int", "float", "str", "bool", "datetime"]
TableSource = Literal["event", "object", "event_object", "object_object"]
AggKind = Literal["count", "count_distinct", "min", "max", "sum", "avg"]
SortDirection = Literal["ASC", "DESC"]


class SourceFieldModel(BaseModel):
    kind: Literal["field"]
    name: str
    cast: CastName | None = None


class SelectItemModel(BaseModel):
    expr: SourceFieldModel
    alias: str | None = None


class GroupByItemModel(BaseModel):
    expr: SourceFieldModel


class AggItemModel(BaseModel):
    kind: AggKind
    expr: SourceFieldModel | None = None
    alias: str | None = None


class OrderByItemModel(BaseModel):
    by: str | SourceFieldModel
    direction: SortDirection


class TableSpecModel(BaseModel):
    source: TableSource
    select: list[SelectItemModel] = []
    group_by: list[GroupByItemModel] = []
    agg: list[AggItemModel] = []
    order_by: list[OrderByItemModel] = []
    distinct: bool = False
    limit: int | None = None


class TablePreviewResponse(BaseModel):
    sql: str
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
