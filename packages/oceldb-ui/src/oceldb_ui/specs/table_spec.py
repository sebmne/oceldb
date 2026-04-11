from __future__ import annotations

from typing import Literal, Optional, TypedDict, Union

CastName = Literal["int", "float", "str", "bool", "datetime"]
TableSource = Literal["event", "object", "event_object", "object_object"]
AggKind = Literal["count", "count_distinct", "min", "max", "sum", "avg"]
SortDirection = Literal["ASC", "DESC"]


class SourceFieldSpec(TypedDict):
    kind: Literal["field"]
    name: str
    cast: Optional[CastName]


class SelectItemSpec(TypedDict):
    expr: SourceFieldSpec
    alias: Optional[str]


class GroupByItemSpec(TypedDict):
    expr: SourceFieldSpec


class AggItemSpec(TypedDict):
    kind: AggKind
    expr: Optional[SourceFieldSpec]
    alias: Optional[str]


class OrderByItemSpec(TypedDict):
    by: Union[str, SourceFieldSpec]
    direction: SortDirection


class TableSpec(TypedDict):
    source: TableSource
    select: list[SelectItemSpec]
    group_by: list[GroupByItemSpec]
    agg: list[AggItemSpec]
    order_by: list[OrderByItemSpec]
    distinct: bool
    limit: Optional[int]
