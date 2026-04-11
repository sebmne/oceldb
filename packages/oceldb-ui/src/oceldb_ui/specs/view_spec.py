from __future__ import annotations

from typing import Any, Literal, Optional, TypedDict, Union

ViewRoot = Literal["event", "object"]
CastName = Literal["int", "float", "str", "bool", "datetime"]


class FieldRefSpec(TypedDict):
    kind: Literal["field"]
    name: str
    cast: Optional[CastName]


class AttrRefSpec(TypedDict):
    kind: Literal["attr"]
    name: str
    cast: Optional[CastName]


ExprRefSpec = Union[FieldRefSpec, AttrRefSpec]


class ComparisonFilterSpec(TypedDict):
    kind: Literal["comparison"]
    left: ExprRefSpec
    op: Literal["==", "!=", ">", ">=", "<", "<="]
    right: Any


class NullFilterSpec(TypedDict):
    kind: Literal["null_check"]
    expr: ExprRefSpec
    op: Literal["is_null", "is_not_null"]


class RelatedExistsFilterSpec(TypedDict):
    kind: Literal["related_exists"]
    object_type: str


class LinkedExistsFilterSpec(TypedDict):
    kind: Literal["linked_exists"]
    object_type: str


class HasEventExistsFilterSpec(TypedDict):
    kind: Literal["has_event_exists"]
    event_type: str


ViewFilterSpec = Union[
    ComparisonFilterSpec,
    NullFilterSpec,
    RelatedExistsFilterSpec,
    LinkedExistsFilterSpec,
    HasEventExistsFilterSpec,
]


class ViewSpec(TypedDict):
    root: ViewRoot
    types: list[str]
    filters: list[ViewFilterSpec]
