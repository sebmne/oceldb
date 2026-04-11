from __future__ import annotations

from pydantic import BaseModel


class DfgNode(BaseModel):
    id: str
    label: str
    frequency: int
    is_start: bool
    is_end: bool
    start_count: int
    end_count: int


class DfgEdge(BaseModel):
    source: str
    target: str
    frequency: int


class DfgResponse(BaseModel):
    nodes: list[DfgNode]
    edges: list[DfgEdge]


class VariantEntry(BaseModel):
    id: int
    activities: list[str]
    frequency: int
    percentage: float


class VariantsResponse(BaseModel):
    variants: list[VariantEntry]
