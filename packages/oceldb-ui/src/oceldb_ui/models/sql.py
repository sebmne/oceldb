from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class SqlRequest(BaseModel):
    query: str
    limit: int = 1000


class SqlResponse(BaseModel):
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    execution_time_ms: float
