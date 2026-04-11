from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class BrowseResponse(BaseModel):
    columns: list[str]
    rows: list[list[Any]]
    total_count: int
