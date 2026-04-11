from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any


def sanitize_value(value: Any) -> Any:
    """Convert DuckDB values to JSON-serializable types."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, (list, tuple)):
        return [sanitize_value(v) for v in value]
    if isinstance(value, dict):
        return {k: sanitize_value(v) for k, v in value.items()}
    return value


def sanitize_rows(rows: list[tuple]) -> list[list]:
    return [[sanitize_value(v) for v in row] for row in rows]
