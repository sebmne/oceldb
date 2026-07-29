"""Canonical scalar conversion for OCEL 2.0 exchange formats."""

from __future__ import annotations

from datetime import datetime, timezone
import math
from typing import Literal

import polars as pl

from oceldb.io._errors import conversion_error
from oceldb.io._schema import AttributeType

ValueStyle = Literal["json", "xml", "sqlite"]
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def timestamp(
    value: object,
    *,
    context: str,
    allow_naive: bool = False,
) -> datetime:
    """Parse an ISO 8601 timestamp and normalize it to UTC."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            raise conversion_error(context, "timestamp must not be empty")
        try:
            parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
        except ValueError as exc:
            raise conversion_error(
                context,
                f"invalid ISO 8601 timestamp {value!r}",
            ) from exc
    else:
        raise conversion_error(context, "timestamp must be an ISO 8601 string")
    if parsed.tzinfo is None:
        if not allow_naive:
            raise conversion_error(context, "timestamp must include a UTC offset")
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def attribute_value(
    value: object,
    declared: AttributeType,
    storage_dtype: pl.DataType,
    *,
    style: ValueStyle,
    context: str,
) -> object:
    """Validate and convert an attribute to its reconciled native dtype."""
    normalized = _normalize(value, declared, style=style, context=context)
    if storage_dtype == pl.String and declared is not AttributeType.STRING:
        return _stringify(normalized)
    if storage_dtype == pl.Float64 and declared is AttributeType.INTEGER:
        assert isinstance(normalized, int) and not isinstance(normalized, bool)
        return float(normalized)
    return normalized


def _normalize(
    value: object,
    declared: AttributeType,
    *,
    style: ValueStyle,
    context: str,
) -> object:
    if value is None:
        raise conversion_error(context, "attribute value must not be null")
    if declared is AttributeType.STRING:
        if not isinstance(value, str):
            raise conversion_error(context, "expected a string")
        return value
    if declared is AttributeType.TIME:
        return timestamp(
            value,
            context=context,
            allow_naive=style == "sqlite",
        )
    if declared is AttributeType.INTEGER:
        if isinstance(value, bool):
            raise conversion_error(context, "expected an integer, got boolean")
        if isinstance(value, int):
            return value
        if style in {"xml", "sqlite"} and isinstance(value, str):
            try:
                return int(value)
            except ValueError as exc:
                raise conversion_error(context, f"invalid integer {value!r}") from exc
        raise conversion_error(context, "expected an integer")
    if declared is AttributeType.FLOAT:
        if isinstance(value, bool):
            raise conversion_error(context, "expected a float, got boolean")
        numeric: int | float | str
        if isinstance(value, (int, float)):
            numeric = value
        elif style in {"xml", "sqlite"} and isinstance(value, str):
            numeric = value
        else:
            raise conversion_error(context, "expected a float")
        try:
            result = float(numeric)
        except (TypeError, ValueError) as exc:
            raise conversion_error(context, f"invalid float {numeric!r}") from exc
        if not math.isfinite(result):
            raise conversion_error(context, "float must be finite")
        return result
    if isinstance(value, bool):
        return value
    if style in {"xml", "sqlite"} and value in (0, 1, "0", "1"):
        return value in (1, "1")
    raise conversion_error(context, "expected a boolean")


def _stringify(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
