"""Canonical value handling shared by readers and writers."""

from datetime import date, datetime, time, timezone
from typing import Any, Literal

from oceldb.io.errors import ValidationMode, issue
from oceldb.schema import AttributeType

ValueStyle = Literal["json", "xml", "sqlite", "xes"]


def parse_datetime(
    value: Any, *, validation: ValidationMode, context: str
) -> datetime | None:
    """Parse an ISO 8601 timestamp and normalize it to UTC."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            issue(validation, f"{context}: invalid ISO 8601 timestamp {value!r}.")
            return None
    else:
        issue(validation, f"{context}: expected an ISO timestamp, got {value!r}.")
        return None
    if parsed.tzinfo is None:
        issue(validation, f"{context}: timestamp has no UTC offset: {value!r}.")
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def coerce_attribute(
    value: Any,
    attr_type: AttributeType,
    *,
    validation: ValidationMode,
    context: str,
) -> Any:
    """Normalize an exchange value to its declared OCEL type."""
    if value is None:
        return None
    try:
        if attr_type is AttributeType.STRING:
            return str(value)
        if attr_type is AttributeType.TIME:
            return parse_datetime(value, validation=validation, context=context)
        if attr_type is AttributeType.INTEGER:
            if isinstance(value, bool):
                raise ValueError
            return int(value)
        if attr_type is AttributeType.FLOAT:
            if isinstance(value, bool):
                raise ValueError
            return float(value)
        if attr_type is AttributeType.BOOLEAN:
            if isinstance(value, bool):
                return value
            if isinstance(value, int) and value in (0, 1):
                return bool(value)
            if isinstance(value, str) and value.strip().lower() in {
                "true",
                "false",
                "1",
                "0",
            }:
                return value.strip().lower() in {"true", "1"}
    except (TypeError, ValueError, OverflowError):
        pass
    issue(
        validation,
        f"{context}: value {value!r} does not match declared type {attr_type.value!r}.",
    )
    return None


def format_datetime(value: date | datetime) -> str:
    """Format a date or datetime as a canonical UTC ISO 8601 timestamp."""
    if isinstance(value, datetime):
        timestamp = value
    else:
        timestamp = datetime.combine(value, time.min, tzinfo=timezone.utc)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def encode_scalar(value: Any, *, style: ValueStyle) -> Any:
    """Encode a normalized scalar using a format's required representation."""
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return format_datetime(value)
    if isinstance(value, bool):
        if style == "xml":
            return "1" if value else "0"
        if style == "xes":
            return "true" if value else "false"
        return value
    if style in {"xml", "xes"}:
        return str(value)
    return value


def encode_attribute(
    value: Any,
    attr_type: AttributeType,
    *,
    style: Literal["json", "xml", "sqlite"],
    validation: ValidationMode,
    context: str,
) -> Any:
    """Coerce and encode a declared OCEL attribute for an exchange format."""
    normalized = coerce_attribute(
        value, attr_type, validation=validation, context=context
    )
    return encode_scalar(normalized, style=style)


def qualifier(value: Any, *, validation: ValidationMode, context: str) -> str:
    """Require the string qualifier mandated by OCEL 2.0."""
    if isinstance(value, str):
        return value
    issue(validation, f"{context}: qualifier must be a string, got {value!r}.")
    return "" if value is None else str(value)
