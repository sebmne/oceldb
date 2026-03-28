"""SQL helper utilities for escaping and literal conversion."""

from __future__ import annotations

from oceldb.types import ScalarValue


def escape_string(value: str) -> str:
    """Escape single quotes for safe inclusion in SQL string literals.

    Args:
        value: The raw string value.

    Returns:
        The escaped string (``'`` becomes ``''``).
    """
    return value.replace("'", "''")


def sql_literal(value: ScalarValue) -> str:
    """Convert a Python scalar to a SQL literal.

    Strings are single-quoted and escaped. Numeric types are passed through
    as-is. Booleans are explicitly rejected to prevent silent type coercion
    (``bool`` is a subclass of ``int`` in Python).

    Args:
        value: A string, int, or float to convert.

    Returns:
        A string safe for interpolation into a SQL query.

    Raises:
        TypeError: If *value* is a ``bool``.
    """
    if isinstance(value, bool):
        raise TypeError(
            f"Boolean {value!r} is not a valid scalar value — use 1/0 or 'true'/'false'"
        )
    if isinstance(value, str):
        return f"'{escape_string(value)}'"
    return str(value)
