"""Errors raised by OCEL exchange-format converters."""

from oceldb.errors import OCELDBError


class OCELConversionError(OCELDBError, ValueError):
    """An exchange file cannot be converted without losing OCEL semantics."""


def conversion_error(context: str, message: str) -> OCELConversionError:
    """Build a consistently contextualized conversion error."""
    return OCELConversionError(f"{context}: {message}")
