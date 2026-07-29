"""OCEL 2.0 exchange-format conversion.

The functions in this module convert SQLite, JSON, and XML exchange files
directly into oceldb's native storage format. They do not materialize the
complete log in memory.
"""

from oceldb.io._api import (
    ExchangeFormat,
    convert_json,
    convert_ocel,
    convert_sqlite,
    convert_xml,
    detect_format,
)
from oceldb.io._errors import OCELConversionError

__all__ = [
    "ExchangeFormat",
    "OCELConversionError",
    "convert_json",
    "convert_ocel",
    "convert_sqlite",
    "convert_xml",
    "detect_format",
]
