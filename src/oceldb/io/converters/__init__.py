"""Built-in source registrations."""

from __future__ import annotations

from oceldb.io.converters import json as _json
from oceldb.io.converters import sqlite as _sqlite
from oceldb.io.converters import xml as _xml
from oceldb.io.registry import register

register(_json.SPEC)
register(_xml.SPEC)
register(_sqlite.SPEC)
