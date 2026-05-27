"""DuckDB SQL and partition-name helpers."""

from __future__ import annotations

import urllib.parse


def quote_identifier(name: str) -> str:
    """Wrap *name* in double quotes, doubling any embedded double quotes."""
    return '"' + name.replace('"', '""') + '"'


def sql_string(value: str) -> str:
    """Wrap *value* in single quotes, doubling any embedded single quotes."""
    return "'" + value.replace("'", "''") + "'"


def encode_type_name(type_name: str) -> str:
    """URL-encode a type name for use in a Hive partition directory name."""
    return urllib.parse.quote(type_name, safe="")
