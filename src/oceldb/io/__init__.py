"""File-format import helpers.

The SQLite converter depends on DuckDB, so this package exposes it lazily. Use
``convert_sqlite`` when you want to persist a native oceldb directory, and
``read_sqlite`` when you want to open a SQLite export through the conversion
cache.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from oceldb.io.sqlite import convert_sqlite, read_sqlite

__all__ = ["convert_sqlite", "read_sqlite"]


def __getattr__(name: str) -> object:
    if name in __all__:
        from oceldb.io.sqlite import convert_sqlite, read_sqlite

        exports = {
            "convert_sqlite": convert_sqlite,
            "read_sqlite": read_sqlite,
        }
        return exports[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
