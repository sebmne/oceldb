"""Polars-backed access to OCEL 2.0 logs.

The top-level package exposes the lightweight :class:`OCEL` handle and lazily
loads SQLite import support on demand:

```
from oceldb import OCEL, read_sqlite

ocel = OCEL.read("converted-log")
sqlite_ocel = read_sqlite("export.sqlite")
```

All query accessors return :class:`polars.LazyFrame` objects. Call ``collect()``
or another Polars execution method to materialize results.
"""

from typing import TYPE_CHECKING

from oceldb.ocel import OCEL

if TYPE_CHECKING:
    from oceldb.io.sqlite import read_sqlite

__all__ = ["OCEL", "read_sqlite"]


def __getattr__(name: str) -> object:
    # Lazy so `import oceldb` stays DuckDB-free; conversion pulls duckdb only here.
    if name == "read_sqlite":
        from oceldb.io.sqlite import read_sqlite

        return read_sqlite
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
