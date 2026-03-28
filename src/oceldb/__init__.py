"""oceldb — Filter OCEL 2.0 event logs on disk via DuckDB.

Usage::

    from oceldb import Ocel, event, obj

    with Ocel.read("log.sqlite") as ocel:
        filtered = (ocel.view()
            .where(event.type == "Create Order")
            .where(event.time > "2022-01-01")
            .create())
        filtered.to_sqlite("filtered.sqlite")
"""

from oceldb.expr import event, obj
from oceldb.ocel import Ocel
from oceldb.view import ViewBuilder

__all__ = [
    "Ocel",
    "ViewBuilder",
    "event",
    "obj",
]
