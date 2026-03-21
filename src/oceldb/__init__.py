"""oceldb — Filter OCEL 2.0 event logs on disk via DuckDB.

Usage::

    from oceldb import Ocel, event, obj

    ocel = Ocel.read("log.sqlite")
    filtered = (
        ocel.view()
        .filter(event.type == "Create Order")
        .filter(event.time > "2022-01-01")
        .filter(obj.type == "order")
        .create()
    )
    filtered.to_sqlite("filtered.sqlite")
    pm4py_ocel = filtered.to_pm4py()
"""

from .expr import Domain, event, obj
from .ocel import Ocel, Summary

__all__ = ["Domain", "Ocel", "Summary", "event", "obj"]
