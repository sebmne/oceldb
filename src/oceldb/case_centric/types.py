"""Public semantic type aliases."""

from typing import TypeAlias

from oceldb.expr import Table

CaseCentricEventLog: TypeAlias = Table
"""A lazy case-centric event log produced by :meth:`oceldb.OCEL.flatten`.

Case-centric event logs use ``case:concept:name`` as case id,
``concept:name`` as activity, ``time:timestamp`` as timestamp, and
``ocel_event_id`` as stable event id.
"""
