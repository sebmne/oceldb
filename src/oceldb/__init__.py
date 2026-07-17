"""Polars-backed access to OCEL 2.0 event logs.

The public surface is the ``OCEL`` class itself, ``oceldb.operations`` for
filters and transformations, and ``oceldb.core.schema`` for the reserved
column-name constants.
"""

from oceldb.ocel import OCEL

__all__ = ["OCEL"]
