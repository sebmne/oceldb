"""Transformations that derive tables or sub-logs from an ``OCEL``.

``flatten`` and ``case_table`` return Polars lazy frames; ``view``, ``project``,
and ``rename_types`` return derived ``OCEL`` logs; ``collect`` is a terminal pipe
step that materializes a lazy frame. Each can be called directly or as a ``>>``
step.
"""

from oceldb.transformations.case_table import case_table
from oceldb.transformations.flatten import flatten
from oceldb.transformations.project import project
from oceldb.transformations.rename import rename_types
from oceldb.transformations.view import view
from oceldb.transformations.collect import collect

__all__ = ["case_table", "flatten", "project", "rename_types", "view", "collect"]
