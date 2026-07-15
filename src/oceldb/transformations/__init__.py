"""Transformations that derive tables or sub-logs from an ``OCEL``.

``flatten`` returns a Polars lazy frame; ``view``, ``project``, and
``rename_types`` return derived ``OCEL`` logs. Each can be called directly or
as a ``>>`` step.
"""

from oceldb.transformations.flatten import flatten
from oceldb.transformations.project import project
from oceldb.transformations.rename import rename_types
from oceldb.transformations.view import view

__all__ = ["flatten", "project", "rename_types", "view"]
