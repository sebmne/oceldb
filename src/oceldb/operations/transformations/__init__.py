"""Transformations that derive tables or sub-logs from an ``OCEL``.

``flatten`` returns a Polars lazy frame; ``view``, ``project``, and
``rename_types`` return derived ``OCEL`` logs. Each can be called directly or
as a ``>>`` step.
"""

from oceldb.operations.transformations.flatten import flatten
from oceldb.operations.transformations.project import project
from oceldb.operations.transformations.rename import rename_types
from oceldb.operations.transformations.view import view

__all__ = ["flatten", "project", "rename_types", "view"]
