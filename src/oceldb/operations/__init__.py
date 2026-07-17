"""Public filter and transformation operations on ``OCEL`` logs.

Each operation is a plain function taking an ``OCEL`` (or, for filters,
also predicate/scope arguments) and returning a derived ``OCEL`` or
``polars.LazyFrame``. Every operation can be called directly
(``filter_events_by_type(ocel, "Load")``) or curried into a ``>>`` pipeline
step (``ocel >> filter_events_by_type("Load")``); see ``oceldb.operations.step``.
"""

from oceldb.operations.filters import (
    filter_e2o_by_qualifier,
    filter_events_by_attribute,
    filter_events_by_id,
    filter_events_by_object_count,
    filter_events_by_time,
    filter_events_by_type,
    filter_o2o_by_qualifier,
    filter_objects_by_attribute,
    filter_objects_by_event_count,
    filter_objects_by_id,
    filter_objects_by_o2o_count,
    filter_objects_by_type,
    sample_events,
    sample_objects,
)
from oceldb.operations.transformations import flatten, project, rename_types, view

__all__ = [
    "filter_e2o_by_qualifier",
    "filter_events_by_attribute",
    "filter_events_by_id",
    "filter_events_by_object_count",
    "filter_events_by_time",
    "filter_events_by_type",
    "filter_o2o_by_qualifier",
    "filter_objects_by_attribute",
    "filter_objects_by_event_count",
    "filter_objects_by_id",
    "filter_objects_by_o2o_count",
    "filter_objects_by_type",
    "flatten",
    "project",
    "rename_types",
    "sample_events",
    "sample_objects",
    "view",
]
