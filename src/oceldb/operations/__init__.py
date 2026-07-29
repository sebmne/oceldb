"""Lazy filters and transformations for :class:`oceldb.OCEL`.

Every operation supports direct and pipeline use::

    filtered = filter_events_by_type(ocel, "Load")
    filtered = ocel >> filter_events_by_type("Load")

Filters return a new immutable OCEL. Event filters preserve selected
relationless events and induce object membership through surviving E2O
relations. Object filters apply the symmetric rule. Relation filters induce
both endpoint sets. O2O-only filters never change event or object membership.

Required selectors reject empty input. Optional type scopes use ``None`` for
all types and reject empty iterables. Predicate nulls mean "no match"; include
mode drops them and exclude mode retains them.
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
)
from oceldb.operations.transformations import flatten, project, view

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
    "view",
]
