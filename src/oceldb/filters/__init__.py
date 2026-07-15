"""Sub-log filters that take an ``OCEL`` and return a pruned ``OCEL``.

Every filter removes rows and then prunes the connected core: dropped events,
dropped objects, and relations pointing to removed rows are discarded together.
Each filter can be called directly (``filter_events_by_type(ocel, ...)``) or as
a pipe step (``ocel >> filter_events_by_type(...)``).
"""

from oceldb.filters.events import (
    filter_events_by_attribute,
    filter_events_by_id,
    filter_events_by_object_count,
    filter_events_by_time,
    filter_events_by_type,
    sample_events,
)
from oceldb.filters.objects import (
    filter_objects_by_attribute,
    filter_objects_by_event_count,
    filter_objects_by_id,
    filter_objects_by_o2o_count,
    filter_objects_by_type,
    sample_objects,
)
from oceldb.filters.relations import (
    filter_e2o_by_qualifier,
    filter_o2o_by_qualifier,
)

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
    "sample_events",
    "sample_objects",
]
