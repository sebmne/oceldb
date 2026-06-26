"""Reserved column names used by oceldb logical tables.

Use these constants when building frames for :class:`oceldb.OCEL` or composing
Polars expressions against oceldb accessors. The string values are the stable
schema contract for the native storage layout and public dataframe API.
"""

OCEL_ID = "ocel_id"
OCEL_TYPE = "ocel_type"
OCEL_TIME = "ocel_time"
OCEL_CHANGED_FIELD = "ocel_changed_field"

OCEL_EVENT_ID = "ocel_event_id"
OCEL_EVENT_TYPE = "ocel_event_type"
OCEL_OBJECT_ID = "ocel_object_id"
OCEL_OBJECT_TYPE = "ocel_object_type"

OCEL_SOURCE_ID = "ocel_source_id"
OCEL_SOURCE_TYPE = "ocel_source_type"
OCEL_TARGET_ID = "ocel_target_id"
OCEL_TARGET_TYPE = "ocel_target_type"

OCEL_QUALIFIER = "ocel_qualifier"

__all__ = [
    "OCEL_ID",
    "OCEL_TYPE",
    "OCEL_TIME",
    "OCEL_CHANGED_FIELD",
    "OCEL_EVENT_ID",
    "OCEL_EVENT_TYPE",
    "OCEL_OBJECT_ID",
    "OCEL_OBJECT_TYPE",
    "OCEL_SOURCE_ID",
    "OCEL_SOURCE_TYPE",
    "OCEL_TARGET_ID",
    "OCEL_TARGET_TYPE",
    "OCEL_QUALIFIER",
]
