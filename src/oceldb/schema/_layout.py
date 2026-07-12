"""Reserved columns and canonical metadata for the five logical OCEL tables."""

import polars as pl

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

EVENT_CORE = (OCEL_ID, OCEL_TIME)
OBJECT_CORE = (OCEL_ID,)
CHANGE_CORE = (OCEL_ID, OCEL_TIME, OCEL_CHANGED_FIELD)

E2O_COLUMNS = (
    OCEL_EVENT_ID,
    OCEL_EVENT_TYPE,
    OCEL_OBJECT_ID,
    OCEL_OBJECT_TYPE,
    OCEL_QUALIFIER,
)
O2O_COLUMNS = (
    OCEL_SOURCE_ID,
    OCEL_SOURCE_TYPE,
    OCEL_TARGET_ID,
    OCEL_TARGET_TYPE,
    OCEL_QUALIFIER,
)

EVENTS_SCHEMA: dict[str, pl.DataType] = {
    OCEL_ID: pl.String(),
    OCEL_TIME: pl.Datetime("us", "UTC"),
    OCEL_TYPE: pl.String(),
}
OBJECTS_SCHEMA: dict[str, pl.DataType] = {
    OCEL_ID: pl.String(),
    OCEL_TYPE: pl.String(),
}
OBJECT_CHANGES_SCHEMA: dict[str, pl.DataType] = {
    OCEL_ID: pl.String(),
    OCEL_TIME: pl.Datetime("us", "UTC"),
    OCEL_TYPE: pl.String(),
    OCEL_CHANGED_FIELD: pl.String(),
}
E2O_SCHEMA: dict[str, pl.DataType] = {name: pl.String() for name in E2O_COLUMNS}
O2O_SCHEMA: dict[str, pl.DataType] = {name: pl.String() for name in O2O_COLUMNS}
