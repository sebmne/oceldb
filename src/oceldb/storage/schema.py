"""Reserved column names and mandatory table-schema descriptors for oceldb v1."""

OCEL_ID = "ocel_id"
OCEL_TYPE = "ocel_type"
OCEL_TIME = "ocel_time"
OCEL_CHANGED_FIELD = "ocel_changed_field"

OCEL_EVENT_ID = "ocel_event_id"
OCEL_EVENT_TYPE = "ocel_event_type"
OCEL_OBJECT_ID = "ocel_object_id"
OCEL_OBJECT_TYPE = "ocel_object_type"
OCEL_QUALIFIER = "ocel_qualifier"

OCEL_SOURCE_ID = "ocel_source_id"
OCEL_SOURCE_TYPE = "ocel_source_type"
OCEL_TARGET_ID = "ocel_target_id"
OCEL_TARGET_TYPE = "ocel_target_type"

EVENT_TABLE_REQUIRED: dict[str, str] = {
    OCEL_ID: "VARCHAR",
    OCEL_TYPE: "VARCHAR",
    OCEL_TIME: "TIMESTAMP",
}

OBJECT_TABLE_REQUIRED: dict[str, str] = {
    OCEL_ID: "VARCHAR",
    OCEL_TYPE: "VARCHAR",
}

OBJECT_CHANGES_REQUIRED: dict[str, str] = {
    OCEL_ID: "VARCHAR",
    OCEL_TIME: "TIMESTAMP",
    OCEL_CHANGED_FIELD: "VARCHAR",  # nullable in practice; VARCHAR here for reference
}

EVENT_OBJECT_REQUIRED: dict[str, str] = {
    OCEL_EVENT_ID: "VARCHAR",
    OCEL_EVENT_TYPE: "VARCHAR",
    OCEL_OBJECT_ID: "VARCHAR",
    OCEL_OBJECT_TYPE: "VARCHAR",
    OCEL_QUALIFIER: "VARCHAR",  # nullable in practice
}

OBJECT_OBJECT_REQUIRED: dict[str, str] = {
    OCEL_SOURCE_ID: "VARCHAR",
    OCEL_SOURCE_TYPE: "VARCHAR",
    OCEL_TARGET_ID: "VARCHAR",
    OCEL_TARGET_TYPE: "VARCHAR",
    OCEL_QUALIFIER: "VARCHAR",  # nullable in practice
}
