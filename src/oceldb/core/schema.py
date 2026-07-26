"""Reserved OCEL column names and core table schemas.

These are the stable contract for the native storage format — see
``docs/storage-format.md``.
"""

from typing import Final

import polars as pl

OCEL_ID: Final = "ocel_id"
OCEL_TYPE: Final = "ocel_type"
OCEL_TIME: Final = "ocel_time"
OCEL_CHANGED_FIELD: Final = "ocel_changed_field"
OCEL_IS_INITIAL: Final = "ocel_is_initial"
OCEL_EVENT_ID: Final = "ocel_event_id"
OCEL_EVENT_TYPE: Final = "ocel_event_type"
OCEL_OBJECT_ID: Final = "ocel_object_id"
OCEL_OBJECT_TYPE: Final = "ocel_object_type"
OCEL_SOURCE_ID: Final = "ocel_source_id"
OCEL_SOURCE_TYPE: Final = "ocel_source_type"
OCEL_TARGET_ID: Final = "ocel_target_id"
OCEL_TARGET_TYPE: Final = "ocel_target_type"
OCEL_QUALIFIER: Final = "ocel_qualifier"

RESERVED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        OCEL_ID,
        OCEL_TYPE,
        OCEL_TIME,
        OCEL_CHANGED_FIELD,
        OCEL_IS_INITIAL,
        OCEL_EVENT_ID,
        OCEL_EVENT_TYPE,
        OCEL_OBJECT_ID,
        OCEL_OBJECT_TYPE,
        OCEL_SOURCE_ID,
        OCEL_SOURCE_TYPE,
        OCEL_TARGET_ID,
        OCEL_TARGET_TYPE,
        OCEL_QUALIFIER,
    }
)


EVENT_SCHEMA: Final[dict[str, pl.DataType]] = {
    OCEL_ID: pl.String(),
    OCEL_TIME: pl.Datetime("us", "UTC"),
    OCEL_TYPE: pl.String(),
}
OBJECT_SCHEMA: Final[dict[str, pl.DataType]] = {
    OCEL_ID: pl.String(),
    OCEL_TYPE: pl.String(),
}
CHANGE_SCHEMA: Final[dict[str, pl.DataType]] = {
    OCEL_ID: pl.String(),
    OCEL_TIME: pl.Datetime("us", "UTC"),
    OCEL_CHANGED_FIELD: pl.String(),
    OCEL_IS_INITIAL: pl.Boolean(),
    OCEL_TYPE: pl.String(),
}
E2O_SCHEMA: Final[dict[str, pl.DataType]] = {
    name: pl.String()
    for name in (
        OCEL_EVENT_ID,
        OCEL_EVENT_TYPE,
        OCEL_OBJECT_ID,
        OCEL_OBJECT_TYPE,
        OCEL_QUALIFIER,
    )
}
O2O_SCHEMA: Final[dict[str, pl.DataType]] = {
    name: pl.String()
    for name in (
        OCEL_SOURCE_ID,
        OCEL_SOURCE_TYPE,
        OCEL_TARGET_ID,
        OCEL_TARGET_TYPE,
        OCEL_QUALIFIER,
    )
}
