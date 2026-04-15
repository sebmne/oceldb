from typing import Any


class OCEL:
    event_id_column: str
    object_id_column: str
    object_type_column: str
    event_activity: str
    event_timestamp: str
    qualifier: str
    changed_field: str
    events: Any
    objects: Any
    relations: Any
    globals: dict[str, object]
    parameters: dict[str, object]
    o2o: Any | None
    e2e: Any | None
    object_changes: Any | None

    def __init__(
        self,
        events: Any = ...,
        objects: Any = ...,
        relations: Any = ...,
        globals: dict[str, object] | None = ...,
        parameters: dict[str, object] | None = ...,
        o2o: Any | None = ...,
        e2e: Any | None = ...,
        object_changes: Any | None = ...,
    ) -> None: ...

    def get_extended_table(self, ot_prefix: str = ...) -> Any: ...

    def get_summary(self) -> object: ...

    def is_ocel20(self) -> bool: ...
