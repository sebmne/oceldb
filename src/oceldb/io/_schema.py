"""Schema inference and materialization shared by format writers."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping

import polars as pl

from oceldb import schema as s
from oceldb.io.errors import ValidationMode, issue
from oceldb.schema import AttributeType, OCELSchema
from oceldb.ocel import OCEL

_EVENT_FIXED = {s.OCEL_ID, s.OCEL_TIME, s.OCEL_TYPE}
_CHANGE_FIXED = {s.OCEL_ID, s.OCEL_TIME, s.OCEL_TYPE, s.OCEL_CHANGED_FIELD}


@dataclass(frozen=True)
class MaterializedOCEL:
    events: pl.DataFrame
    objects: pl.DataFrame
    object_changes: pl.DataFrame
    e2o: pl.DataFrame
    o2o: pl.DataFrame
    schema: OCELSchema


def materialize(ocel: OCEL) -> MaterializedOCEL:
    """Collect an OCEL once and resolve its complete exchange schema."""
    events = ocel.events().collect()
    objects = ocel.objects().collect()
    changes = ocel.object_changes().collect()
    declared = ocel.schema or OCELSchema.empty()
    schema = OCELSchema(
        event_types=_infer_types(events, _EVENT_FIXED, declared.event_types),
        object_types=_infer_object_types(objects, changes, declared.object_types),
    )
    return MaterializedOCEL(
        events=events,
        objects=objects,
        object_changes=changes,
        e2o=ocel.event_object().collect(),
        o2o=ocel.object_object().collect(),
        schema=schema,
    )


def validate_for_exchange(data: MaterializedOCEL, validation: ValidationMode) -> None:
    """Validate core identities and denormalized relationship metadata."""
    event_types: dict[str, str] = {}
    for row in data.events.iter_rows(named=True):
        event_id = _identifier(row.get(s.OCEL_ID), validation, "event id")
        type_name = _identifier(
            row.get(s.OCEL_TYPE), validation, f"event {event_id!r} type"
        )
        if event_id in event_types:
            issue(validation, f"Duplicate event id {event_id!r}.")
        event_types[event_id] = type_name
        if type_name not in data.schema.event_types:
            issue(validation, f"Event {event_id!r} uses undeclared type {type_name!r}.")
        if row.get(s.OCEL_TIME) is None:
            issue(validation, f"Event {event_id!r} has no timestamp.")

    object_types: dict[str, str] = {}
    for row in data.objects.iter_rows(named=True):
        object_id = _identifier(row.get(s.OCEL_ID), validation, "object id")
        type_name = _identifier(
            row.get(s.OCEL_TYPE), validation, f"object {object_id!r} type"
        )
        if object_id in object_types:
            issue(validation, f"Duplicate object id {object_id!r}.")
        object_types[object_id] = type_name
        if type_name not in data.schema.object_types:
            issue(
                validation, f"Object {object_id!r} uses undeclared type {type_name!r}."
            )

    for row in data.e2o.iter_rows(named=True):
        event_id = str(row.get(s.OCEL_EVENT_ID, ""))
        object_id = str(row.get(s.OCEL_OBJECT_ID, ""))
        _check_reference(
            event_id,
            row.get(s.OCEL_EVENT_TYPE),
            event_types,
            validation,
            "E2O event",
        )
        _check_reference(
            object_id,
            row.get(s.OCEL_OBJECT_TYPE),
            object_types,
            validation,
            "E2O object",
        )

    for row in data.o2o.iter_rows(named=True):
        _check_reference(
            str(row.get(s.OCEL_SOURCE_ID, "")),
            row.get(s.OCEL_SOURCE_TYPE),
            object_types,
            validation,
            "O2O source",
        )
        _check_reference(
            str(row.get(s.OCEL_TARGET_ID, "")),
            row.get(s.OCEL_TARGET_TYPE),
            object_types,
            validation,
            "O2O target",
        )

    for row in data.object_changes.iter_rows(named=True):
        object_id = str(row.get(s.OCEL_ID, ""))
        _check_reference(
            object_id,
            row.get(s.OCEL_TYPE),
            object_types,
            validation,
            "object change",
        )
        if row.get(s.OCEL_TIME) is None:
            issue(validation, f"Object change for {object_id!r} has no timestamp.")


def _identifier(value: object, validation: ValidationMode, context: str) -> str:
    if isinstance(value, str) and value:
        return value
    issue(validation, f"{context} must be a non-empty string, got {value!r}.")
    return "" if value is None else str(value)


def _check_reference(
    identifier: str,
    denormalized_type: object,
    known: dict[str, str],
    validation: ValidationMode,
    context: str,
) -> None:
    actual_type = known.get(identifier)
    if actual_type is None:
        issue(validation, f"{context} references unknown id {identifier!r}.")
    elif denormalized_type != actual_type:
        issue(
            validation,
            f"{context} {identifier!r} declares type {denormalized_type!r}; "
            f"expected {actual_type!r}.",
        )


def _infer_object_types(
    objects: pl.DataFrame,
    changes: pl.DataFrame,
    declared: Mapping[str, Mapping[str, AttributeType]],
) -> dict[str, dict[str, AttributeType]]:
    result = _copy_declared(declared)
    if objects.height:
        for value in objects.get_column(s.OCEL_TYPE).drop_nulls().unique().to_list():
            result.setdefault(str(value), {})
    inferred = _infer_types(changes, _CHANGE_FIXED, result)
    return inferred


def _infer_types(
    frame: pl.DataFrame,
    fixed: set[str],
    declared: Mapping[str, Mapping[str, AttributeType]],
) -> dict[str, dict[str, AttributeType]]:
    result = _copy_declared(declared)
    if s.OCEL_TYPE not in frame.columns:
        return result
    type_values = frame.get_column(s.OCEL_TYPE).drop_nulls().unique().to_list()
    attributes = [name for name in frame.columns if name not in fixed]
    for value in type_values:
        type_name = str(value)
        current = result.setdefault(type_name, {})
        sub = frame.filter(pl.col(s.OCEL_TYPE) == value)
        for name in attributes:
            if name in current:
                continue
            column = sub.get_column(name)
            if column.null_count() < column.len():
                current[name] = AttributeType.from_polars(column.dtype)
    return result


def _copy_declared(
    values: Mapping[str, Mapping[str, AttributeType]],
) -> dict[str, dict[str, AttributeType]]:
    return {name: dict(attributes) for name, attributes in values.items()}
