"""Logical invariants shared by every OCEL persistence boundary."""

from collections.abc import Mapping

import polars as pl

from oceldb import schema as s
from oceldb.core.dataset import OCELDataset
from oceldb.errors import OCELValidationError
from oceldb.schema._layout import (
    E2O_SCHEMA,
    EVENTS_SCHEMA,
    OBJECT_CHANGES_SCHEMA,
    OBJECTS_SCHEMA,
    O2O_SCHEMA,
)


def validate_dataset(dataset: OCELDataset) -> None:
    """Raise when *dataset* violates canonical logical OCEL invariants.

    Validation executes small lazy queries over identifiers, types, and
    relations. It does not materialize event or object attribute columns.
    """
    issues = dataset_issues(dataset)
    if issues:
        raise OCELValidationError("Invalid OCEL dataset:\n- " + "\n- ".join(issues))


def validate_storage_input(dataset: OCELDataset) -> None:
    """Validate resolvable logical schemas before physical normalization.

    Full identity and relationship validation runs against the staged native
    files after writing.
    """
    tables = dataset.tables
    frames = {
        "events": tables.events.all(),
        "objects": tables.objects.all(),
        "object_changes": tables.object_changes.all(),
        "event_object": tables.event_object.all(),
        "object_object": tables.object_object.all(),
    }
    required = {
        "events": EVENTS_SCHEMA,
        "objects": OBJECTS_SCHEMA,
        "object_changes": OBJECT_CHANGES_SCHEMA,
        "event_object": E2O_SCHEMA,
        "object_object": O2O_SCHEMA,
    }
    issues = [
        issue
        for name, frame in frames.items()
        for issue in _schema_issues(name, frame, required[name])
    ]
    if issues:
        raise OCELValidationError("Invalid OCEL dataset:\n- " + "\n- ".join(issues))


def dataset_issues(dataset: OCELDataset) -> tuple[str, ...]:
    """Return logical invariant violations found in *dataset*."""
    source_tables = dataset.tables
    tables: dict[str, pl.LazyFrame] = {
        "events": source_tables.events.all(),
        "objects": source_tables.objects.all(),
        "object_changes": source_tables.object_changes.all(),
        "event_object": source_tables.event_object.all(),
        "object_object": source_tables.object_object.all(),
    }
    required_schemas = {
        "events": EVENTS_SCHEMA,
        "objects": OBJECTS_SCHEMA,
        "object_changes": OBJECT_CHANGES_SCHEMA,
        "event_object": E2O_SCHEMA,
        "object_object": O2O_SCHEMA,
    }
    issues: list[str] = []
    for name, frame in tables.items():
        issues.extend(_schema_issues(name, frame, required_schemas[name]))
    if issues:
        return tuple(issues)
    # Run the largest narrow hash operations before wide attribute aggregates
    # so Polars can reuse its allocator without stacking working sets at peak.
    event_duplicate_issues = _duplicate_issues(tables["events"], "event")
    object_duplicate_issues = _duplicate_issues(tables["objects"], "object")
    reference_issues = _reference_table_issues(tables)

    event_required = (
        (s.OCEL_ID, "id"),
        (s.OCEL_TYPE, "type"),
        (s.OCEL_TIME, "timestamp"),
    )
    object_required = ((s.OCEL_ID, "id"), (s.OCEL_TYPE, "type"))
    change_required = (
        (s.OCEL_ID, "object id"),
        (s.OCEL_TYPE, "object type"),
        (s.OCEL_TIME, "timestamp"),
    )
    e2o_required = (
        (s.OCEL_EVENT_ID, "event id"),
        (s.OCEL_EVENT_TYPE, "event type"),
        (s.OCEL_OBJECT_ID, "object id"),
        (s.OCEL_OBJECT_TYPE, "object type"),
        (s.OCEL_QUALIFIER, "qualifier"),
    )
    o2o_required = (
        (s.OCEL_SOURCE_ID, "source id"),
        (s.OCEL_SOURCE_TYPE, "source type"),
        (s.OCEL_TARGET_ID, "target id"),
        (s.OCEL_TARGET_TYPE, "target type"),
        (s.OCEL_QUALIFIER, "qualifier"),
    )
    event_checks = _table_checks(tables["events"], required=event_required)
    object_checks = _table_checks(tables["objects"], required=object_required)
    change_checks = _table_checks(
        tables["object_changes"],
        required=change_required,
        check_changed_field=True,
    )
    e2o_checks = _table_checks(tables["event_object"], required=e2o_required)
    o2o_checks = _table_checks(tables["object_object"], required=o2o_required)

    issues.extend(_required_issues(event_checks, "event", event_required[:2]))
    issues.extend(event_duplicate_issues)
    issues.extend(_required_issues(object_checks, "object", object_required))
    issues.extend(object_duplicate_issues)
    issues.extend(_required_issues(event_checks, "event", event_required[2:]))
    issues.extend(_required_issues(change_checks, "object change", change_required))
    issues.extend(_required_issues(e2o_checks, "E2O relation", e2o_required))
    issues.extend(_required_issues(o2o_checks, "O2O relation", o2o_required))
    issues.extend(_empty_changed_field_issues(change_checks))

    issues.extend(reference_issues)
    return tuple(issues)


def _reference_table_issues(tables: Mapping[str, pl.LazyFrame]) -> list[str]:
    references = (
        (
            "object_changes",
            "objects",
            s.OCEL_ID,
            s.OCEL_TYPE,
            s.OCEL_ID,
            s.OCEL_TYPE,
            "object change",
        ),
        (
            "event_object",
            "events",
            s.OCEL_EVENT_ID,
            s.OCEL_EVENT_TYPE,
            s.OCEL_ID,
            s.OCEL_TYPE,
            "E2O event",
        ),
        (
            "event_object",
            "objects",
            s.OCEL_OBJECT_ID,
            s.OCEL_OBJECT_TYPE,
            s.OCEL_ID,
            s.OCEL_TYPE,
            "E2O object",
        ),
        (
            "object_object",
            "objects",
            s.OCEL_SOURCE_ID,
            s.OCEL_SOURCE_TYPE,
            s.OCEL_ID,
            s.OCEL_TYPE,
            "O2O source",
        ),
        (
            "object_object",
            "objects",
            s.OCEL_TARGET_ID,
            s.OCEL_TARGET_TYPE,
            s.OCEL_ID,
            s.OCEL_TYPE,
            "O2O target",
        ),
    )
    issues: list[str] = []
    for (
        source,
        target,
        source_id,
        source_type,
        target_id,
        target_type,
        context,
    ) in references:
        issues.extend(
            _reference_issues(
                tables[source],
                tables[target],
                reference_id=source_id,
                reference_type=source_type,
                target_id=target_id,
                target_type=target_type,
                context=context,
            )
        )
    return issues


def _schema_issues(
    name: str,
    frame: pl.LazyFrame,
    required: Mapping[str, pl.DataType],
) -> list[str]:
    try:
        actual = frame.collect_schema()
    except Exception as exc:
        return [f"{name} schema cannot be resolved: {exc}"]
    missing = [column for column in required if column not in actual]
    issues = [f"{name} is missing required columns: {missing}"] if missing else []
    incompatible = [
        f"{column} ({actual[column]!r}, expected {dtype!r})"
        for column, dtype in required.items()
        if column in actual and actual[column] != dtype
    ]
    if incompatible:
        issues.append(f"{name} has incompatible columns: {', '.join(incompatible)}")
    return issues


def _table_checks(
    frame: pl.LazyFrame,
    *,
    required: tuple[tuple[str, str], ...],
    check_changed_field: bool = False,
) -> Mapping[str, object]:
    """Evaluate all scalar checks for one logical table in one scan."""
    expressions = [
        (
            pl.col(column).is_null()
            | (pl.col(column).cast(pl.String).str.len_chars() == 0)
        )
        .any()
        .alias(_empty_alias(column))
        for column, _ in required
    ]
    if check_changed_field:
        empty = pl.col(s.OCEL_CHANGED_FIELD).is_not_null() & (
            pl.col(s.OCEL_CHANGED_FIELD).str.len_chars() == 0
        )
        expressions.append(
            pl.col(s.OCEL_ID).filter(empty).first().alias("_empty_changed_field")
        )
    result = frame.select(*expressions).collect(engine="streaming")
    return dict(result.row(0, named=True))


def _required_issues(
    checks: Mapping[str, object],
    context: str,
    columns: tuple[tuple[str, str], ...],
) -> list[str]:
    return [
        f"{context} has an empty {label}"
        for column, label in columns
        if checks.get(_empty_alias(column)) is True
    ]


def _duplicate_issues(frame: pl.LazyFrame, kind: str) -> list[str]:
    duplicate = _first_row(
        frame.group_by(s.OCEL_ID)
        .len()
        .filter(pl.col("len") > 1)
        .select(s.OCEL_ID, "len")
    )
    if duplicate is None:
        return []
    return [
        f"duplicate {kind} id {duplicate[s.OCEL_ID]!r} appears {duplicate['len']} times"
    ]


def _empty_changed_field_issues(checks: Mapping[str, object]) -> list[str]:
    value = checks.get("_empty_changed_field")
    if value is None:
        return []
    return [f"object change for {value!r} has an empty changed field"]


def _empty_alias(column: str) -> str:
    return f"_empty_{column}"


def _reference_issues(
    references: pl.LazyFrame,
    targets: pl.LazyFrame,
    *,
    reference_id: str,
    reference_type: str,
    target_id: str,
    target_type: str,
    context: str,
) -> list[str]:
    known = targets.select(
        pl.col(target_id).alias(reference_id),
        pl.col(target_type).alias(reference_type),
    )
    invalid = (
        references.select(reference_id, reference_type)
        .join(known, on=[reference_id, reference_type], how="anti")
        .limit(1)
        .collect(engine="streaming")
    )
    if invalid.height == 0:
        return []

    sample = dict(invalid.row(0, named=True))
    expected = (
        targets.filter(pl.col(target_id) == sample[reference_id])
        .select(target_type)
        .limit(1)
        .collect(engine="streaming")
    )
    if expected.height == 0:
        return [f"{context} references unknown id {sample[reference_id]!r}"]
    return [
        f"{context} {sample[reference_id]!r} declares type "
        f"{sample[reference_type]!r}; expected {expected.item()!r}"
    ]


def _first_row(frame: pl.LazyFrame) -> dict[str, object] | None:
    result = frame.limit(1).collect(engine="streaming")
    if result.height == 0:
        return None
    return dict(result.row(0, named=True))
