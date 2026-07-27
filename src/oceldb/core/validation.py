"""Explicit logical validation for the five canonical OCEL tables."""

from collections.abc import Mapping

import polars as pl

from oceldb.core import schema as s
from oceldb.core.schema import (
    CHANGE_SCHEMA,
    E2O_SCHEMA,
    EVENT_SCHEMA,
    OBJECT_SCHEMA,
    O2O_SCHEMA,
)
from oceldb.errors import OCELValidationError


def validate_tables(
    *,
    events: pl.LazyFrame,
    objects: pl.LazyFrame,
    object_changes: pl.LazyFrame,
    e2o: pl.LazyFrame,
    o2o: pl.LazyFrame,
) -> None:
    """Raise if the tables violate the stable logical OCEL contract."""
    validate_schemas(
        events=events,
        objects=objects,
        object_changes=object_changes,
        e2o=e2o,
        o2o=o2o,
    )
    issues: list[str] = []
    issues.extend(_required_value_issues(events, "event", EVENT_SCHEMA))
    issues.extend(_required_value_issues(objects, "object", OBJECT_SCHEMA))
    issues.extend(
        _required_value_issues(
            object_changes,
            "object change",
            {
                s.OCEL_ID: CHANGE_SCHEMA[s.OCEL_ID],
                s.OCEL_TIME: CHANGE_SCHEMA[s.OCEL_TIME],
                s.OCEL_TYPE: CHANGE_SCHEMA[s.OCEL_TYPE],
                s.OCEL_IS_INITIAL: CHANGE_SCHEMA[s.OCEL_IS_INITIAL],
            },
        )
    )
    issues.extend(
        _required_value_issues(
            e2o,
            "E2O relation",
            {
                name: dtype
                for name, dtype in E2O_SCHEMA.items()
                if name != s.OCEL_QUALIFIER
            },
        )
    )
    issues.extend(
        _required_value_issues(
            o2o,
            "O2O relation",
            {
                name: dtype
                for name, dtype in O2O_SCHEMA.items()
                if name != s.OCEL_QUALIFIER
            },
        )
    )
    issues.extend(_duplicate_id_issues(events, "event"))
    issues.extend(_duplicate_id_issues(objects, "object"))
    issues.extend(
        _reference_issues(
            object_changes,
            objects,
            s.OCEL_ID,
            s.OCEL_TYPE,
            s.OCEL_ID,
            s.OCEL_TYPE,
            "object change",
        )
    )
    issues.extend(
        _reference_issues(
            e2o,
            events,
            s.OCEL_EVENT_ID,
            s.OCEL_EVENT_TYPE,
            s.OCEL_ID,
            s.OCEL_TYPE,
            "E2O event",
        )
    )
    issues.extend(
        _reference_issues(
            e2o,
            objects,
            s.OCEL_OBJECT_ID,
            s.OCEL_OBJECT_TYPE,
            s.OCEL_ID,
            s.OCEL_TYPE,
            "E2O object",
        )
    )
    issues.extend(
        _reference_issues(
            o2o,
            objects,
            s.OCEL_SOURCE_ID,
            s.OCEL_SOURCE_TYPE,
            s.OCEL_ID,
            s.OCEL_TYPE,
            "O2O source",
        )
    )
    issues.extend(
        _reference_issues(
            o2o,
            objects,
            s.OCEL_TARGET_ID,
            s.OCEL_TARGET_TYPE,
            s.OCEL_ID,
            s.OCEL_TYPE,
            "O2O target",
        )
    )
    issues.extend(_change_issues(object_changes))
    if issues:
        _raise(issues)


def validate_schemas(
    *,
    events: pl.LazyFrame,
    objects: pl.LazyFrame,
    object_changes: pl.LazyFrame,
    e2o: pl.LazyFrame,
    o2o: pl.LazyFrame,
) -> None:
    """Validate the five table schemas without reading row data."""
    validate_table_schemas(
        {
            "events": events,
            "objects": objects,
            "object_changes": object_changes,
            "e2o": e2o,
            "o2o": o2o,
        }
    )


def validate_table_schemas(tables: Mapping[str, pl.LazyFrame]) -> None:
    """Validate named canonical table schemas without reading row data."""
    schemas = {
        "events": EVENT_SCHEMA,
        "objects": OBJECT_SCHEMA,
        "object_changes": CHANGE_SCHEMA,
        "e2o": E2O_SCHEMA,
        "o2o": O2O_SCHEMA,
    }
    unknown = sorted(set(tables) - set(schemas))
    if unknown:
        raise ValueError(f"Unknown OCEL tables: {unknown}")
    attribute_tables = {"events", "object_changes"}
    issues = [
        issue
        for name, frame in tables.items()
        for issue in _schema_issues(
            name,
            frame,
            schemas[name],
            allow_attributes=name in attribute_tables,
        )
    ]
    if issues:
        _raise(issues)


def _schema_issues(
    name: str,
    frame: pl.LazyFrame,
    required: Mapping[str, pl.DataType],
    *,
    allow_attributes: bool,
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
    extra = set(actual.names()) - set(required)
    if not allow_attributes and extra:
        issues.append(f"{name} has unexpected columns: {sorted(extra)}")
    reserved_attributes = sorted(extra & s.RESERVED_COLUMNS)
    if allow_attributes and reserved_attributes:
        issues.append(f"{name} uses reserved attribute columns: {reserved_attributes}")
    return issues


def _required_value_issues(
    frame: pl.LazyFrame,
    context: str,
    required: Mapping[str, pl.DataType],
) -> list[str]:
    expressions: list[pl.Expr] = []
    for column, dtype in required.items():
        invalid = pl.col(column).is_null()
        if dtype == pl.String:
            invalid |= pl.col(column).str.len_chars() == 0
        expressions.append(invalid.any().alias(column))
    checks = frame.select(expressions).collect(engine="streaming").row(0, named=True)
    return [
        f"{context} has a null or empty {column}"
        for column in required
        if checks[column]
    ]


def _duplicate_id_issues(frame: pl.LazyFrame, context: str) -> list[str]:
    duplicate = _first_row(
        frame.group_by(s.OCEL_ID)
        .len()
        .filter(pl.col("len") > 1)
        .select(s.OCEL_ID, "len")
    )
    if duplicate is None:
        return []
    return [
        f"duplicate {context} id {duplicate[s.OCEL_ID]!r} appears "
        f"{duplicate['len']} times"
    ]


def _reference_issues(
    references: pl.LazyFrame,
    targets: pl.LazyFrame,
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
    invalid = _first_row(
        references.select(reference_id, reference_type).join(
            known, on=[reference_id, reference_type], how="anti"
        )
    )
    if invalid is None:
        return []
    expected = (
        targets.filter(pl.col(target_id) == invalid[reference_id])
        .select(target_type)
        .limit(1)
        .collect(engine="streaming")
    )
    if expected.is_empty():
        return [f"{context} references unknown id {invalid[reference_id]!r}"]
    return [
        f"{context} {invalid[reference_id]!r} declares type "
        f"{invalid[reference_type]!r}; expected {expected.item()!r}"
    ]


def _change_issues(changes: pl.LazyFrame) -> list[str]:
    issues: list[str] = []
    duplicate_initial = _first_row(
        changes.filter(pl.col(s.OCEL_IS_INITIAL))
        .group_by(s.OCEL_ID)
        .len()
        .filter(pl.col("len") > 1)
    )
    if duplicate_initial is not None:
        issues.append(
            f"object {duplicate_initial[s.OCEL_ID]!r} has multiple initial states"
        )

    invalid_field = _first_row(
        changes.filter(
            (pl.col(s.OCEL_IS_INITIAL) & pl.col(s.OCEL_CHANGED_FIELD).is_not_null())
            | (
                ~pl.col(s.OCEL_IS_INITIAL)
                & (
                    pl.col(s.OCEL_CHANGED_FIELD).is_null()
                    | (pl.col(s.OCEL_CHANGED_FIELD).str.len_chars() == 0)
                )
            )
        ).select(s.OCEL_ID)
    )
    if invalid_field is not None:
        issues.append(
            f"object change for {invalid_field[s.OCEL_ID]!r} has an invalid "
            "ocel_changed_field"
        )

    initial_after_change = _first_row(
        changes.group_by(s.OCEL_ID)
        .agg(
            pl.col(s.OCEL_TIME)
            .filter(pl.col(s.OCEL_IS_INITIAL))
            .min()
            .alias("_initial_time"),
            pl.col(s.OCEL_TIME).min().alias("_first_time"),
        )
        .filter(pl.col("_initial_time") > pl.col("_first_time"))
    )
    if initial_after_change is not None:
        issues.append(
            f"object {initial_after_change[s.OCEL_ID]!r} has an initial state "
            "after another change"
        )

    core = set(CHANGE_SCHEMA)
    attributes = [name for name in changes.collect_schema().names() if name not in core]
    known_field = pl.lit(False)
    for attribute in attributes:
        known_field |= pl.col(s.OCEL_CHANGED_FIELD) == attribute
    unknown_field = _first_row(
        changes.filter(
            ~pl.col(s.OCEL_IS_INITIAL) & ~known_field.fill_null(False)
        ).select(s.OCEL_ID)
    )
    if unknown_field is not None:
        issues.append(
            f"object change for {unknown_field[s.OCEL_ID]!r} names an "
            "unknown ocel_changed_field"
        )

    for attribute in attributes:
        conflict = _first_row(
            changes.group_by(s.OCEL_ID, s.OCEL_TIME)
            .agg(
                pl.col(attribute).drop_nulls().n_unique().alias("_values"),
                (
                    (pl.col(s.OCEL_CHANGED_FIELD) == attribute)
                    & pl.col(attribute).is_null()
                )
                .any()
                .alias("_tombstone"),
                pl.col(attribute).is_not_null().any().alias("_has_value"),
            )
            .filter(
                (pl.col("_values") > 1)
                | (pl.col("_tombstone") & pl.col("_has_value"))
            )
        )
        if conflict is not None:
            issues.append(
                f"object {conflict[s.OCEL_ID]!r} has conflicting updates for "
                f"{attribute!r} at one timestamp"
            )
            break
    return issues


def _first_row(frame: pl.LazyFrame) -> dict[str, object] | None:
    result = frame.limit(1).collect(engine="streaming")
    if result.is_empty():
        return None
    return dict(result.row(0, named=True))


def _raise(issues: list[str]) -> None:
    raise OCELValidationError("Invalid OCEL:\n- " + "\n- ".join(issues))
