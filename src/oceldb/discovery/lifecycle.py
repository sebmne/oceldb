from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from statistics import mean, median
from typing import Mapping, cast

from oceldb.core.ocel import OCEL
from oceldb.inspect import object_attributes, object_types
from oceldb.sql.object_history import render_object_change_batches_source


@dataclass(frozen=True)
class LifecycleState:
    attributes: tuple[tuple[str, object | None], ...]

    def project(self, *attributes: str) -> "LifecycleState":
        attribute_set = set(attributes)
        return LifecycleState(
            attributes=tuple(
                (name, value)
                for name, value in self.attributes
                if name in attribute_set
            )
        )

    def as_dict(self) -> dict[str, object | None]:
        return dict(self.attributes)

    def get(self, attribute: str) -> object | None:
        for name, value in self.attributes:
            if name == attribute:
                return value
        raise KeyError(f"Unknown lifecycle attribute: {attribute!r}")


@dataclass(frozen=True)
class LifecycleStateCount:
    state: LifecycleState
    count: int


@dataclass(frozen=True)
class LifecycleTransition:
    source: LifecycleState
    target: LifecycleState
    changed_attributes: tuple[str, ...]
    count: int
    mean_duration_seconds: float | None
    median_duration_seconds: float | None
    min_duration_seconds: float | None
    max_duration_seconds: float | None


@dataclass(frozen=True)
class _LifecycleStep:
    time: datetime
    state: LifecycleState


@dataclass(frozen=True)
class _ObjectLifecycleSequence:
    object_id: str
    steps: tuple[_LifecycleStep, ...]


@dataclass(frozen=True)
class ObjectLifecycle:
    object_type: str
    attributes: tuple[str, ...]
    include_null: bool
    collapse_repeats: bool
    object_count: int
    objects_with_changes: int
    objects_without_changes: int
    objects_with_lifecycle: int
    objects_without_lifecycle: int
    avg_steps_per_object: float | None
    median_steps_per_object: float | None
    min_steps_per_object: int | None
    max_steps_per_object: int | None
    states: tuple[LifecycleStateCount, ...]
    starts: tuple[LifecycleStateCount, ...]
    ends: tuple[LifecycleStateCount, ...]
    transitions: tuple[LifecycleTransition, ...]
    _changed_object_ids: frozenset[str] = field(repr=False, compare=False)
    _sequences: tuple[_ObjectLifecycleSequence, ...] = field(repr=False, compare=False)

    def project(self, *attributes: str) -> "ObjectLifecycle":
        projected_attributes = _dedupe_preserving_order(attributes)
        if not projected_attributes:
            raise ValueError("project(...) requires at least one attribute")

        unknown_attributes = [
            attribute
            for attribute in projected_attributes
            if attribute not in self.attributes
        ]
        if unknown_attributes:
            unknown = ", ".join(repr(value) for value in unknown_attributes)
            raise ValueError(
                f"Unknown lifecycle attributes for projection: {unknown}"
            )

        if projected_attributes == self.attributes:
            return self

        projected_sequences = tuple(
            _ObjectLifecycleSequence(
                object_id=sequence.object_id,
                steps=_project_steps(
                    sequence.steps,
                    attributes=projected_attributes,
                    include_null=self.include_null,
                    collapse_repeats=self.collapse_repeats,
                ),
            )
            for sequence in self._sequences
        )

        return _summarize_lifecycle(
            object_type=self.object_type,
            attributes=projected_attributes,
            include_null=self.include_null,
            collapse_repeats=self.collapse_repeats,
            changed_object_ids=self._changed_object_ids,
            sequences=projected_sequences,
        )

    def state(
        self,
        /,
        **attributes: object | None,
    ) -> LifecycleStateCount:
        expected = set(self.attributes)
        provided = set(attributes)
        if provided != expected:
            missing = tuple(sorted(expected - provided))
            extra = tuple(sorted(provided - expected))
            raise ValueError(
                "state(...) requires an exact lifecycle state specification; "
                f"missing={missing}, extra={extra}"
            )

        target = LifecycleState(
            attributes=tuple(
                (attribute, attributes[attribute])
                for attribute in self.attributes
            )
        )

        for entry in self.states:
            if entry.state == target:
                return entry
        raise KeyError(f"Unknown lifecycle state: {target.attributes!r}")

    def transition(
        self,
        *,
        source: LifecycleState | Mapping[str, object | None],
        target: LifecycleState | Mapping[str, object | None],
    ) -> LifecycleTransition:
        source_state = self._coerce_state(source)
        target_state = self._coerce_state(target)

        for entry in self.transitions:
            if entry.source == source_state and entry.target == target_state:
                return entry
        raise KeyError(
            "Unknown lifecycle transition: "
            f"{source_state.attributes!r} -> {target_state.attributes!r}"
        )

    def _coerce_state(
        self,
        value: LifecycleState | Mapping[str, object | None],
    ) -> LifecycleState:
        if isinstance(value, LifecycleState):
            attribute_names = tuple(name for name, _ in value.attributes)
            if attribute_names != self.attributes:
                raise ValueError(
                    "LifecycleState does not match lifecycle attributes; "
                    f"expected {self.attributes!r}, got {attribute_names!r}"
                )
            return value

        provided = set(value)
        expected = set(self.attributes)
        if provided != expected:
            missing = tuple(sorted(expected - provided))
            extra = tuple(sorted(provided - expected))
            raise ValueError(
                "transition(...) requires exact source/target states over the "
                f"lifecycle attributes; missing={missing}, extra={extra}"
            )

        return LifecycleState(
            attributes=tuple(
                (attribute, value[attribute])
                for attribute in self.attributes
            )
        )


def object_lifecycle(
    ocel: OCEL,
    object_type: str,
    *,
    attributes: tuple[str, ...] | None = None,
    include_null: bool = False,
    collapse_repeats: bool = True,
) -> ObjectLifecycle:
    """
    Summarize the lifecycle of one object type as reconstructed state sequences.

    This is a discovery helper, not a row-query API. It groups raw
    `object_change` rows into timestamp batches, reconstructs per-object states
    over the selected type-owned attributes, and returns aggregate lifecycle
    statistics such as state frequencies, start/end states, and transitions.

    Use `ocel.query.object_changes(...)` for raw sparse history rows and
    `ocel.query.object_states(...)` for queryable latest/as-of state tables.
    """
    selected_attributes = _resolve_attributes(
        ocel,
        object_type=object_type,
        attributes=attributes,
    )
    _validate_timestamp_batches(
        ocel,
        object_type=object_type,
        attributes=selected_attributes,
    )

    object_ids = _load_object_ids(ocel, object_type)
    objects_with_changes = _load_changed_object_ids(ocel, object_type)
    raw_steps = _load_projected_state_steps(
        ocel,
        object_type=object_type,
        attributes=selected_attributes,
    )
    sequences = _build_lifecycle_sequences(
        raw_steps,
        attributes=selected_attributes,
        include_null=include_null,
        collapse_repeats=collapse_repeats,
    )

    materialized_sequences = tuple(
        _ObjectLifecycleSequence(
            object_id=object_id,
            steps=tuple(
                _LifecycleStep(time=timestamp, state=state)
                for timestamp, state in sequences.get(object_id, ())
            ),
        )
        for object_id in object_ids
    )

    return _summarize_lifecycle(
        object_type=object_type,
        attributes=selected_attributes,
        include_null=include_null,
        collapse_repeats=collapse_repeats,
        changed_object_ids=frozenset(objects_with_changes),
        sequences=materialized_sequences,
    )


def _resolve_attributes(
    ocel: OCEL,
    *,
    object_type: str,
    attributes: tuple[str, ...] | None,
) -> tuple[str, ...]:
    available_object_types = set(object_types(ocel))
    if object_type not in available_object_types:
        raise ValueError(f"Unknown object type: {object_type!r}")

    available_attributes = tuple(object_attributes(ocel, object_type))
    available_set = set(available_attributes)

    if attributes is None:
        selected_attributes = available_attributes
    else:
        selected_attributes = _dedupe_preserving_order(attributes)

    if not selected_attributes:
        raise ValueError(
            "object_lifecycle(...) requires at least one object attribute"
        )

    unknown_attributes = [
        attribute
        for attribute in selected_attributes
        if attribute not in available_set
    ]
    if unknown_attributes:
        unknown = ", ".join(repr(value) for value in unknown_attributes)
        raise ValueError(
            f"Unknown object attributes for {object_type!r}: {unknown}"
        )

    return selected_attributes


def _validate_timestamp_batches(
    ocel: OCEL,
    *,
    object_type: str,
    attributes: tuple[str, ...],
) -> None:
    subqueries: list[str] = []
    object_type_sql = _render_string_literal(object_type)

    for attribute in attributes:
        attribute_sql = _quote_ident(attribute)
        subqueries.append(f"""
            SELECT
                {_render_string_literal(attribute)} AS "attribute",
                c."ocel_id",
                c."ocel_time"
            FROM "object_change" c
            WHERE c."ocel_type" = {object_type_sql}
            GROUP BY c."ocel_id", c."ocel_time"
            HAVING COUNT(
                DISTINCT CASE
                    WHEN c.{attribute_sql} IS NOT NULL THEN c.{attribute_sql}
                    ELSE NULL
                END
            ) > 1
        """)

    row = ocel.sql(f"""
        {" UNION ALL ".join(subqueries)}
        LIMIT 1
    """).fetchone()

    if row is None:
        return

    raise ValueError(
        "object_lifecycle(...) requires deterministic post-timestamp states; "
        f"found multiple distinct values for attribute {row[0]!r} on object "
        f"{row[1]!r} at timestamp {row[2]!r}"
    )


def _load_object_ids(ocel: OCEL, object_type: str) -> tuple[str, ...]:
    rows = ocel.sql(f"""
        SELECT "ocel_id"
        FROM "object"
        WHERE "ocel_type" = {_render_string_literal(object_type)}
        ORDER BY "ocel_id"
    """).fetchall()
    return tuple(row[0] for row in rows)


def _load_changed_object_ids(ocel: OCEL, object_type: str) -> set[str]:
    rows = ocel.sql(f"""
        SELECT DISTINCT "ocel_id"
        FROM "object_change"
        WHERE "ocel_type" = {_render_string_literal(object_type)}
    """).fetchall()
    return {row[0] for row in rows}


def _load_projected_state_steps(
    ocel: OCEL,
    *,
    object_type: str,
    attributes: tuple[str, ...],
) -> list[tuple[str, datetime, tuple[object | None, ...]]]:
    object_change_columns = (
        "ocel_id",
        "ocel_type",
        "ocel_time",
        "ocel_changed_field",
        *attributes,
    )
    batch_updates_sql = render_object_change_batches_source(
        object_change_columns,
        object_types=(object_type,),
    )
    state_columns = ",\n            ".join(
        (
            f'LAST_VALUE(bu.{_quote_ident(attribute)} IGNORE NULLS) '
            f'OVER state_window AS {_quote_ident(attribute)}'
        )
        for attribute in attributes
    )
    select_columns = ", ".join(_quote_ident(attribute) for attribute in attributes)

    rows = ocel.sql(f"""
        WITH batch_states AS (
            SELECT
                bu."ocel_id",
                bu."ocel_time",
                {state_columns}
            FROM ({batch_updates_sql}) bu
            WINDOW state_window AS (
                PARTITION BY bu."ocel_id"
                ORDER BY bu."ocel_time"
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        )
        SELECT
            "ocel_id",
            "ocel_time",
            {select_columns}
        FROM batch_states
        ORDER BY "ocel_id", "ocel_time"
    """).fetchall()

    return [
        (
            cast(str, row[0]),
            cast(datetime, row[1]),
            tuple(cast(object | None, value) for value in row[2:]),
        )
        for row in rows
    ]


def _build_lifecycle_sequences(
    raw_steps: list[tuple[str, datetime, tuple[object | None, ...]]],
    *,
    attributes: tuple[str, ...],
    include_null: bool,
    collapse_repeats: bool,
) -> dict[str, tuple[tuple[datetime, LifecycleState], ...]]:
    sequences: dict[str, list[tuple[datetime, LifecycleState]]] = defaultdict(list)

    for object_id, timestamp, values in raw_steps:
        if not include_null and all(value is None for value in values):
            continue

        state = LifecycleState(
            attributes=tuple(zip(attributes, values, strict=True))
        )
        current = sequences[object_id]

        if collapse_repeats and current and current[-1][1] == state:
            continue

        current.append((timestamp, state))

    return {
        object_id: tuple(steps)
        for object_id, steps in sequences.items()
    }


def _project_steps(
    steps: tuple[_LifecycleStep, ...],
    *,
    attributes: tuple[str, ...],
    include_null: bool,
    collapse_repeats: bool,
) -> tuple[_LifecycleStep, ...]:
    projected_steps: list[_LifecycleStep] = []

    for step in steps:
        projected_state = step.state.project(*attributes)
        values = tuple(value for _, value in projected_state.attributes)
        if not include_null and all(value is None for value in values):
            continue
        if collapse_repeats and projected_steps and projected_steps[-1].state == projected_state:
            continue
        projected_steps.append(
            _LifecycleStep(
                time=step.time,
                state=projected_state,
            )
        )

    return tuple(projected_steps)


def _summarize_lifecycle(
    *,
    object_type: str,
    attributes: tuple[str, ...],
    include_null: bool,
    collapse_repeats: bool,
    changed_object_ids: frozenset[str],
    sequences: tuple[_ObjectLifecycleSequence, ...],
) -> ObjectLifecycle:
    step_counts = [len(sequence.steps) for sequence in sequences]
    objects_with_lifecycle = sum(1 for count in step_counts if count > 0)

    state_counts: Counter[LifecycleState] = Counter()
    start_counts: Counter[LifecycleState] = Counter()
    end_counts: Counter[LifecycleState] = Counter()
    transition_durations: defaultdict[
        tuple[LifecycleState, LifecycleState],
        list[float],
    ] = defaultdict(list)

    for sequence in sequences:
        if not sequence.steps:
            continue

        states = [step.state for step in sequence.steps]
        state_counts.update(states)
        start_counts[states[0]] += 1
        end_counts[states[-1]] += 1

        for source_step, target_step in zip(sequence.steps, sequence.steps[1:]):
            duration = (target_step.time - source_step.time).total_seconds()
            transition_durations[(source_step.state, target_step.state)].append(
                float(duration)
            )

    return ObjectLifecycle(
        object_type=object_type,
        attributes=attributes,
        include_null=include_null,
        collapse_repeats=collapse_repeats,
        object_count=len(sequences),
        objects_with_changes=len(changed_object_ids),
        objects_without_changes=len(sequences) - len(changed_object_ids),
        objects_with_lifecycle=objects_with_lifecycle,
        objects_without_lifecycle=len(sequences) - objects_with_lifecycle,
        avg_steps_per_object=_avg_ints(step_counts),
        median_steps_per_object=_median_ints(step_counts),
        min_steps_per_object=min(step_counts) if step_counts else None,
        max_steps_per_object=max(step_counts) if step_counts else None,
        states=_finalize_state_counts(state_counts),
        starts=_finalize_state_counts(start_counts),
        ends=_finalize_state_counts(end_counts),
        transitions=_finalize_transitions(
            transition_durations,
            attributes=attributes,
        ),
        _changed_object_ids=changed_object_ids,
        _sequences=sequences,
    )


def _finalize_state_counts(
    counts: Counter[LifecycleState],
) -> tuple[LifecycleStateCount, ...]:
    rows = [
        LifecycleStateCount(state=state, count=count)
        for state, count in counts.items()
    ]
    rows.sort(key=lambda row: (-row.count, _state_sort_key(row.state)))
    return tuple(rows)


def _finalize_transitions(
    durations_by_transition: defaultdict[tuple[LifecycleState, LifecycleState], list[float]],
    *,
    attributes: tuple[str, ...],
) -> tuple[LifecycleTransition, ...]:
    rows: list[LifecycleTransition] = []

    for (source, target), durations in durations_by_transition.items():
        rows.append(
            LifecycleTransition(
                source=source,
                target=target,
                changed_attributes=_changed_attributes(
                    source,
                    target,
                    attributes=attributes,
                ),
                count=len(durations),
                mean_duration_seconds=_avg_floats(durations),
                median_duration_seconds=_median_floats(durations),
                min_duration_seconds=min(durations) if durations else None,
                max_duration_seconds=max(durations) if durations else None,
            )
        )

    rows.sort(
        key=lambda row: (
            -row.count,
            _state_sort_key(row.source),
            _state_sort_key(row.target),
        )
    )
    return tuple(rows)


def _changed_attributes(
    source: LifecycleState,
    target: LifecycleState,
    *,
    attributes: tuple[str, ...],
) -> tuple[str, ...]:
    source_values = source.as_dict()
    target_values = target.as_dict()
    return tuple(
        attribute
        for attribute in attributes
        if source_values[attribute] != target_values[attribute]
    )


def _state_sort_key(state: LifecycleState) -> tuple[str, ...]:
    return tuple(f"{name}={value!r}" for name, value in state.attributes)


def _dedupe_preserving_order(values: tuple[str, ...]) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return tuple(result)


def _avg_ints(values: list[int]) -> float | None:
    if not values:
        return None
    return float(mean(values))


def _median_ints(values: list[int]) -> float | None:
    if not values:
        return None
    return float(median(values))


def _avg_floats(values: list[float]) -> float | None:
    if not values:
        return None
    return float(mean(values))


def _median_floats(values: list[float]) -> float | None:
    if not values:
        return None
    return float(median(values))


def _quote_ident(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _render_string_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"
