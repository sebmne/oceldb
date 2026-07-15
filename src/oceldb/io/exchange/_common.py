"""Shared semantic normalization for OCEL exchange formats."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal
from collections.abc import Iterable, Mapping

import polars as pl

from oceldb import schema as s
from oceldb.core.dataset import OCELDataset, OCELTables
from oceldb.core.presence import TypeDirectory
from oceldb.io._schema import materialize, validate_for_exchange
from oceldb.io._values import encode_attribute, format_datetime, qualifier
from oceldb.io.errors import ValidationMode
from oceldb.schema import AttributeType, OCELSchema, TypeAttributes
from oceldb.schema._layout import (
    E2O_SCHEMA,
    EVENTS_SCHEMA,
    OBJECT_CHANGES_SCHEMA,
    OBJECTS_SCHEMA,
    O2O_SCHEMA,
)
from oceldb.ocel import OCEL

EPOCH = "1970-01-01T00:00:00+00:00"
EPOCH_DATETIME = datetime(1970, 1, 1, tzinfo=timezone.utc)

ExchangeStyle = Literal["json", "xml"]


@dataclass(frozen=True)
class ExchangeAttribute:
    name: str
    value: Any
    time: str | None = None


@dataclass(frozen=True)
class ExchangeRelation:
    object_id: str
    qualifier: str


@dataclass(frozen=True)
class ExchangeEvent:
    id: str
    type: str
    time: str
    attributes: tuple[ExchangeAttribute, ...]
    relationships: tuple[ExchangeRelation, ...]


@dataclass(frozen=True)
class ExchangeObject:
    id: str
    type: str
    attributes: tuple[ExchangeAttribute, ...]
    relationships: tuple[ExchangeRelation, ...]


@dataclass(frozen=True)
class ExchangeDocument:
    schema: OCELSchema
    events: tuple[ExchangeEvent, ...]
    objects: tuple[ExchangeObject, ...]


@dataclass(frozen=True)
class ParsedEvent:
    row: dict[str, Any]
    relations: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class ParsedObject:
    row: dict[str, Any]
    changes: tuple[dict[str, Any], ...]
    relations: tuple[dict[str, Any], ...]


def prepare_exchange(
    ocel: OCEL,
    *,
    style: ExchangeStyle,
    validation: ValidationMode,
) -> ExchangeDocument:
    """Normalize an OCEL once for document-shaped exchange serializers."""
    data = materialize(ocel)
    validate_for_exchange(data, validation)
    event_relations = _group_relations(
        data.e2o,
        source=s.OCEL_EVENT_ID,
        target=s.OCEL_OBJECT_ID,
        validation=validation,
    )
    object_relations = _group_relations(
        data.o2o,
        source=s.OCEL_SOURCE_ID,
        target=s.OCEL_TARGET_ID,
        validation=validation,
    )

    events: list[ExchangeEvent] = []
    for row in data.events.sort(s.OCEL_TIME, s.OCEL_ID).iter_rows(named=True):
        event_id = str(row[s.OCEL_ID])
        type_name = str(row[s.OCEL_TYPE])
        events.append(
            ExchangeEvent(
                id=event_id,
                type=type_name,
                time=format_datetime(row[s.OCEL_TIME]),
                attributes=_event_attributes(
                    row,
                    data.schema.event_types.get(type_name, {}),
                    style=style,
                    validation=validation,
                    context=f"event {event_id!r}",
                ),
                relationships=event_relations.get(event_id, ()),
            )
        )

    changes: dict[str, list[ExchangeAttribute]] = {}
    for row in data.object_changes.sort(
        s.OCEL_ID, s.OCEL_TIME, s.OCEL_CHANGED_FIELD
    ).iter_rows(named=True):
        object_id = str(row[s.OCEL_ID])
        declarations = data.schema.object_types.get(str(row[s.OCEL_TYPE]), {})
        changed = row.get(s.OCEL_CHANGED_FIELD)
        names = [str(changed)] if changed else list(declarations)
        for name in names:
            value = row.get(name)
            if value is None:
                continue
            encoded = encode_attribute(
                value,
                declarations.get(name, AttributeType.STRING),
                style=style,
                validation=validation,
                context=f"object {object_id!r} attribute {name!r}",
            )
            if encoded is not None:
                changes.setdefault(object_id, []).append(
                    ExchangeAttribute(
                        name=name,
                        value=encoded,
                        time=format_datetime(row[s.OCEL_TIME]),
                    )
                )

    objects = tuple(
        ExchangeObject(
            id=str(row[s.OCEL_ID]),
            type=str(row[s.OCEL_TYPE]),
            attributes=tuple(changes.get(str(row[s.OCEL_ID]), ())),
            relationships=object_relations.get(str(row[s.OCEL_ID]), ()),
        )
        for row in data.objects.sort(s.OCEL_ID).iter_rows(named=True)
    )
    return ExchangeDocument(data.schema, tuple(events), objects)


def _event_attributes(
    row: dict[str, Any],
    declarations: TypeAttributes,
    *,
    style: ExchangeStyle,
    validation: ValidationMode,
    context: str,
) -> tuple[ExchangeAttribute, ...]:
    fixed = {s.OCEL_ID, s.OCEL_TIME, s.OCEL_TYPE}
    names = list(declarations) or [name for name in row if name not in fixed]
    attributes: list[ExchangeAttribute] = []
    for name in names:
        value = row.get(name)
        if value is None:
            continue
        encoded = encode_attribute(
            value,
            declarations.get(name, AttributeType.STRING),
            style=style,
            validation=validation,
            context=f"{context} attribute {name!r}",
        )
        if encoded is not None:
            attributes.append(ExchangeAttribute(name, encoded))
    return tuple(attributes)


def _group_relations(
    frame: pl.DataFrame,
    *,
    source: str,
    target: str,
    validation: ValidationMode,
) -> dict[str, tuple[ExchangeRelation, ...]]:
    grouped: dict[str, list[ExchangeRelation]] = {}
    if source not in frame.columns:
        return {}
    for row in frame.iter_rows(named=True):
        source_id = str(row[source])
        target_id = str(row[target])
        grouped.setdefault(source_id, []).append(
            ExchangeRelation(
                object_id=target_id,
                qualifier=qualifier(
                    row[s.OCEL_QUALIFIER],
                    validation=validation,
                    context=f"relation {source_id!r} -> {target_id!r}",
                ),
            )
        )
    return {key: tuple(relations) for key, relations in grouped.items()}


def empty_lf(schema: Mapping[str, pl.DataType]) -> pl.LazyFrame:
    return pl.DataFrame({k: pl.Series([], dtype=v) for k, v in schema.items()}).lazy()


def rows_to_lf(
    rows: list[dict[str, Any]],
    core_schema: Mapping[str, pl.DataType] | None = None,
) -> pl.LazyFrame:
    """Build a lazy frame while preserving canonical all-null core datatypes."""
    frame = pl.from_dicts(rows, infer_schema_length=None).lazy()
    return normalize_core(frame, core_schema) if core_schema is not None else frame


def normalize_core(
    frame: pl.LazyFrame, core_schema: Mapping[str, pl.DataType]
) -> pl.LazyFrame:
    """Cast and order the reserved columns of an existing lazy frame."""
    actual = frame.collect_schema()
    for name, dtype in core_schema.items():
        if name not in actual:
            frame = frame.with_columns(pl.lit(None, dtype=dtype).alias(name))
        elif actual[name] != dtype:
            frame = frame.with_columns(pl.col(name).cast(dtype, strict=True))
    return frame.select(
        *core_schema,
        *[name for name in actual if name not in core_schema],
    )


def build_ocel(
    *,
    event_rows: list[dict[str, Any]],
    object_rows: list[dict[str, Any]],
    object_change_rows: list[dict[str, Any]],
    e2o_rows: list[dict[str, Any]],
    o2o_rows: list[dict[str, Any]],
    schema: OCELSchema | None = None,
) -> OCEL:
    """Assemble an in-memory ``OCEL`` from parsed row dictionaries.

    Each list of rows is turned into a lazy frame; empty lists fall back to a
    correctly typed empty frame. Event and object-change timestamps are parsed
    from their ISO 8601 string form. The declared ``schema`` seeds the
    attribute directory so a later export keeps unused declarations. Shared by
    the JSON and XML readers.
    """
    tables = OCELTables.from_frames(
        events=rows_to_lf(event_rows, EVENTS_SCHEMA)
        if event_rows
        else empty_lf(EVENTS_SCHEMA),
        objects=rows_to_lf(object_rows, OBJECTS_SCHEMA)
        if object_rows
        else empty_lf(OBJECTS_SCHEMA),
        object_changes=rows_to_lf(object_change_rows, OBJECT_CHANGES_SCHEMA)
        if object_change_rows
        else empty_lf(OBJECT_CHANGES_SCHEMA),
        event_object=rows_to_lf(e2o_rows, E2O_SCHEMA)
        if e2o_rows
        else empty_lf(E2O_SCHEMA),
        object_object=rows_to_lf(o2o_rows, O2O_SCHEMA)
        if o2o_rows
        else empty_lf(O2O_SCHEMA),
    )
    return OCEL(
        OCELDataset(tables=tables),
        presence=TypeDirectory.from_schema(schema) if schema is not None else None,
    )


def collect_parsed(
    schema: OCELSchema,
    objects: Iterable[ParsedObject],
    events: Iterable[ParsedEvent],
) -> OCEL:
    """Collect normalized parser records into an in-memory OCEL."""
    object_rows: list[dict[str, Any]] = []
    change_rows: list[dict[str, Any]] = []
    o2o_rows: list[dict[str, Any]] = []
    for record in objects:
        object_rows.append(record.row)
        change_rows.extend(record.changes)
        o2o_rows.extend(record.relations)

    event_rows: list[dict[str, Any]] = []
    e2o_rows: list[dict[str, Any]] = []
    for record in events:
        event_rows.append(record.row)
        e2o_rows.extend(record.relations)

    return build_ocel(
        event_rows=event_rows,
        object_rows=object_rows,
        object_change_rows=change_rows,
        e2o_rows=e2o_rows,
        o2o_rows=o2o_rows,
        schema=schema,
    )


def parse_timestamps(lf: pl.LazyFrame, col: str) -> pl.LazyFrame:
    # OCEL timestamps are ISO 8601 with optional fractional seconds and
    # a colon-separated UTC offset (e.g. "2021-01-01T00:00:00+00:00").
    # Polars 1.x requires an explicit format when the string contains a TZ offset.
    return lf.with_columns(
        pl.col(col)
        .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%:z", time_unit="us", strict=False)
        .fill_null(
            # Fallback for fractional-second variants (e.g. ".000")
            pl.col(col).str.to_datetime(
                format="%Y-%m-%dT%H:%M:%S%.f%:z", time_unit="us", strict=False
            )
        )
    )
