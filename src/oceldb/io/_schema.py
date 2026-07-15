"""Schema resolution and materialization shared by format writers."""

from collections.abc import Mapping
from dataclasses import dataclass

import polars as pl

from oceldb.core.presence import TypeDirectory
from oceldb.core.validation import table_issues
from oceldb.io.errors import ValidationMode, issue
from oceldb.schema import OCELSchema
from oceldb.ocel import OCEL


@dataclass(frozen=True)
class MaterializedOCEL:
    events: pl.DataFrame
    objects: pl.DataFrame
    object_changes: pl.DataFrame
    e2o: pl.DataFrame
    o2o: pl.DataFrame
    schema: OCELSchema


def materialize(ocel: OCEL) -> MaterializedOCEL:
    """Collect an OCEL once and resolve its complete exchange schema.

    A seeded attribute directory (logs opened or read at an IO boundary)
    keeps declared-but-empty types and all-null attributes; otherwise the
    schema is observed from the collected tables, typing attributes by their
    physical dtype.
    """
    events = ocel.events().collect()
    objects = ocel.objects().collect()
    changes = ocel.object_changes().collect()
    directory = ocel._presence
    if directory is None:
        directory = TypeDirectory.probe(
            events=events.lazy(),
            objects=objects.lazy(),
            object_changes=changes.lazy(),
        )
    return MaterializedOCEL(
        events=events,
        objects=objects,
        object_changes=changes,
        e2o=ocel.event_object().collect(),
        o2o=ocel.object_object().collect(),
        schema=directory.to_schema(),
    )


def validate_for_exchange(data: MaterializedOCEL, validation: ValidationMode) -> None:
    """Apply the canonical table validator with exchange error semantics."""
    validate_frames_for_io(
        {
            "events": data.events.lazy(),
            "objects": data.objects.lazy(),
            "object_changes": data.object_changes.lazy(),
            "event_object": data.e2o.lazy(),
            "object_object": data.o2o.lazy(),
        },
        validation,
    )


def validate_ocel_for_io(ocel: OCEL, validation: ValidationMode) -> None:
    """Apply canonical validation to an OCEL with strict/warn/none semantics."""
    validate_frames_for_io(
        {
            "events": ocel.events(),
            "objects": ocel.objects(),
            "object_changes": ocel.object_changes(),
            "event_object": ocel.event_object(),
            "object_object": ocel.object_object(),
        },
        validation,
    )


def validate_frames_for_io(
    frames: Mapping[str, pl.LazyFrame], validation: ValidationMode
) -> None:
    """Apply canonical validation using strict/warn/none IO semantics."""
    if validation == "none":
        return
    for message in table_issues(**frames):
        issue(validation, message)
