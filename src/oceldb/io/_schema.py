"""Schema resolution and materialization shared by format writers."""

from dataclasses import dataclass

import polars as pl

from oceldb.core.dataset import OCELDataset, OCELTables
from oceldb.core.presence import TypeDirectory
from oceldb.core.validation import dataset_issues
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

    def as_dataset(self) -> OCELDataset:
        """Expose the collected tables through the canonical dataset contract."""
        return OCELDataset(
            tables=OCELTables.from_frames(
                events=self.events.lazy(),
                objects=self.objects.lazy(),
                object_changes=self.object_changes.lazy(),
                event_object=self.e2o.lazy(),
                object_object=self.o2o.lazy(),
            ),
        )


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
    """Apply the canonical dataset validator with exchange error semantics."""
    validate_dataset_for_io(data.as_dataset(), validation)


def validate_dataset_for_io(dataset: OCELDataset, validation: ValidationMode) -> None:
    """Apply canonical validation using strict/warn/none IO semantics."""
    if validation == "none":
        return
    for message in dataset_issues(dataset):
        issue(validation, message)
