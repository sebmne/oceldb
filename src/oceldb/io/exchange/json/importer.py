"""Incrementally import OCEL 2.0 JSON into native storage."""

from pathlib import Path

from oceldb.io.errors import ValidationMode, check_validation_mode
from oceldb.io.exchange.json.reader import JSONParser
from oceldb.io.native.batch import NativeBatchSink


def import_json(
    source: str | Path,
    target: str | Path,
    *,
    overwrite: bool = False,
    validation: ValidationMode = "strict",
    batch_size: int = 10_000,
) -> None:
    """Stream an OCEL JSON document into staged native Parquet storage."""
    validation = check_validation_mode(validation)
    parser = JSONParser(source, validation=validation)
    with NativeBatchSink(
        target,
        parser.schema,
        overwrite=overwrite,
        batch_size=batch_size,
        validation=validation,
    ) as sink:
        for record in parser.objects():
            sink.add_objects((record.row,))
            sink.add_object_changes(record.changes)
            sink.add_o2o(record.relations)
        for record in parser.events():
            sink.add_events((record.row,))
            sink.add_e2o(record.relations)
