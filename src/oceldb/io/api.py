"""Format-dispatching public OCEL I/O API."""

from pathlib import Path

from oceldb.io.codecs import ExchangeFormat, detect_codec, get_codec
from oceldb.io.errors import (
    OCELIOError,
    ValidationMode,
    check_validation_mode,
    io_boundary,
)
from oceldb.ocel import OCEL


def import_ocel(
    source: str | Path,
    target: str | Path,
    *,
    format: ExchangeFormat | None = None,
    overwrite: bool = False,
    validation: ValidationMode = "strict",
) -> OCEL:
    """Import an exchange file into a native dataset and open it lazily."""
    validation = check_validation_mode(validation)
    source_path = Path(source)
    if source_path.is_dir():
        raise OCELIOError(
            "import_ocel expects JSON, XML, or SQLite; use OCEL.open() for "
            "an existing native dataset."
        )
    with io_boundary(f"import {source_path} into", target):
        codec = get_codec(format) if format is not None else detect_codec(source_path)
        codec.import_to(
            source_path,
            Path(target),
            overwrite=overwrite,
            validation=validation,
        )
        return OCEL.open(target)


def export_ocel(
    source: OCEL | str | Path,
    target: str | Path,
    *,
    format: ExchangeFormat | None = None,
    overwrite: bool = False,
    validation: ValidationMode = "strict",
) -> None:
    """Export an OCEL or native dataset to JSON, XML, or SQLite."""
    validation = check_validation_mode(validation)
    target_path = Path(target)
    if format is None and not target_path.suffix:
        raise OCELIOError(
            "export_ocel expects JSON, XML, or SQLite; use OCEL.write() for "
            "native storage."
        )
    with io_boundary("export OCEL to", target_path):
        ocel = source if isinstance(source, OCEL) else OCEL.open(source)
        codec = get_codec(format) if format is not None else detect_codec(target_path)
        codec.write(
            ocel,
            target_path,
            overwrite=overwrite,
            validation=validation,
        )
