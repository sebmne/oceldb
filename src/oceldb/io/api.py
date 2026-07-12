"""Format-dispatching public OCEL I/O API."""

from __future__ import annotations

from pathlib import Path

from oceldb.io.codecs import ExchangeFormat, detect_codec, get_codec
from oceldb.io.errors import ValidationMode
from oceldb.ocel import OCEL


def open_ocel(path: str | Path) -> OCEL:
    """Open a native Parquet dataset lazily.

    Unlike exchange readers, this only creates Parquet scans and reads the
    small manifest. Use :func:`import_ocel` to persist JSON, XML, or SQLite as
    a native dataset before opening it for repeated analysis.
    """
    return OCEL.open(path)


def import_ocel(
    source: str | Path,
    target: str | Path,
    *,
    format: ExchangeFormat | None = None,
    overwrite: bool = False,
    validation: ValidationMode = "strict",
) -> OCEL:
    """Import an exchange file into a native dataset and open it lazily."""
    source_path = Path(source)
    if source_path.is_dir():
        raise ValueError(
            "import_ocel expects JSON, XML, or SQLite; use open_ocel() for "
            "an existing native dataset."
        )
    codec = get_codec(format) if format is not None else detect_codec(source_path)
    codec.import_to(
        source_path,
        Path(target),
        overwrite=overwrite,
        validation=validation,
    )
    return open_ocel(target)


def export_ocel(
    source: OCEL | str | Path,
    target: str | Path,
    *,
    format: ExchangeFormat | None = None,
    overwrite: bool = False,
    validation: ValidationMode = "strict",
) -> None:
    """Export an OCEL or native dataset to JSON, XML, or SQLite."""
    ocel = source if isinstance(source, OCEL) else open_ocel(source)
    target_path = Path(target)
    if format is None and not target_path.suffix:
        raise ValueError(
            "export_ocel expects JSON, XML, or SQLite; use OCEL.write() for "
            "native storage."
        )
    codec = get_codec(format) if format is not None else detect_codec(target_path)
    codec.write(
        ocel,
        target_path,
        overwrite=overwrite,
        validation=validation,
    )
