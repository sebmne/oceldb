"""Public dispatch for direct OCEL exchange-to-native conversion."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
import tempfile
from typing import Literal

from oceldb import OCEL
from oceldb.core.write import _check_target, _target_path
from oceldb.io._errors import OCELConversionError
from oceldb.types import PathLikeStr

ExchangeFormat = Literal["sqlite", "json", "xml"]
Converter = Callable[[Path, Path, Path, bool, bool, int], OCEL]
_EXTENSIONS: dict[str, ExchangeFormat] = {
    ".sqlite": "sqlite",
    ".sqlite3": "sqlite",
    ".db": "sqlite",
    ".json": "json",
    ".jsonocel": "json",
    ".xml": "xml",
    ".xmlocel": "xml",
}


def convert_ocel(
    source: PathLikeStr,
    target: PathLikeStr,
    *,
    format: ExchangeFormat | None = None,
    overwrite: bool = False,
    validate: bool = True,
    batch_size: int = 10_000,
) -> OCEL:
    """Convert an OCEL 2.0 exchange file into native storage.

    The input format is inferred from the extension and, when necessary, a
    small content signature. Pass ``format`` to make dispatch explicit.

    Args:
        source: OCEL 2.0 SQLite, JSON, or XML file.
        target: Destination native snapshot directory.
        format: Explicit source format. Defaults to automatic detection.
        overwrite: Replace an existing valid native snapshot.
        validate: Execute complete logical validation before installation.
        batch_size: Maximum normalized rows buffered per logical table.

    Returns:
        An opened, lazy :class:`~oceldb.OCEL` for the native snapshot.

    Raises:
        FileNotFoundError: If ``source`` does not exist.
        FileExistsError: If ``target`` exists and overwrite is disabled.
        OCELConversionError: If the exchange document is malformed.
        ValueError: If an option is invalid.
    """
    source_path = _source_path(source)
    selected = detect_format(source_path) if format is None else format
    if selected not in {"sqlite", "json", "xml"}:
        raise ValueError("format must be 'sqlite', 'json', or 'xml'.")
    if selected == "sqlite":
        return convert_sqlite(
            source_path,
            target,
            overwrite=overwrite,
            validate=validate,
            batch_size=batch_size,
        )
    if selected == "json":
        return convert_json(
            source_path,
            target,
            overwrite=overwrite,
            validate=validate,
            batch_size=batch_size,
        )
    return convert_xml(
        source_path,
        target,
        overwrite=overwrite,
        validate=validate,
        batch_size=batch_size,
    )


def convert_json(
    source: PathLikeStr,
    target: PathLikeStr,
    *,
    overwrite: bool = False,
    validate: bool = True,
    batch_size: int = 10_000,
) -> OCEL:
    """Convert an OCEL 2.0 JSON file directly into native storage.

    Args:
        source: Source ``.json`` or ``.jsonocel`` file.
        target: Destination native snapshot directory.
        overwrite: Replace an existing valid native snapshot.
        validate: Execute complete logical validation before installation.
        batch_size: Maximum normalized rows buffered per logical table.

    Returns:
        An opened, lazy OCEL for the installed native snapshot.

    Raises:
        FileNotFoundError: If ``source`` does not exist.
        FileExistsError: If ``target`` already exists.
        OCELConversionError: If the JSON document violates the exchange
            contract.
        OCELValidationError: If logical validation fails.
    """
    from oceldb.io.converters.json import convert

    return _convert(
        convert,
        source,
        target,
        overwrite=overwrite,
        validate=validate,
        batch_size=batch_size,
    )


def convert_xml(
    source: PathLikeStr,
    target: PathLikeStr,
    *,
    overwrite: bool = False,
    validate: bool = True,
    batch_size: int = 10_000,
) -> OCEL:
    """Convert an OCEL 2.0 XML file directly into native storage.

    Args:
        source: Source ``.xml`` or ``.xmlocel`` file.
        target: Destination native snapshot directory.
        overwrite: Replace an existing valid native snapshot.
        validate: Execute complete logical validation before installation.
        batch_size: Maximum normalized rows buffered per logical table.

    Returns:
        An opened, lazy OCEL for the installed native snapshot.

    Raises:
        FileNotFoundError: If ``source`` does not exist.
        FileExistsError: If ``target`` already exists.
        OCELConversionError: If the XML document violates the exchange
            contract.
        OCELValidationError: If logical validation fails.
    """
    from oceldb.io.converters.xml import convert

    return _convert(
        convert,
        source,
        target,
        overwrite=overwrite,
        validate=validate,
        batch_size=batch_size,
    )


def convert_sqlite(
    source: PathLikeStr,
    target: PathLikeStr,
    *,
    overwrite: bool = False,
    validate: bool = True,
    batch_size: int = 10_000,
) -> OCEL:
    """Convert an OCEL 2.0 SQLite file directly into native storage.

    Args:
        source: Source ``.sqlite`` or ``.sqlite3`` file.
        target: Destination native snapshot directory.
        overwrite: Replace an existing valid native snapshot.
        validate: Execute complete logical validation before installation.
        batch_size: Maximum normalized rows buffered per logical table.

    Returns:
        An opened, lazy OCEL for the installed native snapshot.

    Raises:
        FileNotFoundError: If ``source`` does not exist.
        FileExistsError: If ``target`` already exists.
        OCELConversionError: If the SQLite database violates the exchange
            contract.
        OCELValidationError: If logical validation fails.
    """
    from oceldb.io.converters.sqlite import convert

    return _convert(
        convert,
        source,
        target,
        overwrite=overwrite,
        validate=validate,
        batch_size=batch_size,
    )


def detect_format(path: PathLikeStr) -> ExchangeFormat:
    """Detect an OCEL 2.0 exchange format from its suffix or signature.

    Args:
        path: Existing exchange file.

    Returns:
        ``"sqlite"``, ``"json"``, or ``"xml"``.

    Raises:
        FileNotFoundError: If ``path`` does not exist.
        OCELConversionError: If neither the suffix nor content is recognized.
    """
    source = _source_path(path)
    suffix = source.suffix.lower()
    if suffix in _EXTENSIONS:
        return _EXTENSIONS[suffix]
    with source.open("rb") as stream:
        header = stream.read(256)
    if header.startswith(b"SQLite format 3\x00"):
        return "sqlite"
    stripped = header.lstrip()
    if stripped.startswith(b"{"):
        return "json"
    if stripped.startswith((b"<", b"\xef\xbb\xbf<")):
        return "xml"
    raise OCELConversionError(
        f"Cannot detect the OCEL exchange format for {source}. "
        "Pass format='sqlite', 'json', or 'xml'."
    )


def _convert(
    converter: Converter,
    source: PathLikeStr,
    target: PathLikeStr,
    *,
    overwrite: bool,
    validate: bool,
    batch_size: int,
) -> OCEL:
    source_path = _source_path(source)
    target_path = _target_path(target)
    _check_target(target_path, overwrite=overwrite)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(batch_size, bool) or batch_size < 1:
        raise ValueError("batch_size must be a positive integer.")
    with tempfile.TemporaryDirectory(
        prefix=f".{target_path.name}.conversion-",
        dir=target_path.parent,
    ) as staging:
        try:
            return converter(
                source_path,
                target_path,
                Path(staging),
                overwrite,
                validate,
                batch_size,
            )
        except OCELConversionError as exc:
            raise OCELConversionError(f"Cannot convert {source_path}: {exc}") from exc


def _source_path(path: PathLikeStr) -> Path:
    result = Path(path).expanduser()
    if not result.exists():
        raise FileNotFoundError(f"OCEL exchange file not found: {result}")
    if not result.is_file():
        raise ValueError(f"OCEL exchange source must be a file: {result}")
    return result
