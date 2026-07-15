"""Internal dispatch registry for OCEL exchange codecs."""

from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from collections.abc import Mapping
from typing import Literal, Protocol

from oceldb.io.errors import OCELIOError, ValidationMode
from oceldb.io.exchange.json.importer import import_json
from oceldb.io.exchange.json.reader import read_json
from oceldb.io.exchange.json.writer import write_json
from oceldb.io.exchange.sqlite.importer import import_sqlite
from oceldb.io.exchange.sqlite.writer import write_sqlite
from oceldb.io.exchange.xml.importer import import_xml
from oceldb.io.exchange.xml.reader import read_xml
from oceldb.io.exchange.xml.writer import write_xml
from oceldb.ocel import OCEL

ExchangeFormat = Literal["json", "xml", "sqlite"]


class Reader(Protocol):
    def __call__(
        self, path: str | Path, *, validation: ValidationMode = "strict"
    ) -> OCEL: ...


class Writer(Protocol):
    def __call__(
        self,
        ocel: OCEL,
        path: str | Path,
        *,
        overwrite: bool = False,
        validation: ValidationMode = "strict",
    ) -> None: ...


class Importer(Protocol):
    def __call__(
        self,
        source: str | Path,
        target: str | Path,
        *,
        overwrite: bool = False,
        validation: ValidationMode = "strict",
    ) -> None: ...


@dataclass(frozen=True)
class _ExchangeCodec:
    """Internal adapter for a lossless OCEL exchange encoding."""

    name: ExchangeFormat
    extensions: tuple[str, ...]
    _reader: Reader | None
    _writer: Writer
    _importer: Importer | None = None

    def read(self, path: Path, *, validation: ValidationMode) -> OCEL:
        if self._reader is None:
            raise NotImplementedError(
                f"{self.name} has no one-off reader; import it into native storage."
            )
        return self._reader(path, validation=validation)

    def write(
        self,
        ocel: OCEL,
        path: Path,
        *,
        overwrite: bool,
        validation: ValidationMode,
    ) -> None:
        self._writer(ocel, path, overwrite=overwrite, validation=validation)

    def import_to(
        self,
        source: Path,
        target: Path,
        *,
        overwrite: bool,
        validation: ValidationMode,
    ) -> None:
        """Import into native storage, using a direct converter when present."""
        if self._importer is not None:
            self._importer(
                source,
                target,
                overwrite=overwrite,
                validation=validation,
            )
            return
        self.read(source, validation=validation).write(target, overwrite=overwrite)


_CODECS: Mapping[ExchangeFormat, _ExchangeCodec] = MappingProxyType(
    {
        "json": _ExchangeCodec(
            "json",
            (".json", ".jsonocel"),
            read_json,
            write_json,
            import_json,
        ),
        "xml": _ExchangeCodec(
            "xml",
            (".xml", ".xmlocel"),
            read_xml,
            write_xml,
            import_xml,
        ),
        "sqlite": _ExchangeCodec(
            "sqlite",
            (".sqlite", ".sqlite3", ".db"),
            None,
            write_sqlite,
            import_sqlite,
        ),
    }
)


def get_codec(format: ExchangeFormat) -> _ExchangeCodec:
    """Return a codec by its explicit exchange-format name."""
    try:
        return _CODECS[format]
    except KeyError as exc:
        raise OCELIOError(f"Unsupported OCEL exchange format: {format!r}.") from exc


def detect_codec(path: str | Path) -> _ExchangeCodec:
    """Select an exchange codec from a file extension."""
    suffix = Path(path).suffix.lower()
    for codec in _CODECS.values():
        if suffix in codec.extensions:
            return codec
    raise OCELIOError(
        f"Cannot infer OCEL exchange format from {path}. Pass format explicitly."
    )
