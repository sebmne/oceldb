"""Explicit registry and capabilities for OCEL exchange codecs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from collections.abc import Callable, Mapping
from typing import Literal

from oceldb.io.errors import ValidationMode
from oceldb.io.exchange import (
    import_json,
    import_sqlite,
    import_xml,
    read_json,
    read_xml,
    write_json,
    write_sqlite,
    write_xml,
)
from oceldb.ocel import OCEL

ExchangeFormat = Literal["json", "xml", "sqlite"]


@dataclass(frozen=True)
class CodecCapabilities:
    """Information an encoding can preserve from an object-centric log."""

    schema: bool
    unused_types: bool
    object_changes: bool
    e2o: bool
    o2o: bool
    multiple_object_types: bool
    requires_case_notion: bool = False

    @property
    def lossless_ocel(self) -> bool:
        return (
            all(
                (
                    self.schema,
                    self.unused_types,
                    self.object_changes,
                    self.e2o,
                    self.o2o,
                    self.multiple_object_types,
                )
            )
            and not self.requires_case_notion
        )


_LOSSLESS = CodecCapabilities(
    schema=True,
    unused_types=True,
    object_changes=True,
    e2o=True,
    o2o=True,
    multiple_object_types=True,
)

XES_CAPABILITIES = CodecCapabilities(
    schema=False,
    unused_types=False,
    object_changes=False,
    e2o=False,
    o2o=False,
    multiple_object_types=False,
    requires_case_notion=True,
)

Reader = Callable[[Path, ValidationMode], OCEL]
Writer = Callable[[OCEL, Path, bool, ValidationMode], None]
Importer = Callable[[Path, Path, bool, ValidationMode], None]


@dataclass(frozen=True)
class ExchangeCodec:
    """A registered lossless OCEL exchange encoding."""

    name: ExchangeFormat
    extensions: tuple[str, ...]
    capabilities: CodecCapabilities
    _reader: Reader | None
    _writer: Writer
    _importer: Importer | None = None

    def read(self, path: Path, *, validation: ValidationMode) -> OCEL:
        if self._reader is None:
            raise NotImplementedError(
                f"{self.name} has no one-off reader; import it into native storage."
            )
        return self._reader(path, validation)

    def write(
        self,
        ocel: OCEL,
        path: Path,
        *,
        overwrite: bool,
        validation: ValidationMode,
    ) -> None:
        self._writer(ocel, path, overwrite, validation)

    @property
    def direct_import(self) -> bool:
        """Whether this codec can stream directly into native storage."""
        return self._importer is not None

    @property
    def supports_read(self) -> bool:
        """Whether this codec supports one-off in-memory reads."""
        return self._reader is not None

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
            self._importer(source, target, overwrite, validation)
            return
        self.read(source, validation=validation).write(target, overwrite=overwrite)


def _read_json(path: Path, validation: ValidationMode) -> OCEL:
    return read_json(path, validation=validation)


def _read_xml(path: Path, validation: ValidationMode) -> OCEL:
    return read_xml(path, validation=validation)


def _write_json(
    ocel: OCEL, path: Path, overwrite: bool, validation: ValidationMode
) -> None:
    write_json(ocel, path, overwrite=overwrite, validation=validation)


def _write_xml(
    ocel: OCEL, path: Path, overwrite: bool, validation: ValidationMode
) -> None:
    write_xml(ocel, path, overwrite=overwrite, validation=validation)


def _write_sqlite(
    ocel: OCEL, path: Path, overwrite: bool, validation: ValidationMode
) -> None:
    write_sqlite(ocel, path, overwrite=overwrite, validation=validation)


def _import_sqlite(
    source: Path, target: Path, overwrite: bool, validation: ValidationMode
) -> None:
    import_sqlite(
        source,
        target,
        overwrite=overwrite,
        validation=validation,
    )


def _import_json(
    source: Path, target: Path, overwrite: bool, validation: ValidationMode
) -> None:
    import_json(
        source,
        target,
        overwrite=overwrite,
        validation=validation,
    )


def _import_xml(
    source: Path, target: Path, overwrite: bool, validation: ValidationMode
) -> None:
    import_xml(
        source,
        target,
        overwrite=overwrite,
        validation=validation,
    )


_CODECS: Mapping[ExchangeFormat, ExchangeCodec] = MappingProxyType(
    {
        "json": ExchangeCodec(
            "json",
            (".json", ".jsonocel"),
            _LOSSLESS,
            _read_json,
            _write_json,
            _import_json,
        ),
        "xml": ExchangeCodec(
            "xml",
            (".xml", ".xmlocel"),
            _LOSSLESS,
            _read_xml,
            _write_xml,
            _import_xml,
        ),
        "sqlite": ExchangeCodec(
            "sqlite",
            (".sqlite", ".sqlite3", ".db"),
            _LOSSLESS,
            None,
            _write_sqlite,
            _import_sqlite,
        ),
    }
)


def exchange_codecs() -> Mapping[ExchangeFormat, ExchangeCodec]:
    """Return the immutable built-in codec registry."""
    return _CODECS


def get_codec(format: ExchangeFormat) -> ExchangeCodec:
    """Return a codec by its explicit exchange-format name."""
    return _CODECS[format]


def detect_codec(path: str | Path) -> ExchangeCodec:
    """Select an exchange codec from a file extension."""
    suffix = Path(path).suffix.lower()
    for codec in _CODECS.values():
        if suffix in codec.extensions:
            return codec
    raise ValueError(
        f"Cannot infer OCEL exchange format from {path}. Pass format explicitly."
    )
