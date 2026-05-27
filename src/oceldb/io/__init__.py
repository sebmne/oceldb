"""OCEL input adapters and persisted output helpers."""

from __future__ import annotations

from oceldb.io.convert import convert_ocel
from oceldb.io.registry import ConverterSpec, register, unregister
from oceldb.io.source import ArrowSource, Canonical, Source

from oceldb.io import converters as _converters  # noqa: F401  # pyright: ignore[reportUnusedImport]

__all__ = [
    "convert_ocel",
    "register",
    "unregister",
    "ConverterSpec",
    "Source",
    "ArrowSource",
    "Canonical",
]
