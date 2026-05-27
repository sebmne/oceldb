"""Converter registry."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from threading import Lock
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from oceldb.io.source import Source


@dataclass(frozen=True)
class ConverterSpec:
    format: str
    source_factory: Callable[[Any], "Source"]
    extensions: tuple[str, ...] = ()


_specs: dict[str, ConverterSpec] = {}
_lock = Lock()
_entry_points_loaded = False
_ENTRY_POINT_GROUP = "oceldb.converters"


def register(spec: ConverterSpec) -> None:
    with _lock:
        _specs[spec.format] = spec


def unregister(format: str) -> None:
    """Remove a registered format. No-op if not registered."""
    with _lock:
        _specs.pop(format, None)


def get(format: str) -> ConverterSpec:
    """Return the spec for *format*, loading entry points on first lookup."""
    _ensure_entry_points_loaded()
    try:
        return _specs[format]
    except KeyError:
        supported = ", ".join(sorted(_specs)) or "<none>"
        raise ValueError(
            f"Unsupported OCEL format {format!r}; expected one of {supported}."
        ) from None


def formats() -> list[str]:
    """Return all registered format names in sorted order."""
    _ensure_entry_points_loaded()
    return sorted(_specs)


def infer_format(source: object) -> str | None:
    """Infer a registered format from a path-like source."""
    if not isinstance(source, (str, Path)):
        return None
    suffix = Path(source).suffix.casefold()
    if not suffix:
        return None
    _ensure_entry_points_loaded()
    for spec in _specs.values():
        if suffix in spec.extensions:
            return spec.format
    return None


def _ensure_entry_points_loaded() -> None:
    global _entry_points_loaded
    if _entry_points_loaded:
        return
    with _lock:
        if _entry_points_loaded:
            return
        for ep in _select_entry_points(_ENTRY_POINT_GROUP):
            try:
                spec = ep.load()
            except Exception:  # pragma: no cover - third-party failure
                continue
            if isinstance(spec, ConverterSpec):
                _specs.setdefault(spec.format, spec)
        _entry_points_loaded = True


def _select_entry_points(group: str) -> Iterable[metadata.EntryPoint]:
    return metadata.entry_points().select(group=group)
