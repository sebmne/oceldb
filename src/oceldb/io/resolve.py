"""Resolve user inputs into registered OCEL source adapters."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from oceldb.io.registry import get, infer_format
from oceldb.io.source import Source


@dataclass(frozen=True)
class ResolvedSource:
    source: Source
    kind: str
    path: Path | None


def resolve_source(
    source: object,
    *,
    format: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> ResolvedSource:
    kind = format or infer_format(source)
    if kind is None:
        raise ValueError(
            "Cannot infer OCEL input format from source. Pass format= explicitly."
        )

    spec = get(kind)
    adapter = spec.source_factory(source)
    path = Path(source) if isinstance(source, (str, Path)) else None

    if progress is not None:
        progress(f"Reading {kind.upper()}")

    return ResolvedSource(source=adapter, kind=kind, path=path)
