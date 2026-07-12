"""Typed containers for the canonical lazy OCEL dataset."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from collections.abc import Mapping
from typing import Any

import polars as pl

from oceldb.schema import OCELSchema


@dataclass(frozen=True)
class OCELFrames:
    """The five logical OCEL tables as lazy Polars frames."""

    events: pl.LazyFrame
    objects: pl.LazyFrame
    object_changes: pl.LazyFrame
    event_object: pl.LazyFrame
    object_object: pl.LazyFrame


@dataclass(frozen=True)
class OCELDataset:
    """Canonical frames plus declared schema and native dataset metadata."""

    frames: OCELFrames
    schema: OCELSchema | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))
