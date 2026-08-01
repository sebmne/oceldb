"""Lazy-frame normalization and composition helpers."""

import polars as pl

from oceldb.types import FrameLike


def as_lazy(frame: FrameLike) -> pl.LazyFrame:
    """Convert an eager frame to lazy form without evaluating a lazy input."""
    if isinstance(frame, pl.DataFrame):
        return frame.lazy()
    return frame


def concat_table(
    current: pl.LazyFrame,
    addition: pl.LazyFrame | None,
    *,
    allow_attributes: bool = False,
    schema: dict[str, pl.DataType] | None = None,
) -> pl.LazyFrame:
    """Concatenate a validated table addition without evaluating rows."""
    if addition is None:
        return current
    if allow_attributes:
        return pl.concat((current, addition), how="diagonal")
    assert schema is not None
    columns = tuple(schema)
    return pl.concat(
        (current.select(*columns), addition.select(*columns)),
        how="vertical",
    )
