"""Lazy query-building helpers shared by OCEL accessors and operations."""

from collections.abc import Collection

import polars as pl

from oceldb.core import schema as s
from oceldb.errors import OCELValidationError
from oceldb.types import OneOrMany, normalize_strings


def select_types(
    frame: pl.LazyFrame,
    types: tuple[str, ...],
    schema: dict[str, pl.DataType],
    attributes: dict[str, list[str]] | None,
) -> pl.LazyFrame:
    """Apply a type filter and, when known, narrow attribute columns."""
    normalized = normalize_strings(types, name="types")
    if not normalized:
        return frame
    selected = frame.filter(pl.col(s.OCEL_TYPE).is_in(normalized))
    if attributes is None:
        return selected
    core = (name for name in schema if name != s.OCEL_TYPE)
    kept = list(
        dict.fromkeys(
            name for type_name in normalized for name in attributes.get(type_name, ())
        )
    )
    return selected.select(*core, *kept, s.OCEL_TYPE)


def filter_relation(
    frame: pl.LazyFrame,
    filters: tuple[tuple[str, OneOrMany[str] | None], ...],
) -> pl.LazyFrame:
    """Apply scalar-or-membership predicates without evaluating rows."""
    result = frame
    for column, value in filters:
        values = normalize_strings(value, name=f"{column} filter")
        if values is None:
            continue
        predicate = (
            pl.col(column) == values[0]
            if len(values) == 1
            else pl.col(column).is_in(values)
        )
        result = result.filter(predicate)
    return result


def filter_identifier(
    frame: pl.LazyFrame,
    column: str,
    values: OneOrMany[str] | None,
) -> pl.LazyFrame:
    """Apply an identifier selector without evaluating rows."""
    return filter_relation(frame, ((column, values),))


def distinct_types(frame: pl.LazyFrame) -> list[str]:
    """Collect distinct, non-empty type names using streaming execution."""
    values = (
        frame.select(s.OCEL_TYPE)
        .unique()
        .sort(s.OCEL_TYPE)
        .collect(engine="streaming")
        .get_column(s.OCEL_TYPE)
        .to_list()
    )
    if not all(isinstance(value, str) and value for value in values):
        raise OCELValidationError(
            "Invalid OCEL: type names must be non-null, non-empty strings."
        )
    return values


def require_known_type(
    value: object,
    *,
    known: Collection[str],
    name: str,
) -> None:
    """Require one non-empty known event or object type."""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string.")
    if not value:
        raise ValueError(f"{name} must not be empty.")
    if value not in known:
        raise ValueError(f"Unknown {name.replace('_', ' ')} {value!r}.")
