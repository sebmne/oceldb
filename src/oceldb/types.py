"""Public reusable type aliases for the oceldb API."""

from collections.abc import Iterable
from datetime import date, datetime
from os import PathLike
from pathlib import Path
from typing import TypeAlias, TypeVar, overload

import polars as pl

_T = TypeVar("_T")

FrameLike: TypeAlias = pl.DataFrame | pl.LazyFrame
OneOrMany: TypeAlias = _T | Iterable[_T]
PathLikeStr: TypeAlias = str | PathLike[str] | Path
TimeLike: TypeAlias = str | date | datetime


@overload
def normalize_strings(
    value: None,
    *,
    name: str = "value",
    non_empty: bool = False,
) -> None: ...


@overload
def normalize_strings(
    value: OneOrMany[str],
    *,
    name: str = "value",
    non_empty: bool = False,
) -> list[str]: ...


def normalize_strings(
    value: object,
    *,
    name: str = "value",
    non_empty: bool = False,
) -> list[str] | None:
    """Normalize a scalar-or-iterable string selector.

    Args:
        value: A string, iterable of strings, or ``None``.
        name: Parameter name used in error messages.
        non_empty: Whether a provided selector must contain at least one
            value. ``None`` remains valid and means that the selector was not
            provided.

    Returns:
        A deduplicated list preserving input order, or ``None``.

    Raises:
        TypeError: If the selector or one of its values is not a string.
        ValueError: If a value is empty, or if ``non_empty=True`` and a
            provided iterable is empty.

    """
    if value is None:
        return None
    if isinstance(value, str):
        values = [value]
    else:
        if not isinstance(value, Iterable):
            raise TypeError(f"{name} must be a string, iterable of strings, or None.")
        values = list(value)
    if not all(isinstance(item, str) for item in values):
        raise TypeError(f"{name} must contain only strings.")
    if any(not item for item in values):
        raise ValueError(f"{name} values must not be empty.")
    if non_empty and not values:
        raise ValueError(f"{name} must contain at least one value.")
    return list(dict.fromkeys(values))


__all__ = [
    "FrameLike",
    "OneOrMany",
    "PathLikeStr",
    "TimeLike",
    "normalize_strings",
]
