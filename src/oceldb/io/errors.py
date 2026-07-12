"""Exceptions and validation modes shared by OCEL format adapters."""

from __future__ import annotations

import warnings
from typing import Literal

ValidationMode = Literal["strict", "warn", "none"]


class OCELIOError(ValueError):
    """Raised when an exchange-format document violates its contract."""


class OCELIOWarning(UserWarning):
    """Warning emitted for recoverable input problems in ``warn`` mode."""


def issue(mode: ValidationMode, message: str) -> None:
    """Raise, warn, or ignore an interoperability problem."""
    if mode == "strict":
        raise OCELIOError(message)
    if mode == "warn":
        warnings.warn(message, OCELIOWarning, stacklevel=3)


def check_validation_mode(mode: str) -> ValidationMode:
    if mode not in {"strict", "warn", "none"}:
        raise ValueError("validation must be 'strict', 'warn', or 'none'.")
    return mode  # type: ignore[return-value]
