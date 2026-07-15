"""Exceptions and validation modes shared by OCEL format adapters."""

import warnings
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Literal

from oceldb.errors import OCELDBError, OCELIOError

ValidationMode = Literal["strict", "warn", "none"]


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


@contextmanager
def io_operation(action: str) -> Generator[None]:
    """Translate unexpected adapter failures into a contextual IO error.

    Existing oceldb errors, standard filesystem errors, and missing optional
    dependencies retain their concrete public types.
    """
    try:
        yield
    except (OCELDBError, OSError, ImportError):
        raise
    except Exception as exc:
        raise OCELIOError(f"{action}: {exc}") from exc


@contextmanager
def io_boundary(action: str, path: str | Path) -> Generator[None]:
    """Apply :func:`io_operation` to a path-based operation."""
    with io_operation(f"Cannot {action} {Path(path)}"):
        yield
