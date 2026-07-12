"""Atomic destination handling shared by format writers."""

from __future__ import annotations

from contextlib import contextmanager
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4


@contextmanager
def atomic_file(path: str | Path, *, overwrite: bool) -> Iterator[tuple[Path, Path]]:
    """Yield ``(target, staging)`` and atomically replace the target on success."""
    target = Path(path)
    if target.exists() and not overwrite:
        raise FileExistsError(
            f"File already exists: {target}. Pass overwrite=True to replace it."
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = target.with_name(f".{target.name}.tmp-{uuid4().hex}")
    try:
        yield target, staging
        staging.replace(target)
    except BaseException:
        staging.unlink(missing_ok=True)
        raise
