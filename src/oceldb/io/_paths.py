"""Transactional destination handling shared by format writers."""

from contextlib import contextmanager
from dataclasses import dataclass, field
import shutil
from pathlib import Path
from typing import Generator
from uuid import uuid4


@contextmanager
def atomic_file(path: str | Path, *, overwrite: bool) -> Generator[tuple[Path, Path]]:
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


@dataclass
class DirectoryTransaction:
    """Stage a directory and install it with rollback-safe overwrite semantics."""

    target: Path
    overwrite: bool
    staging: Path = field(init=False)
    _active: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        self.target = Path(self.target)
        self.staging = self.target.with_name(f".{self.target.name}.tmp-{uuid4().hex}")

    def __enter__(self) -> Path:
        if _exists(self.target) and not self.overwrite:
            raise FileExistsError(
                f"Target already exists: {self.target}. "
                "Pass overwrite=True to replace it."
            )
        self.target.parent.mkdir(parents=True, exist_ok=True)
        self.staging.mkdir()
        self._active = True
        return self.staging

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if exc_type is None:
            self.commit()
        else:
            self.abort()

    def commit(self) -> None:
        """Install the staged directory, restoring the old target on failure."""
        if not self._active:
            raise RuntimeError("Directory transaction is not active.")

        backup: Path | None = None
        try:
            if _exists(self.target):
                if not self.overwrite:
                    raise FileExistsError(
                        f"Target already exists: {self.target}. "
                        "Pass overwrite=True to replace it."
                    )
                backup = self.target.with_name(
                    f".{self.target.name}.backup-{uuid4().hex}"
                )
                self.target.rename(backup)
            try:
                self.staging.rename(self.target)
            except BaseException as install_error:
                if backup is not None:
                    try:
                        backup.rename(self.target)
                    except BaseException as restore_error:
                        raise BaseExceptionGroup(
                            f"Failed to install and restore {self.target}.",
                            [install_error, restore_error],
                        ) from install_error
                raise
        except BaseException:
            self.abort()
            raise

        self._active = False
        if backup is not None:
            _remove(backup)

    def abort(self) -> None:
        """Discard staged output without touching the current target."""
        _remove(self.staging)
        self._active = False


def _exists(path: Path) -> bool:
    return path.exists() or path.is_symlink()


def _remove(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        path.unlink(missing_ok=True)
