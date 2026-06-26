"""Cache file conversions into oceldb's native directory layout.

A converter is any ``(source_path, target_dir) -> None`` function that writes a
complete oceldb Parquet directory to ``target_dir``. :func:`cached_conversion`
turns that converter into a reader that accepts only the source path, stores the
conversion under the user's OS cache directory, and returns an :class:`OCEL`.
"""

import functools
import hashlib
import os
from collections.abc import Callable
from pathlib import Path

from oceldb.ocel import OCEL

Converter = Callable[[Path, Path], None]


def cached_conversion(convert: Converter) -> Callable[[str | Path], OCEL]:
    """Wrap a converter in an ``OCEL`` reader with persistent caching.

    Args:
        convert: Function that writes a native oceldb directory from
            ``source_path`` to ``target_dir``. The function must fully populate
            ``target_dir`` or raise an exception.

    Returns:
        A reader function that accepts ``str | Path`` and returns an ``OCEL``.
        The reader converts the source only when no cache entry exists for the
        current source path, size, and modification time.

    Raises:
        FileNotFoundError: If the source path passed to the returned reader does
            not exist.

    Notes:
        Cache entries are intentionally content-adjacent rather than
        content-addressed: changing a file in place creates a new key through
        size or mtime, while repeated reads of the same unchanged file reuse the
        existing converted directory.
    """
    name: str = getattr(convert, "__name__", "convert")

    @functools.wraps(convert)
    def read(source: str | Path) -> OCEL:
        source_path = Path(source)
        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")
        target_dir = _cache_dir() / f"{name}_{_source_key(source_path)}"
        if not target_dir.exists():
            convert(source_path, target_dir)
        return OCEL.read(target_dir)

    return read


def _source_key(source: Path) -> str:
    stat = source.stat()
    payload = f"{source.resolve()}_{stat.st_size}_{stat.st_mtime}"
    return hashlib.md5(payload.encode()).hexdigest()


def _cache_dir() -> Path:
    if os.name == "nt":
        base = Path(os.environ.get("LOCALAPPDATA", "~")).expanduser()
    else:
        base = Path(os.environ.get("XDG_CACHE_HOME", "~/.cache")).expanduser()
    cache_dir = base / "oceldb"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir
