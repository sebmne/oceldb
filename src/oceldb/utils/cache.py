"""Cache directory helpers for converted input files."""

import hashlib
import os
from pathlib import Path


def conversion_cache_dir(source: str | Path, *, name: str) -> Path:
    """Return the cache directory for a converted source file.

    Args:
        source: Source file whose converted representation is cached.
        name: Stable cache namespace, usually the public reader name such as
            ``"read_sqlite"``.

    Returns:
        A directory path under the user's OS cache directory. The directory name
        is keyed by the source's absolute path, file size, and modification
        time. The parent cache root is created if needed; the returned directory
        itself is not created.
    """
    source_path = Path(source)
    return _cache_root() / f"{name}_{_source_key(source_path)}"


def _source_key(source: Path) -> str:
    stat = source.stat()
    payload = f"{source.resolve()}_{stat.st_size}_{stat.st_mtime}"
    return hashlib.md5(payload.encode()).hexdigest()


def _cache_root() -> Path:
    if os.name == "nt":
        base = Path(os.environ.get("LOCALAPPDATA", "~")).expanduser()
    else:
        base = Path(os.environ.get("XDG_CACHE_HOME", "~/.cache")).expanduser()
    cache_dir = base / "oceldb"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir
