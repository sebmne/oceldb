from __future__ import annotations

from pathlib import Path


def normalize_output_path(target: Path, *, packaged: bool) -> Path:
    if not packaged or target.suffix == ".oceldb":
        return target
    return target.with_name(f"{target.name}.oceldb")
