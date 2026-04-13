"""Write OCEL datasets to the canonical directory layout."""

from __future__ import annotations

import shutil
from pathlib import Path

from oceldb.core.ocel import OCEL
from oceldb.io._manifest import (
    LOGICAL_TABLES,
    MANIFEST_FILE,
    build_manifest_from_tables,
    write_manifest,
)


def write_ocel(
    ocel: OCEL,
    target: str | Path,
    *,
    overwrite: bool = False,
) -> Path:
    """
    Persist an OCEL dataset to the canonical directory layout.

    Args:
        ocel: Dataset handle to persist.
        target: Target directory.
        overwrite: Replace an existing target directory.

    Returns:
        The resolved target directory path.
    """
    target_path = Path(target).expanduser().resolve()

    if target_path == ocel.path:
        raise ValueError("Target path must differ from the source OCEL path")

    if target_path.exists():
        if not overwrite:
            raise FileExistsError(
                f"Target already exists: {target_path} (use overwrite=True)"
            )
        _remove_target(target_path)

    _write_directory(ocel, target_path)

    return target_path


def _write_directory(ocel: OCEL, target_dir: Path) -> None:
    try:
        target_dir.mkdir(parents=True, exist_ok=False)

        for table_name in LOGICAL_TABLES:
            _copy_table(ocel, table_name, target_dir / f"{table_name}.parquet")

        manifest = build_manifest_from_tables(
            ocel._con,
            oceldb_version=ocel.manifest.oceldb_version,
            source=ocel.manifest.source,
            created_at=ocel.manifest.created_at,
            drop_empty_custom_columns=False,
        )
        write_manifest(target_dir / MANIFEST_FILE, manifest)
    except Exception:
        if target_dir.exists():
            _remove_target(target_dir)
        raise


def _copy_table(ocel: OCEL, table_name: str, target_file: Path) -> None:
    escaped_target = str(target_file).replace("'", "''")
    ocel._con.execute(f"""
        COPY (
            SELECT *
            FROM "{table_name}"
        ) TO '{escaped_target}' (FORMAT PARQUET)
    """)


def _remove_target(target: Path) -> None:
    if target.is_dir():
        shutil.rmtree(target)
    else:
        target.unlink()
