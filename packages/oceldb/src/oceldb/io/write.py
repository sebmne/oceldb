"""Write OCEL datasets to canonical directories or packaged archives."""

from __future__ import annotations

import shutil
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory

from oceldb.core.ocel import OCEL
from oceldb.io._manifest import LOGICAL_TABLES, MANIFEST_FILE, write_manifest
from oceldb.io._paths import normalize_output_path


def write_ocel(
    ocel: OCEL,
    target: str | Path,
    *,
    overwrite: bool = False,
    packaged: bool = False,
) -> Path:
    """
    Persist an OCEL to the canonical directory format or a packaged archive.
    """
    target_path = normalize_output_path(
        Path(target).expanduser().resolve(),
        packaged=packaged,
    )

    if target_path == ocel.path:
        raise ValueError("Target path must differ from the source OCEL path")

    if target_path.exists():
        if not overwrite:
            raise FileExistsError(
                f"Target already exists: {target_path} (use overwrite=True)"
            )
        _remove_target(target_path)

    if packaged:
        with TemporaryDirectory(prefix="oceldb_write_") as tmpdir:
            staging_dir = Path(tmpdir) / "dataset"
            _write_directory(
                ocel,
                staging_dir,
                packaging="archive",
            )
            _write_archive(staging_dir, target_path)
    else:
        _write_directory(
            ocel,
            target_path,
            packaging="directory",
        )

    return target_path


def _write_directory(ocel: OCEL, target_dir: Path, *, packaging: str) -> None:
    try:
        target_dir.mkdir(parents=True, exist_ok=False)

        for table_name in LOGICAL_TABLES:
            _copy_table(ocel, table_name, target_dir / f"{table_name}.parquet")

        write_manifest(
            target_dir / MANIFEST_FILE,
            ocel.manifest.with_packaging(packaging),
        )
    except Exception:
        if target_dir.exists():
            _remove_target(target_dir)
        raise


def _copy_table(ocel: OCEL, table_name: str, target_file: Path) -> None:
    escaped_target = str(target_file).replace("'", "''")
    ocel._con.execute(f"""
        COPY (
            SELECT *
            FROM "{ocel.schema}"."{table_name}"
        ) TO '{escaped_target}' (FORMAT PARQUET)
    """)


def _write_archive(source_dir: Path, target_file: Path) -> None:
    target_file.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(target_file, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for child in sorted(source_dir.iterdir()):
            archive.write(child, arcname=child.name)


def _remove_target(target: Path) -> None:
    if target.is_dir():
        shutil.rmtree(target)
    else:
        target.unlink()
