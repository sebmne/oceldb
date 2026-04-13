from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

from oceldb.core.manifest import OCELManifest
from oceldb.core.ocel import OCEL
from oceldb.io._manifest import LOGICAL_TABLES, MANIFEST_FILE, load_manifest


def read_ocel(path: str | Path) -> OCEL:
    """
    Open an OCEL dataset stored in the canonical directory layout.
    """
    source_path = Path(path).expanduser().resolve()

    if not source_path.exists():
        raise FileNotFoundError(f"OCEL source does not exist: {source_path}")

    if not source_path.is_dir():
        raise ValueError(
            f"OCEL source must be a directory in canonical oceldb layout: {source_path}"
        )

    return _open_ocel_directory(source_path)


def _validate_directory_layout(path: Path) -> None:
    required = [MANIFEST_FILE, *(f"{table_name}.parquet" for table_name in LOGICAL_TABLES)]
    missing = [name for name in required if not (path / name).exists()]
    if missing:
        raise FileNotFoundError(
            f"OCEL source '{path}' is missing required files: {', '.join(missing)}"
        )


def _validate_loaded_tables(
    con: duckdb.DuckDBPyConnection,
    manifest: OCELManifest,
) -> None:
    for table_name in LOGICAL_TABLES:
        rows = con.execute(f'DESCRIBE "{table_name}"').fetchall()
        actual_columns = {row[0] for row in rows}
        expected_columns = set(manifest.table(table_name).columns)
        missing = expected_columns - actual_columns

        if missing:
            raise ValueError(
                f'Table "{table_name}" is missing required columns: '
                f"{', '.join(sorted(missing))}"
            )


def _open_ocel_directory(
    data_path: Path,
    *,
    source_path: Path | None = None,
    tempdir: TemporaryDirectory[str] | None = None,
) -> OCEL:
    manifest_path = data_path / MANIFEST_FILE
    if manifest_path.exists():
        manifest = load_manifest(manifest_path)
    else:
        _validate_directory_layout(data_path)
        manifest = load_manifest(manifest_path)

    _validate_directory_layout(data_path)
    source = data_path if source_path is None else source_path

    con = duckdb.connect()

    try:
        for table_name in LOGICAL_TABLES:
            parquet_path = str(data_path / f"{table_name}.parquet").replace("'", "''")
            con.execute(f"""
                CREATE VIEW "{table_name}" AS
                SELECT *
                FROM read_parquet('{parquet_path}')
            """)

        _validate_loaded_tables(con, manifest)
    except Exception:
        con.close()
        if tempdir is not None:
            tempdir.cleanup()
        raise

    return OCEL(
        path=source,
        con=con,
        manifest=manifest,
        tempdir=tempdir,
    )
