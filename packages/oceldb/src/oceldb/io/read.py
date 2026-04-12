from __future__ import annotations

import uuid
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

from oceldb.core.ocel import OCEL
from oceldb.io._manifest import LOGICAL_TABLES, MANIFEST_FILE, load_manifest


def read_ocel(path: str | Path) -> OCEL:
    """
    Read an OCEL stored as either a canonical directory or a packaged archive.
    """
    source_path = Path(path).expanduser().resolve()

    if not source_path.exists():
        raise FileNotFoundError(f"OCEL source does not exist: {source_path}")

    tempdir: TemporaryDirectory[str] | None = None

    if source_path.is_dir():
        data_path = source_path
        packaging = "directory"
    elif source_path.is_file():
        if not zipfile.is_zipfile(source_path):
            raise ValueError(f"Expected a valid .oceldb archive, got: {source_path}")
        tempdir = TemporaryDirectory(prefix="oceldb_")
        data_path = Path(tempdir.name)
        with zipfile.ZipFile(source_path) as archive:
            archive.extractall(data_path)
        packaging = "archive"
    else:
        raise ValueError(f"Unsupported OCEL source: {source_path}")

    _validate_directory_layout(data_path)

    manifest = load_manifest(data_path / MANIFEST_FILE).with_packaging(packaging)

    con = duckdb.connect()
    schema = f"ocel_{uuid.uuid4().hex[:8]}"
    con.execute(f'CREATE SCHEMA "{schema}"')

    for table_name in LOGICAL_TABLES:
        parquet_path = str(data_path / f"{table_name}.parquet").replace("'", "''")
        con.execute(f"""
            CREATE VIEW "{schema}"."{table_name}" AS
            SELECT *
            FROM read_parquet('{parquet_path}')
        """)

    _validate_loaded_tables(con, schema, manifest)

    return OCEL(
        path=source_path,
        data_path=data_path,
        con=con,
        manifest=manifest,
        schema=schema,
        tempdir=tempdir,
    )


def _validate_directory_layout(path: Path) -> None:
    required = [MANIFEST_FILE, *(f"{table_name}.parquet" for table_name in LOGICAL_TABLES)]
    missing = [name for name in required if not (path / name).exists()]
    if missing:
        raise FileNotFoundError(
            f"OCEL source '{path}' is missing required files: {', '.join(missing)}"
        )


def _validate_loaded_tables(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    manifest,
) -> None:
    for table_name in LOGICAL_TABLES:
        rows = con.execute(f'DESCRIBE "{schema}"."{table_name}"').fetchall()
        actual_columns = {row[0] for row in rows}
        expected_columns = set(manifest.table(table_name).columns)
        missing = expected_columns - actual_columns

        if missing:
            raise ValueError(
                f'Table "{schema}.{table_name}" is missing required columns: '
                f"{', '.join(sorted(missing))}"
            )
