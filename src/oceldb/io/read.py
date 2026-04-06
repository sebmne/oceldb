from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path

import duckdb

from oceldb.core.metadata import OCELMetadata
from oceldb.core.ocel import OCEL


def read_ocel(path: str | Path) -> OCEL:
    """
    Read an OCEL stored in the oceldb directory format.

    Expected directory contents:
        - event.parquet
        - object.parquet
        - event_object.parquet
        - object_object.parquet
        - event_type.parquet
        - object_type.parquet
        - metadata.json
    """
    path = Path(path).expanduser().resolve()

    if not path.exists():
        raise FileNotFoundError(f"OCEL directory does not exist: {path}")

    if not path.is_dir():
        raise NotADirectoryError(f"Expected an OCEL directory, got: {path}")

    required_files = [
        "event.parquet",
        "object.parquet",
        "event_object.parquet",
        "object_object.parquet",
        "event_type.parquet",
        "object_type.parquet",
        "metadata.json",
    ]

    missing = [name for name in required_files if not (path / name).exists()]
    if missing:
        raise FileNotFoundError(
            f"OCEL directory '{path}' is missing required files: {', '.join(missing)}"
        )

    metadata_path = path / "metadata.json"
    try:
        raw = json.loads(metadata_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid metadata.json in '{path}': {e}") from e

    required_metadata_keys = {"oceldb_version", "source", "converted_at"}
    missing_keys = required_metadata_keys - raw.keys()
    if missing_keys:
        raise ValueError(
            f"metadata.json in '{path}' is missing required keys: "
            f"{', '.join(sorted(missing_keys))}"
        )

    try:
        converted_at = datetime.fromisoformat(raw["converted_at"])
    except Exception as e:
        raise ValueError(
            f"Invalid converted_at value in metadata.json: {raw['converted_at']!r}"
        ) from e

    metadata = OCELMetadata(
        oceldb_version=str(raw["oceldb_version"]),
        source=str(raw["source"]),
        converted_at=converted_at,
    )

    con = duckdb.connect()
    schema = f"ocel_{uuid.uuid4().hex[:8]}"

    con.execute(f"CREATE SCHEMA {schema}")

    con.execute(f"""
        CREATE VIEW {schema}.event AS
        SELECT * FROM read_parquet('{str(path / "event.parquet").replace("'", "''")}')
    """)

    con.execute(f"""
        CREATE VIEW {schema}.object AS
        SELECT * FROM read_parquet('{str(path / "object.parquet").replace("'", "''")}')
    """)

    con.execute(f"""
        CREATE VIEW {schema}.event_object AS
        SELECT * FROM read_parquet('{str(path / "event_object.parquet").replace("'", "''")}')
    """)

    con.execute(f"""
        CREATE VIEW {schema}.object_object AS
        SELECT * FROM read_parquet('{str(path / "object_object.parquet").replace("'", "''")}')
    """)

    con.execute(f"""
        CREATE VIEW {schema}.event_type AS
        SELECT * FROM read_parquet('{str(path / "event_type.parquet").replace("'", "''")}')
    """)

    con.execute(f"""
        CREATE VIEW {schema}.object_type AS
        SELECT * FROM read_parquet('{str(path / "object_type.parquet").replace("'", "''")}')
    """)

    _validate_ocel_tables(con, schema)

    return OCEL(
        path=path,
        con=con,
        metadata=metadata,
        schema=schema,
    )


def _validate_ocel_tables(con: duckdb.DuckDBPyConnection, schema: str) -> None:
    expected = {
        "event": {"ocel_id", "ocel_type", "ocel_time", "attributes"},
        "object": {
            "ocel_id",
            "ocel_type",
            "ocel_time",
            "ocel_changed_field",
            "attributes",
        },
        "event_object": {"ocel_event_id", "ocel_object_id"},
        "object_object": {"ocel_source_id", "ocel_target_id"},
        "event_type": {"ocel_type"},
        "object_type": {"ocel_type"},
    }

    for table_name, required_columns in expected.items():
        rows = con.execute(f"DESCRIBE {schema}.{table_name}").fetchall()
        actual_columns = {row[0] for row in rows}
        missing = required_columns - actual_columns

        if missing:
            raise ValueError(
                f"Table '{schema}.{table_name}' is missing required columns: "
                f"{', '.join(sorted(missing))}"
            )
