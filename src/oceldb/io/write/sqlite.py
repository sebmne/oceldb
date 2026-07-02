"""write_sqlite: export an OCEL to the OCEL 2.0 SQLite format."""

import re
import sqlite3
from pathlib import Path

import polars as pl

from oceldb import schema as s
from oceldb.ocel import OCEL

_EVENT_FIXED = {s.OCEL_ID, s.OCEL_TIME, s.OCEL_TYPE}
_OC_FIXED = {s.OCEL_ID, s.OCEL_TIME, s.OCEL_TYPE, s.OCEL_CHANGED_FIELD}


def write_sqlite(ocel: OCEL, path: str | Path, *, overwrite: bool = False) -> None:
    """Write an :class:`~oceldb.OCEL` to an OCEL 2.0 SQLite file.

    Produces the standard OCEL 2.0 SQLite schema: ``event`` / ``object``
    master tables, ``event_map_type`` / ``object_map_type`` type-to-suffix
    mappings, per-type ``event_<suffix>`` / ``object_<suffix>`` attribute
    tables, and ``event_object`` / ``object_object`` relation tables. The
    output can be read back with :func:`~oceldb.io.read_sqlite` or any
    other OCEL 2.0-compatible tool such as pm4py.

    Args:
        ocel: The log to export.
        path: Destination ``.sqlite`` file path.
        overwrite: Replace an existing file when ``True``. The default
            raises :class:`FileExistsError`.

    Raises:
        FileExistsError: If ``path`` exists and ``overwrite`` is ``False``.

    Examples:
        >>> from oceldb.io import write_sqlite
        >>> write_sqlite(ocel, "output.sqlite")
        >>> write_sqlite(ocel >> view(object_types="order"), "orders.sqlite", overwrite=True)
    """
    path = Path(path)
    if path.exists() and not overwrite:
        raise FileExistsError(
            f"File already exists: {path}. Pass overwrite=True to replace it."
        )
    if path.exists():
        path.unlink()

    events = ocel.events().collect()
    objects = ocel.objects().collect()
    oc = ocel.object_changes().collect()
    e2o = ocel.event_object().collect()
    o2o = ocel.object_object().collect()

    con = sqlite3.connect(path)
    try:
        _create_schema(con)
        _write_events(con, events)
        _write_objects(con, objects, oc)
        _write_e2o(con, e2o)
        if len(o2o) > 0:
            _write_o2o(con, o2o)
        con.commit()
    except BaseException:
        con.close()
        path.unlink(missing_ok=True)
        raise
    finally:
        con.close()


def _create_schema(con: sqlite3.Connection) -> None:
    con.executescript("""
        CREATE TABLE event (ocel_id TEXT PRIMARY KEY, ocel_type TEXT);
        CREATE TABLE object (ocel_id TEXT PRIMARY KEY, ocel_type TEXT);
        CREATE TABLE event_map_type (ocel_type TEXT PRIMARY KEY, ocel_type_map TEXT);
        CREATE TABLE object_map_type (ocel_type TEXT PRIMARY KEY, ocel_type_map TEXT);
        CREATE TABLE event_object (
            ocel_event_id TEXT, ocel_object_id TEXT, ocel_qualifier TEXT
        );
        CREATE TABLE object_object (
            ocel_source_id TEXT, ocel_target_id TEXT, ocel_qualifier TEXT
        );
    """)


def _write_events(con: sqlite3.Connection, events: pl.DataFrame) -> None:
    con.executemany(
        "INSERT INTO event VALUES (?, ?)",
        events.select(s.OCEL_ID, s.OCEL_TYPE).iter_rows(),
    )
    used_suffixes: set[str] = set()
    for et in events[s.OCEL_TYPE].unique().sort().to_list():
        suffix = _unique_suffix(et, used_suffixes)
        used_suffixes.add(suffix)
        con.execute("INSERT INTO event_map_type VALUES (?, ?)", (et, suffix))

        type_df = events.filter(pl.col(s.OCEL_TYPE) == et)
        attr_cols = [
            c for c in type_df.columns
            if c not in _EVENT_FIXED and type_df[c].is_not_null().any()
        ]
        col_defs = (
            ["ocel_id TEXT", "ocel_time TEXT"]
            + [f'"{c}" {_sql_type(type_df[c].dtype)}' for c in attr_cols]
        )
        con.execute(f'CREATE TABLE "event_{suffix}" ({", ".join(col_defs)})')
        con.executemany(
            f'INSERT INTO "event_{suffix}" VALUES ({_placeholders(2 + len(attr_cols))})',
            [
                (row[s.OCEL_ID], _fmt(row[s.OCEL_TIME]))
                + tuple(_fmt(row[c]) for c in attr_cols)
                for row in type_df.iter_rows(named=True)
            ],
        )


def _write_objects(
    con: sqlite3.Connection, objects: pl.DataFrame, oc: pl.DataFrame
) -> None:
    con.executemany(
        "INSERT INTO object VALUES (?, ?)",
        objects.select(s.OCEL_ID, s.OCEL_TYPE).iter_rows(),
    )
    used_suffixes: set[str] = set()
    for ot in objects[s.OCEL_TYPE].unique().sort().to_list():
        suffix = _unique_suffix(ot, used_suffixes)
        used_suffixes.add(suffix)
        con.execute("INSERT INTO object_map_type VALUES (?, ?)", (ot, suffix))

        type_oc = oc.filter(pl.col(s.OCEL_TYPE) == ot)
        attr_cols = [
            c for c in type_oc.columns
            if c not in _OC_FIXED and type_oc[c].is_not_null().any()
        ]
        col_defs = (
            ["ocel_id TEXT", "ocel_time TEXT", "ocel_changed_field TEXT"]
            + [f'"{c}" {_sql_type(type_oc[c].dtype)}' for c in attr_cols]
        )
        con.execute(f'CREATE TABLE "object_{suffix}" ({", ".join(col_defs)})')
        if len(type_oc) > 0:
            con.executemany(
                f'INSERT INTO "object_{suffix}" VALUES ({_placeholders(3 + len(attr_cols))})',
                [
                    (row[s.OCEL_ID], _fmt(row[s.OCEL_TIME]), row[s.OCEL_CHANGED_FIELD])
                    + tuple(_fmt(row[c]) for c in attr_cols)
                    for row in type_oc.iter_rows(named=True)
                ],
            )


def _write_e2o(con: sqlite3.Connection, e2o: pl.DataFrame) -> None:
    con.executemany(
        "INSERT INTO event_object VALUES (?, ?, ?)",
        e2o.select(s.OCEL_EVENT_ID, s.OCEL_OBJECT_ID, s.OCEL_QUALIFIER).iter_rows(),
    )


def _write_o2o(con: sqlite3.Connection, o2o: pl.DataFrame) -> None:
    con.executemany(
        "INSERT INTO object_object VALUES (?, ?, ?)",
        o2o.select(s.OCEL_SOURCE_ID, s.OCEL_TARGET_ID, s.OCEL_QUALIFIER).iter_rows(),
    )


def _unique_suffix(name: str, used: set[str]) -> str:
    base = re.sub(r"[^a-z0-9]", "_", name.lower()).strip("_") or "type"
    suffix, n = base, 1
    while suffix in used:
        suffix, n = f"{base}_{n}", n + 1
    return suffix


def _sql_type(dtype: pl.DataType) -> str:
    if isinstance(dtype, (pl.Float32, pl.Float64)):
        return "REAL"
    if dtype.is_integer() or isinstance(dtype, pl.Boolean):
        return "INTEGER"
    return "TEXT"


def _fmt(val: object) -> object:
    if val is None:
        return None
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return val


def _placeholders(n: int) -> str:
    return ", ".join(["?"] * n)
