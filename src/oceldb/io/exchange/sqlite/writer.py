"""write_sqlite: export an OCEL to the OCEL 2.0 SQLite format."""

import re
import sqlite3
from collections.abc import Mapping
from pathlib import Path

import polars as pl

from oceldb import schema as s
from oceldb.io._paths import atomic_file
from oceldb.io._schema import materialize, validate_for_exchange
from oceldb.io._values import encode_attribute, encode_scalar, qualifier
from oceldb.io.errors import ValidationMode, check_validation_mode
from oceldb.schema import TypeAttributes
from oceldb.ocel import OCEL


def write_sqlite(
    ocel: OCEL,
    path: str | Path,
    *,
    overwrite: bool = False,
    validation: ValidationMode = "strict",
) -> None:
    """Write an :class:`~oceldb.OCEL` to an OCEL 2.0 SQLite file.

    Produces the standard OCEL 2.0 SQLite schema: ``event`` / ``object``
    master tables, ``event_map_type`` / ``object_map_type`` type-to-suffix
    mappings, per-type ``event_<suffix>`` / ``object_<suffix>`` attribute
    tables, and ``event_object`` / ``object_object`` relation tables. The
    output can be imported with :func:`~oceldb.io.import_sqlite` or read by any
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
    validation = check_validation_mode(validation)
    data = materialize(ocel)
    validate_for_exchange(data, validation)
    with atomic_file(path, overwrite=overwrite) as (_, staging):
        con = sqlite3.connect(staging)
        try:
            _create_schema(con)
            _write_events(con, data.events, data.schema.event_types, validation)
            _write_objects(
                con,
                data.objects,
                data.object_changes,
                data.schema.object_types,
                validation,
            )
            _write_e2o(con, data.e2o, validation)
            _write_o2o(con, data.o2o, validation)
            con.commit()
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


def _write_events(
    con: sqlite3.Connection,
    events: pl.DataFrame,
    event_types: Mapping[str, TypeAttributes],
    validation: ValidationMode,
) -> None:
    con.executemany(
        "INSERT INTO event VALUES (?, ?)",
        events.select(s.OCEL_ID, s.OCEL_TYPE).iter_rows(),
    )
    used_suffixes: set[str] = set()
    for et, declarations in sorted(event_types.items()):
        suffix = _unique_suffix(et, used_suffixes)
        used_suffixes.add(suffix)
        con.execute("INSERT INTO event_map_type VALUES (?, ?)", (et, suffix))

        type_df = events.filter(pl.col(s.OCEL_TYPE) == et)
        attr_cols = list(declarations)
        col_defs = ["ocel_id TEXT", "ocel_time TEXT"] + [
            f'"{c}" {declarations[c].sqlite_type()}' for c in attr_cols
        ]
        con.execute(f'CREATE TABLE "event_{suffix}" ({", ".join(col_defs)})')
        con.executemany(
            f'INSERT INTO "event_{suffix}" VALUES ({_placeholders(2 + len(attr_cols))})',
            [
                (row[s.OCEL_ID], encode_scalar(row[s.OCEL_TIME], style="sqlite"))
                + tuple(
                    encode_attribute(
                        row.get(c),
                        declarations[c],
                        style="sqlite",
                        validation=validation,
                        context=f"event {row[s.OCEL_ID]!r} attribute {c!r}",
                    )
                    for c in attr_cols
                )
                for row in type_df.iter_rows(named=True)
            ],
        )


def _write_objects(
    con: sqlite3.Connection,
    objects: pl.DataFrame,
    oc: pl.DataFrame,
    object_types: Mapping[str, TypeAttributes],
    validation: ValidationMode,
) -> None:
    con.executemany(
        "INSERT INTO object VALUES (?, ?)",
        objects.select(s.OCEL_ID, s.OCEL_TYPE).iter_rows(),
    )
    used_suffixes: set[str] = set()
    for ot, declarations in sorted(object_types.items()):
        suffix = _unique_suffix(ot, used_suffixes)
        used_suffixes.add(suffix)
        con.execute("INSERT INTO object_map_type VALUES (?, ?)", (ot, suffix))

        type_oc = oc.filter(pl.col(s.OCEL_TYPE) == ot)
        attr_cols = list(declarations)
        col_defs = ["ocel_id TEXT", "ocel_time TEXT", "ocel_changed_field TEXT"] + [
            f'"{c}" {declarations[c].sqlite_type()}' for c in attr_cols
        ]
        con.execute(f'CREATE TABLE "object_{suffix}" ({", ".join(col_defs)})')
        if len(type_oc) > 0:
            con.executemany(
                f'INSERT INTO "object_{suffix}" VALUES ({_placeholders(3 + len(attr_cols))})',
                [
                    (
                        row[s.OCEL_ID],
                        encode_scalar(row[s.OCEL_TIME], style="sqlite"),
                        row[s.OCEL_CHANGED_FIELD],
                    )
                    + tuple(
                        encode_attribute(
                            row.get(c),
                            declarations[c],
                            style="sqlite",
                            validation=validation,
                            context=f"object {row[s.OCEL_ID]!r} attribute {c!r}",
                        )
                        for c in attr_cols
                    )
                    for row in type_oc.iter_rows(named=True)
                ],
            )


def _write_e2o(
    con: sqlite3.Connection, e2o: pl.DataFrame, validation: ValidationMode
) -> None:
    con.executemany(
        "INSERT INTO event_object VALUES (?, ?, ?)",
        (
            (
                row[s.OCEL_EVENT_ID],
                row[s.OCEL_OBJECT_ID],
                qualifier(
                    row[s.OCEL_QUALIFIER],
                    validation=validation,
                    context=f"E2O {row[s.OCEL_EVENT_ID]!r}",
                ),
            )
            for row in e2o.iter_rows(named=True)
        ),
    )


def _write_o2o(
    con: sqlite3.Connection, o2o: pl.DataFrame, validation: ValidationMode
) -> None:
    con.executemany(
        "INSERT INTO object_object VALUES (?, ?, ?)",
        (
            (
                row[s.OCEL_SOURCE_ID],
                row[s.OCEL_TARGET_ID],
                qualifier(
                    row[s.OCEL_QUALIFIER],
                    validation=validation,
                    context=f"O2O {row[s.OCEL_SOURCE_ID]!r}",
                ),
            )
            for row in o2o.iter_rows(named=True)
        ),
    )


def _unique_suffix(name: str, used: set[str]) -> str:
    base = re.sub(r"[^a-z0-9]", "_", name.lower()).strip("_") or "type"
    suffix, n = base, 1
    while suffix in used:
        suffix, n = f"{base}_{n}", n + 1
    return suffix


def _placeholders(n: int) -> str:
    return ", ".join(["?"] * n)
