"""Convert between oceldb and pm4py OCEL objects."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import polars as pl

from oceldb import schema as s
from oceldb.io._schema import materialize
from oceldb.io._values import format_datetime
from oceldb.io.exchange._common import (
    EPOCH,
    O2O_SCHEMA,
    OBJECT_CHANGES_SCHEMA,
    empty_lf,
    parse_timestamps,
    rows_to_lf,
)
from oceldb.io.exchange.json.writer import write_json
from oceldb.ocel import OCEL

__all__ = ["from_pm4py", "to_pm4py"]

# pm4py column name constants
_EID = "ocel:eid"
_ACTIVITY = "ocel:activity"
_TIMESTAMP = "ocel:timestamp"
_OID = "ocel:oid"
_OID2 = "ocel:oid_2"
_OTYPE = "ocel:type"
_QUALIFIER = "ocel:qualifier"
_FIELD = "ocel:field"
_VALUE = "ocel:value"
_CHANGED_FIELD = "ocel:changed_field"
_CUMCOUNT = "@@cumcount"

_OBJECT_CORE = {_OID, _OTYPE}
_OBJECT_CHANGE_CORE = {_OID, _OTYPE, _TIMESTAMP, _FIELD, _CHANGED_FIELD, _CUMCOUNT}


def from_pm4py(pm4py_ocel: Any) -> OCEL:
    """Convert a pm4py OCEL 2.0 ``OCEL`` object to an oceldb :class:`~oceldb.OCEL`.

    Object attribute histories are read from pm4py's time-stamped
    ``object_changes`` table. Object attributes that pm4py exposes as columns on
    the objects DataFrame are imported as initial object-change rows at the Unix
    epoch, matching the JSON/XML reader behavior for object attributes without a
    timestamp.

    Args:
        pm4py_ocel: A ``pm4py.objects.ocel.obj.OCEL`` instance holding an
            OCEL 2.0 log.

    Returns:
        An ``OCEL`` backed by in-memory Polars lazy frames.

    Raises:
        ImportError: If ``pandas`` is not installed.

    Notes:
        pm4py must be installed separately. oceldb does not declare it as a
        dependency. Object-to-object relations are included when the pm4py
        object exposes a non-empty ``o2o`` DataFrame.

    Examples:
        >>> import pm4py
        >>> from oceldb.io import from_pm4py
        >>> pm4py_ocel = pm4py.read_ocel2_xml("log.xmlocel")
        >>> ocel = from_pm4py(pm4py_ocel)
    """
    try:
        import pandas as pd  # pyright: ignore[reportMissingImports]
    except ImportError:
        raise ImportError("pandas is required for pm4py interop: pip install pandas")

    # --- events ---
    ev_df = pm4py_ocel.events
    events = pl.from_pandas(
        ev_df.rename(
            columns={_EID: s.OCEL_ID, _ACTIVITY: s.OCEL_TYPE, _TIMESTAMP: s.OCEL_TIME}
        )
    ).lazy()

    # --- objects ---
    obj_df = pm4py_ocel.objects
    static_attr_cols = [c for c in obj_df.columns if c not in _OBJECT_CORE]
    obj_pl = pl.from_pandas(
        obj_df.rename(columns={_OID: s.OCEL_ID, _OTYPE: s.OCEL_TYPE})
    )
    objects = obj_pl.select(s.OCEL_ID, s.OCEL_TYPE).lazy()

    oc = _object_changes_from_pm4py(pm4py_ocel, obj_df, static_attr_cols, pd)

    # --- E2O from relations ---
    rel_df = pm4py_ocel.relations[
        [
            c
            for c in [_EID, _ACTIVITY, _OID, _OTYPE, _QUALIFIER]
            if c in pm4py_ocel.relations.columns
        ]
    ]
    e2o_frame = pl.from_pandas(
        rel_df.rename(
            columns={
                _EID: s.OCEL_EVENT_ID,
                _ACTIVITY: s.OCEL_EVENT_TYPE,
                _OID: s.OCEL_OBJECT_ID,
                _OTYPE: s.OCEL_OBJECT_TYPE,
                _QUALIFIER: s.OCEL_QUALIFIER,
            }
        )
    )
    for column in (
        s.OCEL_EVENT_ID,
        s.OCEL_EVENT_TYPE,
        s.OCEL_OBJECT_ID,
        s.OCEL_OBJECT_TYPE,
        s.OCEL_QUALIFIER,
    ):
        if column not in e2o_frame.columns:
            e2o_frame = e2o_frame.with_columns(
                pl.lit(None, dtype=pl.String()).alias(column)
            )
    e2o = e2o_frame.lazy()

    # --- O2O ---
    o2o_df = getattr(pm4py_ocel, "o2o", None)
    if o2o_df is not None and len(o2o_df) > 0 and _OID in o2o_df.columns:
        oid_to_type: dict[str, str] = dict(
            zip(obj_df[_OID].tolist(), obj_df[_OTYPE].tolist())
        )
        o2o_rows: list[dict[str, Any]] = []
        for _, row in o2o_df.iterrows():
            src = str(row[_OID])
            tgt = str(row.get(_OID2, row.get("ocel:oid2", "")))
            o2o_rows.append(
                {
                    s.OCEL_SOURCE_ID: src,
                    s.OCEL_SOURCE_TYPE: oid_to_type.get(src, ""),
                    s.OCEL_TARGET_ID: tgt,
                    s.OCEL_TARGET_TYPE: oid_to_type.get(tgt, ""),
                    s.OCEL_QUALIFIER: str(row.get(_QUALIFIER, "")),
                }
            )
        o2o = rows_to_lf(o2o_rows) if o2o_rows else empty_lf(O2O_SCHEMA)
    else:
        o2o = empty_lf(O2O_SCHEMA)

    result = OCEL.from_frames(
        events=events,
        objects=objects,
        object_changes=oc,
        object_object=o2o,
        event_object=e2o,
    )
    inferred = materialize(result).schema
    return OCEL.from_frames(
        events=events,
        objects=objects,
        object_changes=oc,
        object_object=o2o,
        event_object=e2o,
        schema=inferred,
    )


def _object_changes_from_pm4py(
    pm4py_ocel: Any,
    obj_df: Any,
    static_attr_cols: list[str],
    pd: Any,
) -> pl.LazyFrame:
    """Extract object changes from pm4py's long and object-table formats."""
    rows: list[dict[str, Any]] = []

    oc_df = getattr(pm4py_ocel, "object_changes", None)
    if oc_df is not None and len(oc_df) > 0:
        # PM4PY exposes object changes in two shapes:
        # - long: ocel:oid, ocel:type, ocel:timestamp, ocel:field, ocel:value
        # - sparse wide: ocel:oid, ocel:type, ocel:timestamp, ocel:field, <attr columns...>
        oc_df = oc_df.copy()
        for _, row in oc_df.iterrows():
            rows.extend(_object_change_rows(row, pd))

    for _, row in obj_df.iterrows():
        for field in static_attr_cols:
            value = row.get(field)
            if _is_missing(value, pd):
                continue
            rows.append(
                {
                    s.OCEL_ID: str(row[_OID]),
                    s.OCEL_TYPE: str(row[_OTYPE]),
                    s.OCEL_TIME: EPOCH,
                    s.OCEL_CHANGED_FIELD: None,
                    field: value,
                }
            )

    if not rows:
        return empty_lf(OBJECT_CHANGES_SCHEMA)
    return parse_timestamps(rows_to_lf(rows), s.OCEL_TIME)


def _object_change_rows(row: Any, pd: Any) -> list[dict[str, Any]]:
    changed_field = _field_name(row, pd)
    if changed_field:
        value = _changed_value(row, changed_field, pd)
        if _is_missing(value, pd):
            return []
        return [_object_change_row(row, changed_field, value)]

    rows: list[dict[str, Any]] = []
    for field in row.index:
        if field in _OBJECT_CHANGE_CORE:
            continue
        value = row.get(field)
        if _is_missing(value, pd):
            continue
        rows.append(_object_change_row(row, str(field), value))
    return rows


def _field_name(row: Any, pd: Any) -> str:
    for column in (_FIELD, _CHANGED_FIELD):
        if column not in row:
            continue
        value = row.get(column)
        if not _is_missing(value, pd):
            return str(value)
    return ""


def _changed_value(row: Any, field: str, pd: Any) -> Any:
    if _VALUE in row and not _is_missing(row.get(_VALUE), pd):
        return row.get(_VALUE)
    if field in row:
        return row.get(field)
    return None


def _object_change_row(row: Any, field: str, value: Any) -> dict[str, Any]:
    return {
        s.OCEL_ID: str(row[_OID]),
        s.OCEL_TYPE: str(row[_OTYPE]),
        s.OCEL_TIME: _to_iso(row[_TIMESTAMP]),
        s.OCEL_CHANGED_FIELD: field,
        field: value,
    }


def _is_missing(value: Any, pd: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _to_iso(value: Any) -> str:
    """Render a pm4py timestamp as an ISO 8601 string for :func:`parse_timestamps`.

    Datetime-like values are normalized to the same UTC spelling used by the
    exchange writers; anything else is passed through as plain text.
    """
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, (datetime, date)):
        return format_datetime(value)
    return str(value)


def to_pm4py(ocel: OCEL) -> Any:
    """Return a pm4py OCEL through its public OCEL 2.0 JSON importer.

    Using pm4py's own importer keeps this bridge compatible with its internal
    dataframe model instead of depending on the constructor signature of a
    specific pm4py release.
    """
    try:
        import pm4py  # pyright: ignore[reportMissingImports]
    except ImportError as exc:
        raise ImportError("pm4py interop requires: pip install pm4py") from exc
    with TemporaryDirectory(prefix="oceldb-pm4py-") as directory:
        path = Path(directory) / "exchange.jsonocel"
        write_json(ocel, path)
        reader = getattr(pm4py, "read_ocel2_json", None)
        if reader is None:
            reader = getattr(pm4py, "read_ocel", None)
        if reader is None:
            raise RuntimeError("Installed pm4py has no OCEL 2.0 JSON reader.")
        return reader(str(path))
