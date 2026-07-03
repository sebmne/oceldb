"""Convert a pm4py OCEL object to an oceldb OCEL."""

from __future__ import annotations

from typing import Any

import polars as pl

from oceldb import schema as s
from oceldb.io.read._common import (
    O2O_SCHEMA,
    OBJECT_CHANGES_SCHEMA,
    empty_lf,
    parse_timestamps,
    rows_to_lf,
)
from oceldb.ocel import OCEL

# pm4py column name constants
_EID = "ocel:eid"
_ACTIVITY = "ocel:activity"
_TIMESTAMP = "ocel:timestamp"
_OID = "ocel:oid"
_OID2 = "ocel:oid_2"
_OTYPE = "ocel:type"
_QUALIFIER = "ocel:qualifier"

_OBJECT_CORE = {_OID, _OTYPE}


def read_pm4py(pm4py_ocel: Any) -> OCEL:
    """Convert a pm4py OCEL 2.0 ``OCEL`` object to an oceldb :class:`~oceldb.OCEL`.

    Only OCEL 2.0 logs are supported. Object attribute histories are read from
    pm4py's time-stamped ``object_changes`` table. OCEL 1.0 inputs — which carry
    static object attributes as columns on the objects DataFrame — are rejected;
    convert them to OCEL 2.0 in pm4py first.

    Args:
        pm4py_ocel: A ``pm4py.objects.ocel.obj.OCEL`` instance holding an
            OCEL 2.0 log.

    Returns:
        An ``OCEL`` backed by in-memory Polars lazy frames.

    Raises:
        ImportError: If ``pandas`` is not installed.
        ValueError: If ``pm4py_ocel`` is an OCEL 1.0 log (static object
            attributes stored as columns on the objects DataFrame).

    Notes:
        pm4py must be installed separately. oceldb does not declare it as a
        dependency. Object-to-object relations are included when the pm4py
        object exposes a non-empty ``o2o`` DataFrame.

    Examples:
        >>> import pm4py
        >>> from oceldb.io import read_pm4py
        >>> pm4py_ocel = pm4py.read_ocel2_xml("log.xmlocel")
        >>> ocel = read_pm4py(pm4py_ocel)
    """
    try:
        import pandas as pd  # noqa: F401
    except ImportError:
        raise ImportError("pandas is required for pm4py interop: pip install pandas")

    # --- events ---
    ev_df = pm4py_ocel.events
    events = pl.from_pandas(
        ev_df.rename(
            columns={_EID: s.OCEL_ID, _ACTIVITY: s.OCEL_TYPE, _TIMESTAMP: s.OCEL_TIME}
        )
    ).lazy()

    # --- objects (OCEL 2.0 only) ---
    obj_df = pm4py_ocel.objects
    static_attr_cols = [c for c in obj_df.columns if c not in _OBJECT_CORE]
    if static_attr_cols:
        raise ValueError(
            "read_pm4py only supports OCEL 2.0 logs, but the provided pm4py OCEL "
            "stores static object attributes as columns on the objects table "
            f"({static_attr_cols}), which indicates OCEL 1.0. Convert it to "
            "OCEL 2.0 in pm4py before importing."
        )
    obj_pl = pl.from_pandas(
        obj_df.rename(columns={_OID: s.OCEL_ID, _OTYPE: s.OCEL_TYPE})
    )
    objects = obj_pl.select(s.OCEL_ID, s.OCEL_TYPE).lazy()

    oc = _object_changes_from_pm4py(pm4py_ocel)

    # --- E2O from relations ---
    rel_df = pm4py_ocel.relations[
        [
            c
            for c in [_EID, _ACTIVITY, _OID, _OTYPE, _QUALIFIER]
            if c in pm4py_ocel.relations.columns
        ]
    ]
    e2o = pl.from_pandas(
        rel_df.rename(
            columns={
                _EID: s.OCEL_EVENT_ID,
                _ACTIVITY: s.OCEL_EVENT_TYPE,
                _OID: s.OCEL_OBJECT_ID,
                _OTYPE: s.OCEL_OBJECT_TYPE,
                _QUALIFIER: s.OCEL_QUALIFIER,
            }
        )
    ).lazy()

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

    return OCEL(events=events, objects=objects, object_changes=oc, o2o=o2o, e2o=e2o)


def _object_changes_from_pm4py(pm4py_ocel: Any) -> pl.LazyFrame:
    """Extract OCEL 2.0 object changes if available, else return empty frame."""
    oc_df = getattr(pm4py_ocel, "object_changes", None)
    if oc_df is None or len(oc_df) == 0:
        return empty_lf(OBJECT_CHANGES_SCHEMA)

    # pm4py OCEL 2.0 long format: ocel:oid, ocel:type, ocel:timestamp, ocel:field, ocel:value
    oc_df = oc_df.copy()
    rows: list[dict[str, Any]] = []
    for _, row in oc_df.iterrows():
        field = str(row.get("ocel:field", row.get("ocel:changed_field", "")))
        rows.append(
            {
                s.OCEL_ID: str(row[_OID]),
                s.OCEL_TYPE: str(row[_OTYPE]),
                s.OCEL_TIME: _to_iso(row[_TIMESTAMP]),
                s.OCEL_CHANGED_FIELD: field,
                field: row.get("ocel:value"),
            }
        )
    if not rows:
        return empty_lf(OBJECT_CHANGES_SCHEMA)
    return parse_timestamps(rows_to_lf(rows), s.OCEL_TIME)


def _to_iso(value: Any) -> str:
    """Render a pm4py timestamp as an ISO 8601 string for :func:`parse_timestamps`.

    pandas ``Timestamp`` and ``datetime`` values expose ``isoformat`` (which uses
    the ``T`` separator that :func:`parse_timestamps` expects); anything else is
    passed through as its plain string form.
    """
    isoformat = getattr(value, "isoformat", None)
    return isoformat() if callable(isoformat) else str(value)
