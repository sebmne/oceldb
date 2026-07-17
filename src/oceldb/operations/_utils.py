"""Shared helper for building a derived ``OCEL`` from a source one."""

from typing import Any

import polars as pl

from oceldb.ocel import OCEL

_INHERIT: Any = object()


def replace(
    ocel: OCEL,
    *,
    events: pl.LazyFrame | None = None,
    objects: pl.LazyFrame | None = None,
    object_changes: pl.LazyFrame | None = None,
    e2o: pl.LazyFrame | None = None,
    o2o: pl.LazyFrame | None = None,
    event_attributes: dict[str, list[str]] | None = _INHERIT,
    change_attributes: dict[str, list[str]] | None = _INHERIT,
) -> OCEL:
    """Build a derived ``OCEL``, defaulting each table to *ocel*'s own.

    Narrowing metadata (which attribute columns each type actually has) is
    inherited from *ocel* unchanged unless *event_attributes* or
    *change_attributes* is passed explicitly. This is correct for any
    operation that only filters rows — the vast majority — since a type's
    column set doesn't change when rows are dropped. An operation that adds,
    drops, or renames attribute columns or type names must pass the updated
    dict explicitly; passing ``None`` clears it (falls back to unnarrowed).
    """
    return OCEL(
        events=ocel.events() if events is None else events,
        objects=ocel.objects() if objects is None else objects,
        object_changes=(
            ocel.object_changes() if object_changes is None else object_changes
        ),
        e2o=ocel.e2o() if e2o is None else e2o,
        o2o=ocel.o2o() if o2o is None else o2o,
        event_attributes=(
            ocel._event_attributes if event_attributes is _INHERIT else event_attributes
        ),
        change_attributes=(
            ocel._change_attributes
            if change_attributes is _INHERIT
            else change_attributes
        ),
    )
