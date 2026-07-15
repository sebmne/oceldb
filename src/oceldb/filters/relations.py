"""Relationship filters exposed through :mod:`oceldb.filters`."""

import polars as pl

from oceldb import schema as s
from oceldb.core.pruning import sublog_from_relations
from oceldb.filters._utils import (
    Mode,
    TypeScope,
    match_decision,
    normalize_scope,
    scoped_match,
)
from oceldb.ocel import OCEL
from oceldb.core.step import step


@step
def filter_e2o_by_qualifier(
    ocel: OCEL,
    *qualifiers: str,
    object_types: TypeScope = None,
    mode: Mode = "include",
) -> OCEL:
    """Filter E2O relations by qualifier and retain their connected core."""
    keep = scoped_match(
        pl.col(s.OCEL_QUALIFIER).is_in(list(qualifiers)),
        type_col=s.OCEL_OBJECT_TYPE,
        scope=normalize_scope(object_types),
        mode=mode,
    )
    return sublog_from_relations(ocel, ocel.event_object().filter(keep))


@step
def filter_o2o_by_qualifier(
    ocel: OCEL,
    *qualifiers: str,
    object_types: TypeScope = None,
    mode: Mode = "include",
) -> OCEL:
    """Filter O2O edges by qualifier without changing object membership."""
    decision = match_decision(pl.col(s.OCEL_QUALIFIER).is_in(list(qualifiers)), mode)
    scope = normalize_scope(object_types)
    if scope is None:
        keep = decision
    else:
        in_scope = (
            pl.col(s.OCEL_SOURCE_TYPE).is_in(scope)
            | pl.col(s.OCEL_TARGET_TYPE).is_in(scope)
        ).fill_null(False)
        keep = (~in_scope) | decision
    table = ocel._dataset.tables.object_object
    return OCEL(
        ocel._dataset.with_tables(
            object_object=table.map_partitions(lambda frame: frame.filter(keep)),
        )
    )
