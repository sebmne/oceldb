"""filter_events_by_attribute: keep or remove events matching a predicate."""

import polars as pl

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations.filters._utils import (
    Mode,
    TypeScope,
    normalize_scope,
    scoped_match,
)
from oceldb.operations.pruning import sublog_from_relations
from oceldb.operations.step import step


@step
def filter_events_by_attribute(
    ocel: OCEL,
    predicate: pl.Expr,
    *,
    event_types: TypeScope = None,
    mode: Mode = "include",
) -> OCEL:
    """Keep or remove events matching a Polars predicate.

    Removed events take their E2O relations, and any object or O2O edge left
    with no surviving relation, down with them — see
    :func:`oceldb.operations.pruning.sublog_from_relations`.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        predicate: A Polars expression evaluated against ``ocel.events()``.
            A null result is treated as no match.
        event_types: Limits which event types the predicate applies to;
            events of other types are left untouched. ``None`` applies the
            predicate to every type.
        mode: ``"include"`` keeps matching events; ``"exclude"`` removes
            them (nulls are then retained).

    Returns:
        A new ``OCEL`` pruned to the surviving events and their connected
        core.

    Examples:
        >>> from oceldb.operations.filters import filter_events_by_attribute
        >>> import polars as pl
        >>> sub = filter_events_by_attribute(ocel, pl.col("amount") > 100)
        >>> sub = ocel >> filter_events_by_attribute(pl.col("amount") > 100, mode="exclude")
    """
    keep = scoped_match(
        predicate,
        type_col=s.OCEL_TYPE,
        scope=normalize_scope(event_types),
        mode=mode,
    )
    events = ocel.events().filter(keep)
    event_ids = events.select(pl.col(s.OCEL_ID).alias(s.OCEL_EVENT_ID))
    relations = ocel.e2o().join(event_ids, on=s.OCEL_EVENT_ID, how="semi")
    return sublog_from_relations(ocel, relations, events=events)
