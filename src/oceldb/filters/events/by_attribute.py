"""filter_events_by_attribute: keep or drop events satisfying a predicate."""

from collections.abc import Iterable
from typing import Literal

import polars as pl

from oceldb import schema as s
from oceldb.utils.step import step
from oceldb.core.pruning import sublog_from_relations
from oceldb.filters._utils import normalize_scope, scoped_match
from oceldb.ocel import OCEL


@step
def filter_events_by_attribute(
    ocel: OCEL,
    predicate: pl.Expr,
    *,
    event_types: str | Iterable[str] | None = None,
    mode: Literal["include", "exclude"] = "include",
) -> OCEL:
    """Keep or drop events satisfying *predicate*, optionally scoped by type.

    This is the engine behind the other event filters: they build a predicate
    and delegate here.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        predicate: A Polars expression evaluated against the events frame.
        event_types: Event type(s) the filter applies to. When ``None``
            (default) it applies to all events; when given, only events of those
            types are subject to the filter and events of other types pass
            through unchanged.
        mode: ``"include"`` (default) keeps events matching *predicate*;
            ``"exclude"`` keeps events that do not match. Out-of-scope events are
            kept either way.

    Returns:
        A new ``OCEL`` with non-matching events removed and objects left
        unconnected pruned.

    Examples:
        >>> from oceldb.filters import filter_events_by_attribute
        >>> sub = filter_events_by_attribute(ocel, pl.col("amount") > 1000)
        >>> sub = filter_events_by_attribute(ocel, pl.col("amount") > 1000, event_types="Pay Order")
        >>> sub = ocel >> filter_events_by_attribute(pl.col("amount") > 1000, mode="exclude")
    """
    scope = normalize_scope(event_types)
    keep = scoped_match(predicate, type_col=s.OCEL_TYPE, scope=scope, mode=mode)
    events = ocel.events().filter(keep)
    event_ids = events.select(pl.col(s.OCEL_ID).alias(s.OCEL_EVENT_ID))
    relations = ocel.event_object().join(event_ids, on=s.OCEL_EVENT_ID, how="semi")
    return sublog_from_relations(ocel, relations, events=events)
