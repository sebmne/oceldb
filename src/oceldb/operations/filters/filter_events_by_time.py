"""filter_events_by_time: keep or remove events within a UTC time window."""

import polars as pl

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations.filters._utils import (
    Mode,
    TimeBound,
    TypeScope,
    normalize_time_bound,
)
from oceldb.operations.filters.filter_events_by_attribute import (
    filter_events_by_attribute,
)
from oceldb.operations.step import step


@step
def filter_events_by_time(
    ocel: OCEL,
    *,
    start: TimeBound | None = None,
    end: TimeBound | None = None,
    event_types: TypeScope = None,
    mode: Mode = "include",
) -> OCEL:
    """Keep or remove events within an inclusive, UTC-normalized time window.

    Naive dates and datetimes are interpreted as UTC. Offset-aware values are
    converted to UTC before comparison with canonical ``ocel_time`` values.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        start: Inclusive lower bound (ISO 8601 string, ``date``, or
            ``datetime``). Omit for no lower bound.
        end: Inclusive upper bound, same accepted types as *start*. Omit for
            no upper bound.
        event_types: Limits which event types the window applies to; events
            of other types are left untouched. ``None`` applies it to every
            type.
        mode: ``"include"`` keeps events in the window; ``"exclude"``
            removes them.

    Returns:
        A new ``OCEL`` pruned to the surviving events and their connected
        core.

    Raises:
        ValueError: If neither *start* nor *end* is given, or *start* is
            later than *end*.

    Examples:
        >>> from oceldb.operations.filters import filter_events_by_time
        >>> sub = filter_events_by_time(ocel, start="2024-01-01")
        >>> sub = ocel >> filter_events_by_time(start="2024-01-01", end="2024-02-01")
    """
    start_value = None if start is None else normalize_time_bound(start, name="start")
    end_value = None if end is None else normalize_time_bound(end, name="end")
    if start_value is None and end_value is None:
        raise ValueError("Pass at least one of start or end.")
    if start_value is not None and end_value is not None and start_value > end_value:
        raise ValueError("start must not be later than end.")

    predicate = pl.lit(True)
    timestamp_type = pl.Datetime("us", "UTC")
    if start_value is not None:
        predicate &= pl.col(s.OCEL_TIME) >= pl.lit(start_value, dtype=timestamp_type)
    if end_value is not None:
        predicate &= pl.col(s.OCEL_TIME) <= pl.lit(end_value, dtype=timestamp_type)
    return filter_events_by_attribute(
        ocel,
        predicate,
        event_types=event_types,
        mode=mode,
    )
