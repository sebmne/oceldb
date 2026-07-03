"""sample_events: reduce a log to a random subset of its events."""

from collections.abc import Callable
from typing import overload

from oceldb import schema as s
from oceldb.utils._step import _step
from oceldb.filters._utils import _sample_ids
from oceldb.filters.events.by_id import filter_events_by_id
from oceldb.ocel import OCEL


@overload
def sample_events(
    ocel: OCEL,
    n: int | None = ...,
    *,
    fraction: float | None = ...,
    seed: int | None = ...,
) -> OCEL: ...


@overload
def sample_events(
    n: int | None = ...,
    *,
    fraction: float | None = ...,
    seed: int | None = ...,
) -> Callable[[OCEL], OCEL]: ...


@_step
def sample_events(
    ocel: OCEL,
    n: int | None = None,
    *,
    fraction: float | None = None,
    seed: int | None = None,
) -> OCEL:
    """Keep a random sample of events and prune everything left unconnected.

    Exactly one of *n* or *fraction* must be given. Sampling is without
    replacement; objects, relations, and object changes are pruned to the
    sampled events' connected core.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        n: Number of events to keep. Clamped to the available event count.
        fraction: Fraction of events to keep, in ``[0, 1]``.
        seed: Optional seed for reproducible sampling.

    Raises:
        ValueError: If neither or both of *n* and *fraction* are given, or if
            *fraction* is outside ``[0, 1]`` or *n* is negative.

    Examples:
        >>> from oceldb.filters import sample_events
        >>> sub = sample_events(ocel, 1000, seed=0)
        >>> sub = ocel >> sample_events(fraction=0.1)
    """
    ids = (
        ocel.events().select(s.OCEL_ID).collect().get_column(s.OCEL_ID).drop_nulls()
    )
    keep = _sample_ids(ids, n=n, fraction=fraction, seed=seed)
    return filter_events_by_id(ocel, *keep)
