"""sample_events: keep a random sample of events."""

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations.filters._utils import _sample_ids
from oceldb.operations.filters.filter_events_by_id import filter_events_by_id
from oceldb.operations.step import step


@step
def sample_events(
    ocel: OCEL,
    n: int | None = None,
    *,
    fraction: float | None = None,
    seed: int | None = None,
) -> OCEL:
    """Keep a random sample of events and prune the connected core.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        n: Number of events to sample. Pass exactly one of *n* or
            *fraction*.
        fraction: Fraction of events to sample, in ``[0, 1]``. Pass exactly
            one of *n* or *fraction*.
        seed: Random seed for reproducible sampling. ``None`` samples
            non-deterministically.

    Returns:
        A new ``OCEL`` pruned to the sampled events and their connected
        core.

    Raises:
        ValueError: If both or neither of *n*/*fraction* is given, or a
            value is out of range.

    Examples:
        >>> from oceldb.operations.filters import sample_events
        >>> sub = sample_events(ocel, n=100, seed=0)
        >>> sub = ocel >> sample_events(fraction=0.1)
    """
    ids = ocel.events().select(s.OCEL_ID).collect().get_column(s.OCEL_ID).drop_nulls()
    keep = _sample_ids(ids, n=n, fraction=fraction, seed=seed)
    return filter_events_by_id(ocel, *keep)
