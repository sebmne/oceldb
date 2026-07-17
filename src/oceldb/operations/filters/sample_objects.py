"""sample_objects: keep a random sample of objects."""

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations.filters._utils import _sample_ids
from oceldb.operations.filters.filter_objects_by_id import filter_objects_by_id
from oceldb.operations.step import step


@step
def sample_objects(
    ocel: OCEL,
    n: int | None = None,
    *,
    fraction: float | None = None,
    seed: int | None = None,
) -> OCEL:
    """Keep a random sample of objects and prune the connected core.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        n: Number of objects to sample. Pass exactly one of *n* or
            *fraction*.
        fraction: Fraction of objects to sample, in ``[0, 1]``. Pass exactly
            one of *n* or *fraction*.
        seed: Random seed for reproducible sampling. ``None`` samples
            non-deterministically.

    Returns:
        A new ``OCEL`` pruned to the sampled objects and their connected
        core.

    Raises:
        ValueError: If both or neither of *n*/*fraction* is given, or a
            value is out of range.

    Examples:
        >>> from oceldb.operations.filters import sample_objects
        >>> sub = sample_objects(ocel, n=50, seed=0)
        >>> sub = ocel >> sample_objects(fraction=0.2)
    """
    ids = ocel.objects().select(s.OCEL_ID).collect().get_column(s.OCEL_ID).drop_nulls()
    keep = _sample_ids(ids, n=n, fraction=fraction, seed=seed)
    return filter_objects_by_id(ocel, *keep)
