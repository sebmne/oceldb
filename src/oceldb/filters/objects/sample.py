"""sample_objects: reduce a log to a random subset of its objects."""

from collections.abc import Callable
from typing import overload

from oceldb import schema as s
from oceldb.utils._step import _step
from oceldb.filters._utils import _sample_ids
from oceldb.filters.objects.by_id import filter_objects_by_id
from oceldb.ocel import OCEL


@overload
def sample_objects(
    ocel: OCEL,
    n: int | None = ...,
    *,
    fraction: float | None = ...,
    seed: int | None = ...,
) -> OCEL: ...


@overload
def sample_objects(
    n: int | None = ...,
    *,
    fraction: float | None = ...,
    seed: int | None = ...,
) -> Callable[[OCEL], OCEL]: ...


@_step
def sample_objects(
    ocel: OCEL,
    n: int | None = None,
    *,
    fraction: float | None = None,
    seed: int | None = None,
) -> OCEL:
    """Keep a random sample of objects and prune everything left unconnected.

    Exactly one of *n* or *fraction* must be given. Sampling is without
    replacement; events, relations, and object changes are pruned to the
    sampled objects' connected core.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        n: Number of objects to keep. Clamped to the available object count.
        fraction: Fraction of objects to keep, in ``[0, 1]``.
        seed: Optional seed for reproducible sampling.

    Raises:
        ValueError: If neither or both of *n* and *fraction* are given, or if
            *fraction* is outside ``[0, 1]`` or *n* is negative.

    Examples:
        >>> from oceldb.filters import sample_objects
        >>> sub = sample_objects(ocel, 500, seed=0)
        >>> sub = ocel >> sample_objects(fraction=0.25)
    """
    ids = ocel.objects().select(s.OCEL_ID).collect().get_column(s.OCEL_ID).drop_nulls()
    keep = _sample_ids(ids, n=n, fraction=fraction, seed=seed)
    return filter_objects_by_id(ocel, *keep)
