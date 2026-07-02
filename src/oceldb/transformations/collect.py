"""Terminal pipe steps for the >> pipeline."""

import polars as pl


class _CollectStep:
    def __rrshift__(self, lf: pl.LazyFrame) -> pl.DataFrame:
        return lf.collect()

    def __call__(self, lf: pl.LazyFrame) -> pl.DataFrame:
        return lf.collect()


def collect() -> _CollectStep:
    """Collect a lazy frame, for use as the final step in a ``>>`` pipeline.

    Returns a step that calls ``.collect()`` on whatever lazy frame precedes it.
    This lets you write a complete pipeline in one expression without a separate
    variable for the intermediate lazy frame.

    Returns:
        A callable step that materialises a :class:`polars.LazyFrame` into a
        :class:`polars.DataFrame`.

    Examples:
        >>> from oceldb import collect
        >>> from oceldb.transformations import flatten
        >>> df = ocel >> flatten("order") >> collect()
        >>> df = ocel >> view(object_types="order") >> flatten("order") >> collect()
    """
    return _CollectStep()
