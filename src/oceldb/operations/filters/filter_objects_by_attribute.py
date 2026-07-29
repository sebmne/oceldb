"""filter_objects_by_attribute: keep or remove objects by a state predicate."""

from typing import Literal

import polars as pl

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations.filters._utils import (
    Mode,
    normalize_time_bound,
    validate_mode,
    validate_predicate,
)
from oceldb.operations.pruning import sublog_from_object_ids
from oceldb.operations.step import step
from oceldb.types import OneOrMany, TimeLike, normalize_strings


@step
def filter_objects_by_attribute(
    ocel: OCEL,
    predicate: pl.Expr,
    *,
    object_types: OneOrMany[str] | None = None,
    when: Literal["any", "all"] | TimeLike = "any",
    mode: Mode = "include",
) -> OCEL:
    """Keep or remove objects whose state history satisfies a predicate.

    The predicate is evaluated against :meth:`OCEL.object_states`, with one
    forward-filled state row per recorded change timestamp.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        predicate: A Polars expression evaluated against each state row. A
            null result is treated as no match.
        object_types: Limits which object types the predicate applies to;
            objects of other types are left untouched. ``None`` applies it
            to every type.
        when: ``"any"`` matches an object if any recorded state matches;
            ``"all"`` matches if every recorded state matches (vacuously
            true for an object with no state history); an ISO/date/datetime
            value evaluates the state as of that instant.
        mode: ``"include"`` keeps matching objects; ``"exclude"`` removes
            them.

    Returns:
        A new ``OCEL`` pruned to the surviving objects and their connected
        core.

    Examples:
        >>> from oceldb.operations.filters import filter_objects_by_attribute
        >>> import polars as pl
        >>> sub = filter_objects_by_attribute(ocel, pl.col("status") == "shipped")
        >>> sub = ocel >> filter_objects_by_attribute(
        ...     pl.col("status") == "shipped", when="2024-01-01"
        ... )
    """
    validate_mode(mode)
    scope = normalize_strings(object_types, name="object_types", non_empty=True)
    states = ocel.object_states(*(scope or ()))
    scoped_objects = (
        ocel.objects()
        if scope is None
        else ocel.objects().filter(pl.col(s.OCEL_TYPE).is_in(scope))
    )
    resolved = validate_predicate(predicate).fill_null(False)

    if when == "any":
        matching = states.filter(resolved).select(s.OCEL_ID).unique()
    elif when == "all":
        failing = states.filter(~resolved).select(s.OCEL_ID).unique()
        matching = scoped_objects.select(s.OCEL_ID).join(
            failing, on=s.OCEL_ID, how="anti"
        )
    else:
        bound = normalize_time_bound(when, name="when")
        matching = (
            states.filter(
                pl.col(s.OCEL_TIME) <= pl.lit(bound, dtype=pl.Datetime("us", "UTC"))
            )
            .unique(subset=[s.OCEL_ID], keep="last", maintain_order=True)
            .filter(resolved)
            .select(s.OCEL_ID)
        )

    scoped_ids = scoped_objects.select(s.OCEL_ID)
    kept_scoped = (
        matching
        if mode == "include"
        else scoped_ids.join(matching, on=s.OCEL_ID, how="anti")
    )
    if scope is None:
        kept = kept_scoped
    else:
        outside_scope = (
            ocel.objects()
            .filter(~pl.col(s.OCEL_TYPE).is_in(scope).fill_null(False))
            .select(s.OCEL_ID)
        )
        kept = pl.concat([outside_scope, kept_scoped]).unique()

    return sublog_from_object_ids(
        ocel,
        kept.select(pl.col(s.OCEL_ID).alias(s.OCEL_OBJECT_ID)),
    )
