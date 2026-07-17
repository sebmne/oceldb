"""filter_objects_by_attribute: keep or remove objects by a state predicate."""

from typing import Literal

import polars as pl

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations.filters._utils import (
    Mode,
    TimeBound,
    TypeScope,
    normalize_scope,
    normalize_time_bound,
    validate_mode,
)
from oceldb.operations.pruning import sublog_from_object_ids
from oceldb.operations.states import reconstruct_attribute_states
from oceldb.operations.step import step


@step
def filter_objects_by_attribute(
    ocel: OCEL,
    predicate: pl.Expr,
    *,
    object_types: TypeScope = None,
    when: Literal["sometimes", "always"] | TimeBound = "sometimes",
    mode: Mode = "include",
) -> OCEL:
    """Keep or remove objects whose state history satisfies a predicate.

    The predicate is evaluated against each object's forward-filled
    attribute state, one row per recorded change — see
    :func:`oceldb.operations.states.reconstruct_attribute_states`.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        predicate: A Polars expression evaluated against each state row. A
            null result is treated as no match.
        object_types: Limits which object types the predicate applies to;
            objects of other types are left untouched. ``None`` applies it
            to every type.
        when: ``"sometimes"`` matches an object if any recorded state
            matches; ``"always"`` matches if every recorded state matches
            (vacuously true for an object with no state history); an
            ISO/date/datetime value matches against the state as of that
            instant (not vacuously true for an object with no matching
            state).
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
    scope = normalize_scope(object_types)
    states = reconstruct_attribute_states(
        ocel.object_changes(*(scope or ())), tuple(scope or ())
    )
    scoped_objects = (
        ocel.objects()
        if scope is None
        else ocel.objects().filter(pl.col(s.OCEL_TYPE).is_in(scope))
    )
    resolved = predicate.fill_null(False)

    if when == "sometimes":
        matching = states.filter(resolved).select(s.OCEL_ID).unique()
    elif when == "always":
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
            .sort(s.OCEL_TYPE, s.OCEL_ID, s.OCEL_TIME)
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
