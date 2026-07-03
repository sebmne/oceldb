"""filter_objects_by_attribute: keep objects satisfying a predicate on their states."""

from collections.abc import Callable, Iterable
from typing import Literal, overload

import polars as pl

from oceldb import schema as s
from oceldb.utils import to_list
from oceldb.utils._step import _step
from oceldb.pruning import prune_log
from oceldb.ocel import OCEL


@overload
def filter_objects_by_attribute(
    ocel: OCEL,
    predicate: pl.Expr,
    *,
    object_types: str | Iterable[str] | None = ...,
    when: Literal["sometimes", "always"] | str = ...,
    mode: Literal["include", "exclude"] = ...,
) -> OCEL: ...


@overload
def filter_objects_by_attribute(
    predicate: pl.Expr,
    *,
    object_types: str | Iterable[str] | None = ...,
    when: Literal["sometimes", "always"] | str = ...,
    mode: Literal["include", "exclude"] = ...,
) -> Callable[[OCEL], OCEL]: ...


@_step
def filter_objects_by_attribute(
    ocel: OCEL,
    predicate: pl.Expr,
    *,
    object_types: str | Iterable[str] | None = None,
    when: Literal["sometimes", "always"] | str = "sometimes",
    mode: Literal["include", "exclude"] = "include",
) -> OCEL:
    """Keep objects satisfying *predicate* on their states, optionally scoped by type.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        predicate: A Polars expression evaluated against the object states frame.
        object_types: Object type(s) the filter applies to. When ``None``
            (default) it applies to all object types; when given, only objects
            of those types are subject to the filter and objects of other types
            pass through unchanged.
        when: Controls which point(s) in the object's history *predicate* must
            hold:

            * ``"sometimes"`` *(default)* — at least one recorded state
              satisfies *predicate*.
            * ``"always"`` — every recorded state satisfies *predicate*.
              Objects with no recorded states are matched (vacuously true).
            * Any other string — treated as an inclusive timestamp upper bound;
              *predicate* must hold on the object's last known state at or
              before that instant. Objects with no state at or before the
              timestamp do not match.
        mode: ``"include"`` (default) keeps objects matching the *when*
            condition; ``"exclude"`` keeps objects (within scope) that do not.

    Examples:
        >>> from oceldb.filters import filter_objects_by_attribute
        >>> sub = filter_objects_by_attribute(ocel, pl.col("price") > 100, when="sometimes")
        >>> sub = filter_objects_by_attribute(ocel, pl.col("price") > 100, object_types="order", when="always")
        >>> sub = ocel >> filter_objects_by_attribute(pl.col("price") > 100, mode="exclude")
    """
    scope = to_list(object_types) if object_types is not None else None
    states = ocel.object_states() if scope is None else ocel.object_states(*scope)
    scoped_objects = (
        ocel.objects()
        if scope is None
        else ocel.objects().filter(pl.col(s.OCEL_TYPE).is_in(scope))
    )

    if when == "sometimes":
        matching = states.filter(predicate).select(s.OCEL_ID).unique()
    elif when == "always":
        failing = states.filter(~predicate).select(s.OCEL_ID).unique()
        matching = scoped_objects.select(s.OCEL_ID).join(
            failing, on=s.OCEL_ID, how="anti"
        )
    else:
        matching = (
            states.filter(pl.col(s.OCEL_TIME) <= when)
            .unique(subset=[s.OCEL_ID], keep="last", maintain_order=True)
            .filter(predicate)
            .select(s.OCEL_ID)
        )

    if mode == "include":
        kept = matching
    else:
        kept = scoped_objects.select(s.OCEL_ID).join(matching, on=s.OCEL_ID, how="anti")
    kept_ids = kept.rename({s.OCEL_ID: s.OCEL_OBJECT_ID})

    if scope is None:
        relations = ocel.event_object().join(kept_ids, on=s.OCEL_OBJECT_ID, how="semi")
    else:
        non_target = ocel.event_object().filter(
            ~pl.col(s.OCEL_OBJECT_TYPE).is_in(scope)
        )
        target = (
            ocel.event_object()
            .filter(pl.col(s.OCEL_OBJECT_TYPE).is_in(scope))
            .join(kept_ids, on=s.OCEL_OBJECT_ID, how="semi")
        )
        relations = pl.concat([non_target, target])

    kept_events = relations.select(s.OCEL_EVENT_ID).unique()
    kept_objects = relations.select(s.OCEL_OBJECT_ID).unique()
    return prune_log(
        ocel,
        events=ocel.events().join(
            kept_events, left_on=s.OCEL_ID, right_on=s.OCEL_EVENT_ID, how="semi"
        ),
        objects=ocel.objects().join(
            kept_objects, left_on=s.OCEL_ID, right_on=s.OCEL_OBJECT_ID, how="semi"
        ),
        e2o=relations,
    )
