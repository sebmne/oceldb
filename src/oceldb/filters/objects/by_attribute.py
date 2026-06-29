"""filter_objects_by_attribute: keep objects satisfying a predicate on their states."""

from collections.abc import Callable, Iterable
from typing import Literal, overload

import polars as pl

from oceldb import schema as s
from oceldb.filters._step import _step
from oceldb.filters._utils import _to_list
from oceldb.ocel import OCEL


@overload
def filter_objects_by_attribute(
    ocel: OCEL,
    predicate: pl.Expr,
    *,
    object_types: str | Iterable[str] | None = ...,
    when: Literal["sometimes", "always"] | str = ...,
) -> OCEL: ...


@overload
def filter_objects_by_attribute(
    predicate: pl.Expr,
    *,
    object_types: str | Iterable[str] | None = ...,
    when: Literal["sometimes", "always"] | str = ...,
) -> Callable[[OCEL], OCEL]: ...


@_step
def filter_objects_by_attribute(
    ocel: OCEL,
    predicate: pl.Expr,
    *,
    object_types: str | Iterable[str] | None = None,
    when: Literal["sometimes", "always"] | str = "sometimes",
) -> OCEL:
    """Keep objects satisfying *predicate* on their states, optionally scoped to *object_types*.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        predicate: A Polars expression evaluated against the object states frame.
        object_types: Object type(s) to scope the filter to. When ``None``
            (default) the predicate is applied to all object types; when
            supplied, only objects of those types are filtered and objects of
            other types pass through unchanged.
        when: Controls which point(s) in the object's history *predicate* must
            hold:

            * ``"sometimes"`` *(default)* — at least one recorded state
              satisfies *predicate*.
            * ``"always"`` — every recorded state satisfies *predicate*.
              Objects with no recorded states are kept (vacuously true).
            * Any other string — treated as an inclusive timestamp upper bound;
              *predicate* must hold on the object's last known state at or
              before that instant. Objects with no state at or before the
              timestamp are dropped.

    Examples:
        >>> from oceldb.filters import filter_objects_by_attribute
        >>> sub = filter_objects_by_attribute(ocel, pl.col("price") > 100, when="sometimes")
        >>> sub = filter_objects_by_attribute(ocel, pl.col("price") > 100, object_types="order", when="always")
        >>> sub = ocel >> filter_objects_by_attribute(pl.col("price") > 100, when="2023-06-01")
    """
    states = ocel.object_states() if object_types is None else ocel.object_states(*_to_list(object_types))

    if when == "sometimes":
        satisfying = states.filter(predicate).select(s.OCEL_ID).unique()
    elif when == "always":
        all_ids = (
            ocel.objects().select(s.OCEL_ID)
            if object_types is None
            else ocel.objects().filter(pl.col(s.OCEL_TYPE).is_in(_to_list(object_types))).select(s.OCEL_ID)
        )
        failing = states.filter(~predicate).select(s.OCEL_ID).unique()
        satisfying = all_ids.join(failing, on=s.OCEL_ID, how="anti")
    else:
        satisfying = (
            states
            .filter(pl.col(s.OCEL_TIME) <= when)
            .unique(subset=[s.OCEL_ID], keep="last", maintain_order=True)
            .filter(predicate)
            .select(s.OCEL_ID)
        )

    satisfying_ids = satisfying.rename({s.OCEL_ID: s.OCEL_OBJECT_ID})

    if object_types is None:
        relations = ocel.event_object().join(satisfying_ids, on=s.OCEL_OBJECT_ID, how="semi")
    else:
        types = _to_list(object_types)
        non_target = ocel.event_object().filter(~pl.col(s.OCEL_OBJECT_TYPE).is_in(types))
        target = (
            ocel.event_object()
            .filter(pl.col(s.OCEL_OBJECT_TYPE).is_in(types))
            .join(satisfying_ids, on=s.OCEL_OBJECT_ID, how="semi")
        )
        relations = pl.concat([non_target, target])

    kept_events = relations.select(s.OCEL_EVENT_ID).unique()
    kept_objects = relations.select(s.OCEL_OBJECT_ID).unique()
    return OCEL(
        events=ocel.events().join(
            kept_events, left_on=s.OCEL_ID, right_on=s.OCEL_EVENT_ID, how="semi"
        ),
        objects=ocel.objects().join(
            kept_objects, left_on=s.OCEL_ID, right_on=s.OCEL_OBJECT_ID, how="semi"
        ),
        object_changes=ocel.object_changes().join(
            kept_objects, left_on=s.OCEL_ID, right_on=s.OCEL_OBJECT_ID, how="semi"
        ),
        o2o=ocel.object_object()
        .join(kept_objects, left_on=s.OCEL_SOURCE_ID, right_on=s.OCEL_OBJECT_ID, how="semi")
        .join(kept_objects, left_on=s.OCEL_TARGET_ID, right_on=s.OCEL_OBJECT_ID, how="semi"),
        e2o=relations,
    )
