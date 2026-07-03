"""Filter relations by their ``ocel_qualifier``.

Qualifiers annotate what role an object plays in an event (E2O) or how two
objects relate (O2O). ``filter_e2o_by_qualifier`` treats qualifiers as a sub-log
selection and prunes the connected core; ``filter_o2o_by_qualifier`` only trims
O2O edges, since O2O relations do not decide which events or objects exist.
"""

from collections.abc import Callable, Iterable
from typing import Literal, overload

import polars as pl

from oceldb import schema as s
from oceldb.utils import to_list
from oceldb.utils._step import _step
from oceldb.filters._utils import _scoped_match
from oceldb.pruning import prune_log
from oceldb.ocel import OCEL


@overload
def filter_e2o_by_qualifier(
    ocel: OCEL,
    *qualifiers: str,
    object_types: str | Iterable[str] | None = ...,
    mode: Literal["include", "exclude"] = ...,
) -> OCEL: ...


@overload
def filter_e2o_by_qualifier(
    *qualifiers: str,
    object_types: str | Iterable[str] | None = ...,
    mode: Literal["include", "exclude"] = ...,
) -> Callable[[OCEL], OCEL]: ...


@_step
def filter_e2o_by_qualifier(
    ocel: OCEL,
    *qualifiers: str,
    object_types: str | Iterable[str] | None = None,
    mode: Literal["include", "exclude"] = "include",
) -> OCEL:
    """Keep or remove event-to-object relations by ``ocel_qualifier``.

    Matching E2O relations are kept (or removed), then the connected core is
    pruned: events and objects left without any surviving relation are dropped,
    and O2O relations and object changes are pruned to the survivors.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        *qualifiers: E2O qualifier values to include or exclude.
        object_types: Object type(s) the filter applies to. ``None`` (default)
            applies it to every E2O relation; relations whose object is of
            another type pass through unchanged.
        mode: ``"include"`` (default) keeps matching relations; ``"exclude"``
            removes them.

    Examples:
        >>> from oceldb.filters import filter_e2o_by_qualifier
        >>> sub = filter_e2o_by_qualifier(ocel, "sends", "receives")
        >>> sub = ocel >> filter_e2o_by_qualifier("blocks", mode="exclude")
    """
    scope = to_list(object_types) if object_types is not None else None
    keep = _scoped_match(
        pl.col(s.OCEL_QUALIFIER).is_in(list(qualifiers)),
        type_col=s.OCEL_OBJECT_TYPE,
        scope=scope,
        mode=mode,
    )
    relations = ocel.event_object().filter(keep)
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


@overload
def filter_o2o_by_qualifier(
    ocel: OCEL,
    *qualifiers: str,
    object_types: str | Iterable[str] | None = ...,
    mode: Literal["include", "exclude"] = ...,
) -> OCEL: ...


@overload
def filter_o2o_by_qualifier(
    *qualifiers: str,
    object_types: str | Iterable[str] | None = ...,
    mode: Literal["include", "exclude"] = ...,
) -> Callable[[OCEL], OCEL]: ...


@_step
def filter_o2o_by_qualifier(
    ocel: OCEL,
    *qualifiers: str,
    object_types: str | Iterable[str] | None = None,
    mode: Literal["include", "exclude"] = "include",
) -> OCEL:
    """Keep or remove object-to-object relations by ``ocel_qualifier``.

    Only the O2O table is affected: non-matching O2O edges are dropped while
    events, objects, object changes, and E2O relations pass through unchanged.
    Objects are never removed, since O2O relations do not define object
    membership in the log.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        *qualifiers: O2O qualifier values to include or exclude.
        object_types: Object type(s) the filter applies to. ``None`` (default)
            applies it to every O2O edge; edges touching only other object types
            (neither source nor target in *object_types*) pass through
            unchanged.
        mode: ``"include"`` (default) keeps matching relations; ``"exclude"``
            removes them.

    Examples:
        >>> from oceldb.filters import filter_o2o_by_qualifier
        >>> sub = filter_o2o_by_qualifier(ocel, "contains")
        >>> sub = ocel >> filter_o2o_by_qualifier("duplicates", mode="exclude")
    """
    decision = pl.col(s.OCEL_QUALIFIER).is_in(list(qualifiers))
    if mode == "exclude":
        decision = ~decision
    if object_types is None:
        keep = decision
    else:
        scope = to_list(object_types)
        in_scope = pl.col(s.OCEL_SOURCE_TYPE).is_in(scope) | pl.col(
            s.OCEL_TARGET_TYPE
        ).is_in(scope)
        keep = (~in_scope) | decision
    return OCEL(
        events=ocel.events(),
        objects=ocel.objects(),
        object_changes=ocel.object_changes(),
        o2o=ocel.object_object().filter(keep),
        e2o=ocel.event_object(),
    )
