"""Filter relations by their ``ocel_qualifier``.

Qualifiers annotate what role an object plays in an event (E2O) or how two
objects relate (O2O). ``filter_e2o_by_qualifier`` treats qualifiers as a sub-log
selection and prunes the connected core; ``filter_o2o_by_qualifier`` only trims
O2O edges, since O2O relations do not decide which events or objects exist.
"""

from collections.abc import Iterable
from typing import Literal

import polars as pl

from oceldb import schema as s
from oceldb.utils.step import step
from oceldb.core.pruning import sublog_from_relations
from oceldb.filters._utils import normalize_scope, scoped_match
from oceldb.ocel import OCEL


@step
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
    scope = normalize_scope(object_types)
    keep = scoped_match(
        pl.col(s.OCEL_QUALIFIER).is_in(list(qualifiers)),
        type_col=s.OCEL_OBJECT_TYPE,
        scope=scope,
        mode=mode,
    )
    relations = ocel.event_object().filter(keep)
    return sublog_from_relations(ocel, relations)


@step
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
        scope = normalize_scope(object_types)
        assert scope is not None
        in_scope = pl.col(s.OCEL_SOURCE_TYPE).is_in(scope) | pl.col(
            s.OCEL_TARGET_TYPE
        ).is_in(scope)
        keep = (~in_scope) | decision
    return OCEL.from_frames(
        events=ocel.events(),
        objects=ocel.objects(),
        object_changes=ocel.object_changes(),
        object_object=ocel.object_object().filter(keep),
        event_object=ocel.event_object(),
        schema=ocel.schema,
        metadata=ocel.metadata,
    )
