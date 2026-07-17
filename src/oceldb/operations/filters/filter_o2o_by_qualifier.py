"""filter_o2o_by_qualifier: keep or remove O2O relations by qualifier."""

import polars as pl

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations._utils import replace
from oceldb.operations.filters._utils import (
    Mode,
    TypeScope,
    match_decision,
    normalize_scope,
)
from oceldb.operations.step import step


@step
def filter_o2o_by_qualifier(
    ocel: OCEL,
    *qualifiers: str,
    object_types: TypeScope = None,
    mode: Mode = "include",
) -> OCEL:
    """Keep or remove O2O relations by ``ocel_qualifier``.

    Unlike :func:`filter_e2o_by_qualifier`, this never changes object or
    event membership — it only filters the O2O table itself.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        *qualifiers: One or more qualifier values.
        object_types: Limits the filter to relations where the source or
            target object type is in this scope; other relations are left
            untouched. ``None`` applies it to every type.
        mode: ``"include"`` keeps matching relations; ``"exclude"`` removes
            them.

    Returns:
        A new ``OCEL`` with the filtered O2O table; events, objects, and
        E2O relations are unchanged.

    Examples:
        >>> from oceldb.operations.filters import filter_o2o_by_qualifier
        >>> sub = filter_o2o_by_qualifier(ocel, "belongs to")
        >>> sub = ocel >> filter_o2o_by_qualifier("belongs to", mode="exclude")
    """
    decision = match_decision(pl.col(s.OCEL_QUALIFIER).is_in(list(qualifiers)), mode)
    scope = normalize_scope(object_types)
    if scope is None:
        keep = decision
    else:
        in_scope = (
            pl.col(s.OCEL_SOURCE_TYPE).is_in(scope)
            | pl.col(s.OCEL_TARGET_TYPE).is_in(scope)
        ).fill_null(False)
        keep = (~in_scope) | decision
    return replace(ocel, o2o=ocel.o2o().filter(keep))
