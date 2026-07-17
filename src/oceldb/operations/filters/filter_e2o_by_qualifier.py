"""filter_e2o_by_qualifier: keep or remove E2O relations by qualifier."""

import polars as pl

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations.filters._utils import (
    Mode,
    TypeScope,
    normalize_scope,
    scoped_match,
)
from oceldb.operations.pruning import sublog_from_relations
from oceldb.operations.step import step


@step
def filter_e2o_by_qualifier(
    ocel: OCEL,
    *qualifiers: str,
    object_types: TypeScope = None,
    mode: Mode = "include",
) -> OCEL:
    """Keep or remove E2O relations by ``ocel_qualifier``.

    Removing a relation can strand an event or object with no remaining
    relation; those are pruned along with it — see
    :func:`oceldb.operations.pruning.sublog_from_relations`.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        *qualifiers: One or more qualifier values.
        object_types: Limits which object types the qualifier filter applies
            to; relations to other object types are left untouched. ``None``
            applies it to every type.
        mode: ``"include"`` keeps matching relations; ``"exclude"`` removes
            them.

    Returns:
        A new ``OCEL`` pruned to the surviving events, objects, and
        relations.

    Examples:
        >>> from oceldb.operations.filters import filter_e2o_by_qualifier
        >>> sub = filter_e2o_by_qualifier(ocel, "pays")
        >>> sub = ocel >> filter_e2o_by_qualifier("pays", mode="exclude")
    """
    keep = scoped_match(
        pl.col(s.OCEL_QUALIFIER).is_in(list(qualifiers)),
        type_col=s.OCEL_OBJECT_TYPE,
        scope=normalize_scope(object_types),
        mode=mode,
    )
    return sublog_from_relations(ocel, ocel.e2o().filter(keep))
