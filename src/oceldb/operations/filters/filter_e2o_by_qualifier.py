"""filter_e2o_by_qualifier: keep or remove E2O relations by qualifier."""

import polars as pl

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations.filters._utils import (
    Mode,
    scoped_match_many,
)
from oceldb.operations.pruning import sublog_from_relations
from oceldb.operations.step import step
from oceldb.types import OneOrMany, normalize_strings


@step
def filter_e2o_by_qualifier(
    ocel: OCEL,
    *qualifiers: str,
    event_types: OneOrMany[str] | None = None,
    object_types: OneOrMany[str] | None = None,
    mode: Mode = "include",
) -> OCEL:
    """Keep or remove E2O relations by ``ocel_qualifier``.

    Removing a relation can strand an event or object with no remaining
    relation; those are pruned along with it — see
    :func:`oceldb.operations.pruning.sublog_from_relations`.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        *qualifiers: One or more qualifier values.
        event_types: Limits the filter to relations whose event has one of
            these types. Other relations are left untouched.
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
    selected = normalize_strings(qualifiers, name="qualifiers", non_empty=True)
    keep = scoped_match_many(
        pl.col(s.OCEL_QUALIFIER).is_in(selected),
        scopes=(
            (
                s.OCEL_EVENT_TYPE,
                normalize_strings(
                    event_types,
                    name="event_types",
                    non_empty=True,
                ),
            ),
            (
                s.OCEL_OBJECT_TYPE,
                normalize_strings(
                    object_types,
                    name="object_types",
                    non_empty=True,
                ),
            ),
        ),
        mode=mode,
    )
    return sublog_from_relations(ocel, ocel.e2o().filter(keep))
