"""filter_o2o_by_qualifier: keep or remove O2O relations by qualifier."""

import polars as pl

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations.filters._utils import (
    Mode,
    scoped_match_many,
)
from oceldb.operations.step import step
from oceldb.types import OneOrMany, normalize_strings


@step
def filter_o2o_by_qualifier(
    ocel: OCEL,
    *qualifiers: str,
    source_types: OneOrMany[str] | None = None,
    target_types: OneOrMany[str] | None = None,
    mode: Mode = "include",
) -> OCEL:
    """Keep or remove O2O relations by ``ocel_qualifier``.

    Unlike :func:`filter_e2o_by_qualifier`, this never changes object or
    event membership — it only filters the O2O table itself.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        *qualifiers: One or more qualifier values.
        source_types: Limits the filter to relations whose source has one of
            these object types. Other relations are left untouched.
        target_types: Limits the filter to relations whose target has one of
            these object types. Other relations are left untouched.
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
    selected = normalize_strings(qualifiers, name="qualifiers", non_empty=True)
    keep = scoped_match_many(
        pl.col(s.OCEL_QUALIFIER).is_in(selected),
        scopes=(
            (
                s.OCEL_SOURCE_TYPE,
                normalize_strings(
                    source_types,
                    name="source_types",
                    non_empty=True,
                ),
            ),
            (
                s.OCEL_TARGET_TYPE,
                normalize_strings(
                    target_types,
                    name="target_types",
                    non_empty=True,
                ),
            ),
        ),
        mode=mode,
    )
    return ocel._derive(o2o=ocel.o2o().filter(keep))
