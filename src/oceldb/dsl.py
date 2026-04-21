"""Public DSL surface — re-exports the user-facing expression builders.

The actual IR lives in ``oceldb.expr``; this module is the stable public
name users import from (``import oceldb.dsl as dsl``). Keeping the surface
here means ``oceldb.expr`` can evolve as an implementation detail.
"""

from oceldb.expr import (
    abs_,
    asc,
    avg,
    coalesce,
    col,
    cooccurs_with,
    count,
    count_distinct,
    desc,
    has_event,
    has_object,
    linked,
    lit,
    max_,
    min_,
    round_,
    row_number,
    sum_,
    when,
)

__all__ = [
    "abs_",
    "asc",
    "avg",
    "coalesce",
    "col",
    "cooccurs_with",
    "count",
    "count_distinct",
    "desc",
    "has_event",
    "has_object",
    "linked",
    "lit",
    "max_",
    "min_",
    "round_",
    "row_number",
    "sum_",
    "when",
]
