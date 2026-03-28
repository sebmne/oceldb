"""SQL comparison operators."""

from enum import Enum


class Op(Enum):
    """SQL comparison operator used in :class:`~oceldb.expr._expr.Comparison` nodes.

    Each member's value is the raw SQL operator string.
    """

    EQ = "="
    NE = "!="
    GT = ">"
    GE = ">="
    LT = "<"
    LE = "<="
