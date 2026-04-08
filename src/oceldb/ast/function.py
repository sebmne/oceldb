from dataclasses import dataclass
from typing import Any, Tuple

from oceldb.ast.base import BoolExpr, ScalarExpr


@dataclass(frozen=True)
class InExpr(BoolExpr):
    """
    Boolean IN predicate.
    """

    expr: ScalarExpr
    values: Tuple[Any, ...]
