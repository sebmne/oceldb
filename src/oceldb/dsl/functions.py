from typing import Any, Iterable

from oceldb.ast.base import ScalarExpr
from oceldb.ast.function import InExpr


def in_(expr: ScalarExpr, values: Iterable[Any]) -> InExpr:
    """
    Build an IN predicate.
    """
    values_tuple = tuple(values)
    if not values_tuple:
        raise ValueError("IN predicate requires at least one value")
    return InExpr(expr=expr, values=values_tuple)
