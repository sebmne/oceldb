from typing import Any, Optional

from oceldb.ast.attribute import AttrExpr
from oceldb.dsl._utils import python_type_to_sql_type


def attr(name: str, cast: Optional[type[Any]] = None) -> AttrExpr:
    """
    Build an expression for a dynamic JSON attribute in the current scope.
    """
    if not name:
        raise ValueError("Attribute name must not be empty")
    return AttrExpr(name=name, cast=python_type_to_sql_type(cast))
