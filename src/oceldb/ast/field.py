from dataclasses import dataclass
from typing import Optional

from oceldb.ast.base import ScalarExpr


@dataclass(frozen=True, eq=False)
class FieldExpr(ScalarExpr):
    """
    Scalar expression representing a fixed field in the current scope.
    """

    name: str
    cast: Optional[str] = None
