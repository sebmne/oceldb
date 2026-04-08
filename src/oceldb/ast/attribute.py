from dataclasses import dataclass
from typing import Optional

from oceldb.ast.base import ScalarExpr


@dataclass(frozen=True, eq=False)
class AttrExpr(ScalarExpr):
    """
    Scalar expression representing a dynamic JSON attribute.
    """

    name: str
    cast: Optional[str] = None
