from __future__ import annotations

from dataclasses import dataclass

from oceldb.ast.base import ExprVisitor, ScalarExpr, T


@dataclass(frozen=True, eq=False)
class ColumnExpr(ScalarExpr):
    """
    Scalar expression referencing a typed column in the current query scope.
    """

    name: str

    def accept(self, visitor: ExprVisitor[T]) -> T:
        visit = getattr(visitor, "visit_column")
        return visit(self)
