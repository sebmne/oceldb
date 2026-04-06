from oceldb.expressions.base import Expr
from oceldb.expressions.comparison import (
    BetweenExpr,
    CompareExpr,
    InExpr,
    IsNullExpr,
    NotNullExpr,
)


class ScalarExpr(Expr):
    def __eq__(self, other: object) -> CompareExpr:  # type: ignore[override]
        return CompareExpr(self, "=", other)

    def __ne__(self, other: object) -> CompareExpr:  # type: ignore[override]
        return CompareExpr(self, "!=", other)

    def __gt__(self, other: object) -> CompareExpr:
        return CompareExpr(self, ">", other)

    def __ge__(self, other: object) -> CompareExpr:
        return CompareExpr(self, ">=", other)

    def __lt__(self, other: object) -> CompareExpr:
        return CompareExpr(self, "<", other)

    def __le__(self, other: object) -> CompareExpr:
        return CompareExpr(self, "<=", other)

    def is_null(self) -> IsNullExpr:
        return IsNullExpr(self)

    def is_not_null(self) -> NotNullExpr:
        return NotNullExpr(self)

    def is_in(self, values: list[object]) -> InExpr:
        return InExpr(self, values)

    def between(self, low: object, high: object) -> BetweenExpr:
        return BetweenExpr(self, low, high)
