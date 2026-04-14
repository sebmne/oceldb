from __future__ import annotations

from dataclasses import dataclass

from oceldb.ast.base import BoolExpr, CaseExpr, ScalarValue


def when(condition: BoolExpr) -> "_PendingThen":
    """
    Start a CASE expression using a Polars-like `when(...).then(...).otherwise(...)` API.
    """
    return _PendingThen(branches=(), pending_condition=condition)


@dataclass(frozen=True)
class _PendingThen:
    branches: tuple[tuple[BoolExpr, ScalarValue], ...]
    pending_condition: BoolExpr

    def then(self, value: ScalarValue) -> "_CaseBuilder":
        return _CaseBuilder(
            branches=(
                *self.branches,
                (self.pending_condition, value),
            )
        )


@dataclass(frozen=True)
class _CaseBuilder:
    branches: tuple[tuple[BoolExpr, ScalarValue], ...]

    def when(self, condition: BoolExpr) -> _PendingThen:
        return _PendingThen(
            branches=self.branches,
            pending_condition=condition,
        )

    def otherwise(self, value: ScalarValue) -> CaseExpr:
        return CaseExpr(
            branches=self.branches,
            default=value,
        )
