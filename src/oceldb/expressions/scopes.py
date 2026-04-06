from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Self

from oceldb.expressions.base import Expr
from oceldb.expressions.context import Context
from oceldb.expressions.scalar import ScalarExpr


@dataclass(frozen=True)
class Scope(ABC):
    """
    Base class for scope-like OCEL expressions.

    A scope represents a set of rows relative to the current root object, for
    example:
        - related objects
        - attached events
        - later: linked objects

    Scopes are immutable. Refinement methods like `.where(...)` return a new
    scope of the same concrete type.
    """

    filters: tuple[Expr, ...] = field(default_factory=tuple)

    def where(self, expr: Expr) -> Self:
        """
        Refine this scope with an additional predicate.
        """
        return self._with_filters(self.filters + (expr,))

    def exists(self) -> ScopeExistsExpr:
        """
        Reduce this scope to a boolean expression checking whether at least one
        row exists in the scope.
        """
        return ScopeExistsExpr(self)

    def count(self) -> ScopeCountExpr:
        """
        Reduce this scope to a scalar expression representing its row count.
        """
        return ScopeCountExpr(self)

    def any(self, expr: Expr) -> ScopeExistsExpr:
        """
        Shorthand for `.where(expr).exists()`.
        """
        return self.where(expr).exists()

    def all(self, expr: Expr) -> ScopeAllExpr:
        """
        Reduce this scope to a boolean expression checking whether all rows in
        the scope satisfy a predicate.
        """
        return ScopeAllExpr(self, expr)

    @abstractmethod
    def _with_filters(self, filters: tuple[Expr, ...]) -> Self:
        """
        Return a copy of this scope with the given filters.
        """
        raise NotImplementedError

    @abstractmethod
    def _scope_sql(self, ctx: Context) -> str:
        """
        Build the `FROM ... WHERE ...` SQL fragment describing this scope.

        The fragment should be valid immediately after:
            SELECT 1
        or:
            SELECT COUNT(*)
        """
        raise NotImplementedError


class ScopeExistsExpr(Expr):
    """
    Boolean expression: does the scope contain at least one row?
    """

    def __init__(self, scope: Scope) -> None:
        self.scope = scope

    def to_sql(self, ctx: Context) -> str:
        return f"EXISTS (SELECT 1 {self.scope._scope_sql(ctx)})"

    def __repr__(self) -> str:
        return f"ScopeExistsExpr(scope={self.scope!r})"


class ScopeCountExpr(ScalarExpr):
    """
    Scalar expression: how many rows are in the scope?
    """

    def __init__(self, scope: Scope) -> None:
        self.scope = scope

    def to_sql(self, ctx: Context) -> str:
        return f"(SELECT COUNT(*) {self.scope._scope_sql(ctx)})"

    def __repr__(self) -> str:
        return f"ScopeCountExpr(scope={self.scope!r})"


class ScopeAllExpr(Expr):
    """
    Boolean expression: do all rows in the scope satisfy a condition?

    Implemented as:
        NOT EXISTS row in scope for which NOT(condition)
    """

    def __init__(self, scope: Scope, condition: Expr) -> None:
        self.scope = scope
        self.condition = condition

    def to_sql(self, ctx: Context) -> str:
        child_ctx = self.scope._child_context(ctx)
        return (
            f"NOT EXISTS (SELECT 1 {self.scope._scope_sql(ctx)} "
            f"AND NOT ({self.condition.to_sql(child_ctx)}))"
        )

    def __repr__(self) -> str:
        return f"ScopeAllExpr(scope={self.scope!r}, condition={self.condition!r})"
