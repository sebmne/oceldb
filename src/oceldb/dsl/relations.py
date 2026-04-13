from __future__ import annotations

from dataclasses import dataclass, field
from typing import Tuple

from oceldb.ast.base import BoolExpr
from oceldb.ast.relation import (
    RelationAllExpr,
    RelationCountExpr,
    RelationExistsExpr,
    RelationKind,
    RelationSpec,
)


def related(object_type: str) -> "RelationBuilder":
    return RelationBuilder(kind="related", target_type=object_type)


def linked(object_type: str) -> "RelationBuilder":
    return RelationBuilder(kind="linked", target_type=object_type)


def has_event(event_type: str) -> "RelationBuilder":
    return RelationBuilder(kind="has_event", target_type=event_type)


def has_object(object_type: str) -> "RelationBuilder":
    return RelationBuilder(kind="has_object", target_type=object_type)


@dataclass(frozen=True)
class RelationBuilder:
    """
    Fluent builder for scope-validated relation predicates.
    """

    kind: RelationKind
    target_type: str
    filters: Tuple[BoolExpr, ...] = field(default_factory=tuple)

    def where(self, *exprs: BoolExpr) -> "RelationBuilder":
        if not exprs:
            return self
        return RelationBuilder(
            kind=self.kind,
            target_type=self.target_type,
            filters=self.filters + tuple(exprs),
        )

    def exists(self) -> RelationExistsExpr:
        return RelationExistsExpr(self._spec())

    def count(self) -> RelationCountExpr:
        return RelationCountExpr(self._spec())

    def any(self, expr: BoolExpr) -> RelationExistsExpr:
        return self.where(expr).exists()

    def all(self, expr: BoolExpr) -> RelationAllExpr:
        return RelationAllExpr(self._spec(), expr)

    def _spec(self) -> RelationSpec:
        return RelationSpec(
            kind=self.kind,
            target_type=self.target_type,
            filters=self.filters,
        )
