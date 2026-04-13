from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from oceldb.ast.base import BoolExpr, ExprVisitor, ScalarExpr, T

RelationKind = Literal["related", "linked", "has_event", "has_object"]


@dataclass(frozen=True)
class RelationSpec:
    kind: RelationKind
    target_type: str
    filters: tuple[BoolExpr, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class RelationExistsExpr(BoolExpr):
    spec: RelationSpec

    def accept(self, visitor: ExprVisitor[T]) -> T:
        visit = getattr(visitor, "visit_relation_exists")
        return visit(self)


@dataclass(frozen=True, eq=False)
class RelationCountExpr(ScalarExpr):
    spec: RelationSpec

    def accept(self, visitor: ExprVisitor[T]) -> T:
        visit = getattr(visitor, "visit_relation_count")
        return visit(self)


@dataclass(frozen=True)
class RelationAllExpr(BoolExpr):
    spec: RelationSpec
    condition: BoolExpr

    def accept(self, visitor: ExprVisitor[T]) -> T:
        visit = getattr(visitor, "visit_relation_all")
        return visit(self)
