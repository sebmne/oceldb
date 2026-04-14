from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Self, Tuple

from oceldb.ast.base import BoolExpr
from oceldb.ast.relation import (
    LinkedDirection,
    RelationAllExpr,
    RelationCountExpr,
    RelationExistsExpr,
    RelationKind,
    RelationSpec,
)


def cooccurs_with(object_type: str) -> "RelationBuilder":
    return RelationBuilder(kind="cooccurs_with", target_type=object_type)


def linked(object_type: str) -> "LinkedBuilder":
    return LinkedBuilder(kind="linked", target_type=object_type)


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

    def where(self, *exprs: BoolExpr) -> Self:
        if not exprs:
            return self
        return replace(self, filters=self.filters + tuple(exprs))

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


@dataclass(frozen=True)
class LinkedBuilder(RelationBuilder):
    hop_limit: int | None = 1
    direction: LinkedDirection = "bidirectional"

    def outgoing(self) -> "LinkedBuilder":
        return replace(self, direction="outgoing")

    def incoming(self) -> "LinkedBuilder":
        return replace(self, direction="incoming")

    def bidirectional(self) -> "LinkedBuilder":
        return replace(self, direction="bidirectional")

    def max_hops(self, hops: int | None) -> "LinkedBuilder":
        if hops is None:
            return replace(self, hop_limit=None)
        if hops < 1:
            raise ValueError(
                "linked(...).max_hops(...) requires a positive hop count or None"
            )
        return replace(self, hop_limit=hops)

    def _spec(self) -> RelationSpec:
        return RelationSpec(
            kind=self.kind,
            target_type=self.target_type,
            filters=self.filters,
            linked_direction=self.direction,
            linked_max_hops=self.hop_limit,
        )
