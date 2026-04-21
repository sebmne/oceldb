"""Relation-predicate builders.

The fluent chains (``has_event("Pay Order").exists()``,
``linked("customer").outgoing().max_hops(3).count()``) produce the
``Relation*Expr`` nodes declared in ``oceldb.expr.nodes``.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from oceldb.expr.nodes import (
    Expr,
    LinkedDirection,
    RelationAllExpr,
    RelationCountExpr,
    RelationExistsExpr,
    RelationKind,
    RelationTarget,
)


@dataclass(frozen=True)
class _RelationBuilder:
    kind: RelationKind
    type_name: str
    direction: LinkedDirection = "any"
    hop_limit: int | None = 1

    def exists(self) -> RelationExistsExpr:
        return RelationExistsExpr(target=self._target())

    def count(self) -> RelationCountExpr:
        return RelationCountExpr(target=self._target())

    def any(self, predicate: Expr) -> RelationExistsExpr:
        return RelationExistsExpr(target=self._target(), predicate=predicate)

    def all(self, predicate: Expr) -> RelationAllExpr:
        return RelationAllExpr(target=self._target(), predicate=predicate)

    def _target(self) -> RelationTarget:
        return RelationTarget(
            kind=self.kind,
            type_name=self.type_name,
            direction=self.direction,
            hop_limit=self.hop_limit,
        )


@dataclass(frozen=True)
class _LinkedBuilder(_RelationBuilder):
    def incoming(self) -> "_LinkedBuilder":
        return replace(self, direction="incoming")

    def outgoing(self) -> "_LinkedBuilder":
        return replace(self, direction="outgoing")

    def max_hops(self, hops: int | None) -> "_LinkedBuilder":
        return replace(self, hop_limit=hops)


def has_event(event_type: str) -> _RelationBuilder:
    return _RelationBuilder(kind="has_event", type_name=event_type)


def has_object(object_type: str) -> _RelationBuilder:
    return _RelationBuilder(kind="has_object", type_name=object_type)


def cooccurs_with(object_type: str) -> _RelationBuilder:
    return _RelationBuilder(kind="cooccurs_with", type_name=object_type)


def linked(object_type: str) -> _LinkedBuilder:
    return _LinkedBuilder(kind="linked", type_name=object_type)


__all__ = ["cooccurs_with", "has_event", "has_object", "linked"]
