from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Tuple

from oceldb.ast.base import BoolExpr, ScalarExpr

RelationKind = Literal["related", "linked", "has_event"]


@dataclass(frozen=True)
class RelationSpec:
    """
    Immutable description of a relation traversal from the current root scope.
    """

    kind: RelationKind
    target_type: str
    filters: Tuple[BoolExpr, ...] = ()


@dataclass(frozen=True)
class RelationExistsExpr(BoolExpr):
    spec: RelationSpec


@dataclass(frozen=True)
class RelationAllExpr(BoolExpr):
    spec: RelationSpec
    condition: BoolExpr


@dataclass(frozen=True, eq=False)
class RelationCountExpr(ScalarExpr):
    spec: RelationSpec
