"""Plan IR — the operator tree produced by the DSL and consumed by compile/.

Every node is a frozen dataclass with an ``input`` edge (except the leaf
``SourcePlan``). Nodes expose an ``input`` attribute so generic walkers can
descend without matching on type. A ``PlanVisitor[T]`` with a
``generic_visit`` default lets the compiler handle nodes it cares about and
ignore the rest.
"""

from __future__ import annotations

from abc import ABC
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Generic, TypeVar

from oceldb.expr.nodes import Expr, SortExpr
from oceldb.plan.sources import Source

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------


class PlanNode(ABC):
    """Marker base for all plan nodes."""

    __slots__ = ()

    def children(self) -> Iterable["PlanNode"]:
        return ()


# ---------------------------------------------------------------------------
# Leaf
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SourcePlan(PlanNode):
    source: Source


# ---------------------------------------------------------------------------
# Unary nodes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FilterPlan(PlanNode):
    input: PlanNode
    predicates: tuple[Expr, ...]

    def children(self) -> Iterable[PlanNode]:
        return (self.input,)


@dataclass(frozen=True)
class HavingPlan(PlanNode):
    input: PlanNode
    predicates: tuple[Expr, ...]

    def children(self) -> Iterable[PlanNode]:
        return (self.input,)


@dataclass(frozen=True)
class ExtendPlan(PlanNode):
    """Append new aliased columns (``with_columns``)."""

    input: PlanNode
    assignments: tuple[Expr, ...]

    def children(self) -> Iterable[PlanNode]:
        return (self.input,)


@dataclass(frozen=True)
class ProjectPlan(PlanNode):
    """Replace output columns (``select``)."""

    input: PlanNode
    projections: tuple[Expr, ...]

    def children(self) -> Iterable[PlanNode]:
        return (self.input,)


@dataclass(frozen=True)
class RenamePlan(PlanNode):
    input: PlanNode
    renames: tuple[tuple[str, str], ...]

    def children(self) -> Iterable[PlanNode]:
        return (self.input,)


@dataclass(frozen=True)
class GroupPlan(PlanNode):
    input: PlanNode
    keys: tuple[Expr, ...]
    aggregations: tuple[Expr, ...]

    def children(self) -> Iterable[PlanNode]:
        return (self.input,)


@dataclass(frozen=True)
class SortPlan(PlanNode):
    input: PlanNode
    orderings: tuple[SortExpr, ...]

    def children(self) -> Iterable[PlanNode]:
        return (self.input,)


@dataclass(frozen=True)
class DistinctPlan(PlanNode):
    input: PlanNode

    def children(self) -> Iterable[PlanNode]:
        return (self.input,)


@dataclass(frozen=True)
class LimitPlan(PlanNode):
    input: PlanNode
    n: int

    def children(self) -> Iterable[PlanNode]:
        return (self.input,)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def root_source(node: PlanNode) -> Source:
    """Walk to the leaf ``SourcePlan`` and return its ``Source``."""
    current: PlanNode = node
    while not isinstance(current, SourcePlan):
        current = next(iter(current.children()))
    return current.source


def plan_depth(node: PlanNode) -> int:
    depth = 0
    current: PlanNode = node
    while not isinstance(current, SourcePlan):
        depth += 1
        current = next(iter(current.children()))
    return depth


def contains_node(node: PlanNode, node_types: tuple[type[PlanNode], ...]) -> bool:
    if isinstance(node, node_types):
        return True
    return any(contains_node(child, node_types) for child in node.children())


# ---------------------------------------------------------------------------
# Visitor
# ---------------------------------------------------------------------------


class PlanVisitor(Generic[T]):
    """Visitor with ``generic_visit`` default recursion.

    Subclasses override ``visit_<ClassName>`` methods for nodes they care
    about. Anything unhandled falls back to ``generic_visit``, which recurses
    into ``children()``.
    """

    def visit(self, node: PlanNode) -> T:
        method = getattr(self, f"visit_{type(node).__name__}", None)
        if method is None:
            return self.generic_visit(node)
        return method(node)  # type: ignore[no-any-return]

    def generic_visit(self, node: PlanNode) -> T:
        for child in node.children():
            self.visit(child)
        return None  # type: ignore[return-value]


__all__ = [
    "DistinctPlan",
    "ExtendPlan",
    "FilterPlan",
    "GroupPlan",
    "HavingPlan",
    "LimitPlan",
    "PlanNode",
    "PlanVisitor",
    "ProjectPlan",
    "RenamePlan",
    "SortPlan",
    "SourcePlan",
    "contains_node",
    "plan_depth",
    "root_source",
]
