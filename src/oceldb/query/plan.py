from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal, assert_never

from oceldb.ast.base import BoolExpr, Expr, SortExpr
from oceldb.core.manifest import QuerySourceKind
from oceldb.core.ocel import OCEL

type ObjectStateMode = Literal["latest", "as_of"]


@dataclass(frozen=True)
class EventSource:
    selected_types: tuple[str, ...] = ()


@dataclass(frozen=True)
class ObjectSource:
    selected_types: tuple[str, ...] = ()


@dataclass(frozen=True)
class ObjectChangeSource:
    selected_types: tuple[str, ...] = ()


@dataclass(frozen=True)
class EventOccurrenceSource:
    selected_object_types: tuple[str, ...] = ()


@dataclass(frozen=True)
class ObjectStateSource:
    selected_types: tuple[str, ...] = ()
    mode: ObjectStateMode | None = None
    as_of: date | datetime | str | None = None


@dataclass(frozen=True)
class EventObjectSource:
    pass


@dataclass(frozen=True)
class ObjectObjectSource:
    pass


type SourceSpec = (
    EventSource
    | ObjectSource
    | ObjectChangeSource
    | EventOccurrenceSource
    | ObjectStateSource
    | EventObjectSource
    | ObjectObjectSource
)


@dataclass(frozen=True)
class SourcePlan:
    source: SourceSpec


@dataclass(frozen=True)
class FilterPlan:
    input: QueryPlanNode
    predicates: tuple[BoolExpr, ...]


@dataclass(frozen=True)
class HavingPlan:
    input: QueryPlanNode
    predicates: tuple[BoolExpr, ...]


@dataclass(frozen=True)
class ExtendPlan:
    input: QueryPlanNode
    assignments: tuple[Expr, ...]


@dataclass(frozen=True)
class ProjectPlan:
    input: QueryPlanNode
    projections: tuple[Expr, ...]


@dataclass(frozen=True)
class RenamePlan:
    input: QueryPlanNode
    renames: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class GroupPlan:
    input: QueryPlanNode
    keys: tuple[Expr, ...]
    aggregations: tuple[Expr, ...]


@dataclass(frozen=True)
class SortPlan:
    input: QueryPlanNode
    orderings: tuple[SortExpr, ...]


@dataclass(frozen=True)
class DistinctPlan:
    input: QueryPlanNode


@dataclass(frozen=True)
class LimitPlan:
    input: QueryPlanNode
    n: int


type QueryPlanNode = (
    SourcePlan
    | FilterPlan
    | HavingPlan
    | ExtendPlan
    | ProjectPlan
    | RenamePlan
    | GroupPlan
    | SortPlan
    | DistinctPlan
    | LimitPlan
)


@dataclass(frozen=True)
class QueryPlan:
    ocel: OCEL
    node: QueryPlanNode

    @classmethod
    def from_source(
        cls,
        ocel: OCEL,
        *,
        source: SourceSpec,
    ) -> "QueryPlan":
        return cls(ocel=ocel, node=SourcePlan(source))

    def with_root_source(self, source: SourceSpec) -> "QueryPlan":
        return QueryPlan(
            ocel=self.ocel,
            node=_replace_root_source(self.node, source),
        )


def root_source(node: QueryPlanNode) -> SourceSpec:
    match node:
        case SourcePlan(source=source):
            return source
        case FilterPlan(input=inner) | HavingPlan(input=inner) | ExtendPlan(input=inner) | ProjectPlan(input=inner) | RenamePlan(input=inner) | GroupPlan(input=inner) | SortPlan(input=inner) | DistinctPlan(input=inner) | LimitPlan(input=inner):
            return root_source(inner)
    assert_never(node)


def source_kind(source: SourceSpec) -> QuerySourceKind:
    match source:
        case EventSource():
            return "event"
        case ObjectSource():
            return "object"
        case ObjectChangeSource():
            return "object_change"
        case EventOccurrenceSource():
            return "event_occurrence"
        case ObjectStateSource():
            return "object_state"
        case EventObjectSource():
            return "event_object"
        case ObjectObjectSource():
            return "object_object"
    assert_never(source)


def selected_types(source: SourceSpec) -> tuple[str, ...]:
    match source:
        case EventSource(selected_types=types) | ObjectSource(selected_types=types) | ObjectChangeSource(selected_types=types) | ObjectStateSource(selected_types=types):
            return types
        case EventOccurrenceSource(selected_object_types=types):
            return types
        case EventObjectSource() | ObjectObjectSource():
            return ()
    assert_never(source)


def object_state_mode(source: SourceSpec) -> ObjectStateMode | None:
    if not isinstance(source, ObjectStateSource):
        return None
    return source.mode


def object_state_as_of(source: SourceSpec) -> date | datetime | str | None:
    if not isinstance(source, ObjectStateSource):
        return None
    return source.as_of


def contains_node(node: QueryPlanNode, node_types: tuple[type[object], ...]) -> bool:
    if isinstance(node, node_types):
        return True

    match node:
        case SourcePlan():
            return False
        case FilterPlan(input=inner) | HavingPlan(input=inner) | ExtendPlan(input=inner) | ProjectPlan(input=inner) | RenamePlan(input=inner) | GroupPlan(input=inner) | SortPlan(input=inner) | DistinctPlan(input=inner) | LimitPlan(input=inner):
            return contains_node(inner, node_types)
    assert_never(node)


def plan_depth(node: QueryPlanNode) -> int:
    match node:
        case SourcePlan():
            return 0
        case FilterPlan(input=inner) | HavingPlan(input=inner) | ExtendPlan(input=inner) | ProjectPlan(input=inner) | RenamePlan(input=inner) | GroupPlan(input=inner) | SortPlan(input=inner) | DistinctPlan(input=inner) | LimitPlan(input=inner):
            return plan_depth(inner) + 1
    assert_never(node)


def _replace_root_source(node: QueryPlanNode, source: SourceSpec) -> QueryPlanNode:
    match node:
        case SourcePlan():
            return SourcePlan(source)
        case FilterPlan(input=inner, predicates=predicates):
            return FilterPlan(_replace_root_source(inner, source), predicates)
        case HavingPlan(input=inner, predicates=predicates):
            return HavingPlan(_replace_root_source(inner, source), predicates)
        case ExtendPlan(input=inner, assignments=assignments):
            return ExtendPlan(_replace_root_source(inner, source), assignments)
        case ProjectPlan(input=inner, projections=projections):
            return ProjectPlan(_replace_root_source(inner, source), projections)
        case RenamePlan(input=inner, renames=renames):
            return RenamePlan(_replace_root_source(inner, source), renames)
        case GroupPlan(input=inner, keys=keys, aggregations=aggregations):
            return GroupPlan(_replace_root_source(inner, source), keys, aggregations)
        case SortPlan(input=inner, orderings=orderings):
            return SortPlan(_replace_root_source(inner, source), orderings)
        case DistinctPlan(input=inner):
            return DistinctPlan(_replace_root_source(inner, source))
        case LimitPlan(input=inner, n=n):
            return LimitPlan(_replace_root_source(inner, source), n)
    assert_never(node)
