"""Inductive Miner over directly-follows graphs."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from itertools import combinations
from typing import Literal, cast

import networkx as nx

from oceldb.case_centric.dfg import DirectlyFollowsGraph, dfg_from_variants

ProcessTreeOperator = Literal["activity", "tau", "xor", "sequence", "parallel", "loop"]


@dataclass(frozen=True)
class ProcessTree:
    """A process tree discovered by the inductive miner."""

    operator: ProcessTreeOperator
    label: str | None = None
    children: tuple["ProcessTree", ...] = ()


def discover_process_tree(dfg: DirectlyFollowsGraph) -> ProcessTree:
    """Discover a process tree from a directly-follows graph.

    The mined tree is returned as-is — no structural rewrites are applied,
    so the result faithfully reflects the cuts taken by the inductive miner.
    Minimization is the job of the resulting Petri net (see
    :meth:`PetriNet.reduce_silent_transitions`).
    """
    return _mine(dfg)


def _mine(dfg: DirectlyFollowsGraph) -> ProcessTree:
    if not dfg.activities:
        return ProcessTree("tau")

    if dfg.has_empty_traces:
        return ProcessTree(
            "xor",
            children=(
                ProcessTree("tau"),
                _mine(dfg.without_empty_traces()),
            ),
        )

    if len(dfg.activities) == 1:
        activity = next(iter(dfg.activities))
        if (activity, activity) in dfg.edge_counts:
            return ProcessTree(
                "loop",
                children=(ProcessTree("activity", label=activity), ProcessTree("tau")),
            )
        return ProcessTree("activity", label=activity)

    cut = _xor_cut(dfg)
    if cut is not None:
        return _cut_tree("xor", dfg, cut)

    cut = _sequence_cut(dfg)
    if cut is not None:
        return _cut_tree("sequence", dfg, cut)

    cut = _parallel_cut(dfg)
    if cut is not None:
        return _cut_tree("parallel", dfg, cut)

    cut = _loop_cut(dfg)
    if cut is not None:
        return _cut_tree("loop", dfg, cut)

    fallthrough = _strict_tau_loop(dfg)
    if fallthrough is not None:
        return ProcessTree(
            "loop",
            children=(_mine(fallthrough), ProcessTree("tau")),
        )

    fallthrough = _tau_loop(dfg)
    if fallthrough is not None:
        return ProcessTree(
            "loop",
            children=(_mine(fallthrough), ProcessTree("tau")),
        )

    return _flower(dfg.activities)


def _cut_tree(
    operator: ProcessTreeOperator,
    dfg: DirectlyFollowsGraph,
    cut: tuple[frozenset[str], ...],
) -> ProcessTree:
    if operator == "loop":
        children = tuple(_mine(child) for child in _split_loop(dfg, cut))
    else:
        children = tuple(
            _mine(_project_for_cut(dfg, operator, partition)) for partition in cut
        )
    return ProcessTree(operator, children=children)


def _xor_cut(dfg: DirectlyFollowsGraph) -> tuple[frozenset[str], ...] | None:
    graph: nx.Graph[str] = nx.Graph()
    graph.add_nodes_from(dfg.activities)
    graph.add_edges_from(dfg.edge_counts)
    components = _components(nx.connected_components(graph))
    if len(components) <= 1:
        return None
    return components


def _sequence_cut(dfg: DirectlyFollowsGraph) -> tuple[frozenset[str], ...] | None:
    graph = _directed_graph(dfg)
    components: list[set[str]] = [
        set(component)
        for component in _components(nx.strongly_connected_components(graph))
    ]
    if len(components) <= 1:
        return None

    condensation: nx.DiGraph[int] = nx.condensation(graph, scc=components)
    closure: nx.DiGraph[int] = nx.transitive_closure_dag(condensation)
    component_ids = sorted(condensation.nodes)
    parent = {component_id: component_id for component_id in component_ids}

    def find(component_id: int) -> int:
        root = component_id
        while parent[root] != root:
            root = parent[root]
        while parent[component_id] != component_id:
            next_component_id = parent[component_id]
            parent[component_id] = root
            component_id = next_component_id
        return root

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root == right_root:
            return
        if left_root < right_root:
            parent[right_root] = left_root
        else:
            parent[left_root] = right_root

    for left, right in combinations(component_ids, 2):
        left_before_right = closure.has_edge(left, right)
        right_before_left = closure.has_edge(right, left)
        if not left_before_right and not right_before_left:
            union(left, right)

    groups: dict[int, set[int]] = {}
    for component_id in component_ids:
        groups.setdefault(find(component_id), set()).add(component_id)

    if len(groups) <= 1:
        return None

    group_graph: nx.DiGraph[int] = nx.DiGraph()
    group_graph.add_nodes_from(sorted(groups))
    for source, target in condensation.edges:
        source_group = find(source)
        target_group = find(target)
        if source_group != target_group:
            group_graph.add_edge(source_group, target_group)

    if not nx.is_directed_acyclic_graph(group_graph):
        return None

    cut: list[frozenset[str]] = []
    for group_id in cast(Iterable[int], nx.topological_sort(group_graph)):
        activities: set[str] = set()
        for component_id in sorted(groups[group_id]):
            activities.update(components[component_id])
        cut.append(frozenset(activities))

    if len(cut) <= 1 or any(part == dfg.activities for part in cut):
        return None
    return tuple(cut)


def _parallel_cut(dfg: DirectlyFollowsGraph) -> tuple[frozenset[str], ...] | None:
    graph: nx.Graph[str] = nx.Graph()
    graph.add_nodes_from(dfg.activities)

    for left, right in combinations(sorted(dfg.activities), 2):
        left_to_right = (left, right) in dfg.edge_counts
        right_to_left = (right, left) in dfg.edge_counts
        if not (left_to_right and right_to_left):
            graph.add_edge(left, right)

    components = _components(nx.connected_components(graph))
    if len(components) <= 1:
        return None
    starts = set(dfg.start_counts)
    ends = set(dfg.end_counts)
    if any(
        not (component & starts) or not (component & ends) for component in components
    ):
        return None
    return components


def _loop_cut(dfg: DirectlyFollowsGraph) -> tuple[frozenset[str], ...] | None:
    if not dfg.edge_counts:
        return None

    starts = set(dfg.start_counts)
    ends = set(dfg.end_counts)
    groups = [set(starts | ends)]
    groups.extend(set(component) for component in _loop_redo_components(dfg, groups[0]))

    changed = True
    while changed:
        changed = False
        changed |= _merge_loop_successors_of_starts(dfg, starts, ends, groups)
        changed |= _merge_loop_predecessors_of_ends(dfg, starts, ends, groups)
        changed |= _merge_loop_start_incomplete(dfg, starts, groups)
        changed |= _merge_loop_end_incomplete(dfg, ends, groups)

    groups = [group for group in groups if group]
    if len(groups) <= 1:
        return None

    redo: set[str] = set()
    for group in groups[1:]:
        redo.update(group)
    if not redo:
        return None

    return frozenset(groups[0]), frozenset(redo)


def _loop_redo_components(
    dfg: DirectlyFollowsGraph,
    boundary: set[str],
) -> tuple[frozenset[str], ...]:
    graph: nx.Graph[str] = nx.Graph()
    graph.add_nodes_from(dfg.activities - boundary)
    for source, target in dfg.edge_counts:
        if source not in boundary and target not in boundary:
            graph.add_edge(source, target)
    return _components(nx.connected_components(graph))


def _merge_loop_successors_of_starts(
    dfg: DirectlyFollowsGraph,
    starts: set[str],
    ends: set[str],
    groups: list[set[str]],
) -> bool:
    for start in sorted(starts - ends):
        for source, target in sorted(dfg.edge_counts):
            if source != start:
                continue
            if _merge_loop_group_into_body(groups, target):
                return True
    return False


def _merge_loop_predecessors_of_ends(
    dfg: DirectlyFollowsGraph,
    starts: set[str],
    ends: set[str],
    groups: list[set[str]],
) -> bool:
    for end in sorted(ends - starts):
        for source, target in sorted(dfg.edge_counts):
            if target != end:
                continue
            if _merge_loop_group_into_body(groups, source):
                return True
    return False


def _merge_loop_start_incomplete(
    dfg: DirectlyFollowsGraph,
    starts: set[str],
    groups: list[set[str]],
) -> bool:
    for group in groups[1:]:
        for activity in sorted(group):
            outgoing_starts = {
                target
                for source, target in dfg.edge_counts
                if source == activity and target in starts
            }
            if outgoing_starts and outgoing_starts != starts:
                groups[0].update(group)
                group.clear()
                return True
    return False


def _merge_loop_end_incomplete(
    dfg: DirectlyFollowsGraph,
    ends: set[str],
    groups: list[set[str]],
) -> bool:
    for group in groups[1:]:
        for activity in sorted(group):
            incoming_ends = {
                source
                for source, target in dfg.edge_counts
                if source in ends and target == activity
            }
            if incoming_ends and incoming_ends != ends:
                groups[0].update(group)
                group.clear()
                return True
    return False


def _merge_loop_group_into_body(groups: list[set[str]], activity: str) -> bool:
    for index, group in enumerate(groups):
        if activity not in group:
            continue
        if index == 0:
            return False
        groups[0].update(group)
        group.clear()
        return True
    return False


def _strict_tau_loop(dfg: DirectlyFollowsGraph) -> DirectlyFollowsGraph | None:
    return _tau_loop_projection(dfg, require_previous_end=True)


def _tau_loop(dfg: DirectlyFollowsGraph) -> DirectlyFollowsGraph | None:
    return _tau_loop_projection(dfg, require_previous_end=False)


def _tau_loop_projection(
    dfg: DirectlyFollowsGraph,
    *,
    require_previous_end: bool,
) -> DirectlyFollowsGraph | None:
    starts = set(dfg.start_counts)
    ends = set(dfg.end_counts)
    if not starts:
        return None

    variants: Counter[tuple[str, ...]] = Counter()
    original_count = sum(dfg.variants.values())
    projected_count = 0
    for variant, count in dfg.variants.items():
        if not variant:
            continue

        segment_start = 0
        for index in range(1, len(variant)):
            is_restart = variant[index] in starts
            follows_end = variant[index - 1] in ends
            if is_restart and (follows_end or not require_previous_end):
                variants[variant[segment_start:index]] += count
                projected_count += count
                segment_start = index

        variants[variant[segment_start:]] += count
        projected_count += count

    if projected_count <= original_count:
        return None
    return dfg_from_variants(variants, threshold=dfg.threshold)


def _flower(activities: frozenset[str]) -> ProcessTree:
    return ProcessTree(
        "loop",
        children=(
            ProcessTree("tau"),
            ProcessTree(
                "xor",
                children=tuple(
                    ProcessTree("activity", label=activity)
                    for activity in sorted(activities)
                ),
            ),
        ),
    )


def _directed_graph(dfg: DirectlyFollowsGraph) -> nx.DiGraph[str]:
    graph: nx.DiGraph[str] = nx.DiGraph()
    graph.add_nodes_from(dfg.activities)
    graph.add_edges_from(dfg.edge_counts)
    return graph


def _components(components: Iterable[set[str]]) -> tuple[frozenset[str], ...]:
    return tuple(sorted((frozenset(c) for c in components), key=lambda c: sorted(c)))


def _project_for_cut(
    dfg: DirectlyFollowsGraph,
    operator: ProcessTreeOperator,
    activities: Iterable[str],
) -> DirectlyFollowsGraph:
    if operator != "xor":
        return dfg.project(activities)

    selected = set(activities)
    variants: Counter[tuple[str, ...]] = Counter()
    for variant, count in dfg.variants.items():
        projected = tuple(activity for activity in variant if activity in selected)
        if projected:
            variants[projected] += count

    return dfg_from_variants(variants, threshold=dfg.threshold)


def _split_loop(
    dfg: DirectlyFollowsGraph,
    cut: tuple[frozenset[str], ...],
) -> tuple[DirectlyFollowsGraph, ...]:
    partition_by_activity: dict[str, int] = {}
    for index, activities in enumerate(cut):
        for activity in activities:
            partition_by_activity[activity] = index

    variants_by_partition: list[Counter[tuple[str, ...]]] = [Counter() for _ in cut]
    for variant, count in dfg.variants.items():
        current_partition: int | None = None
        segment: list[str] = []
        for activity in variant:
            partition = partition_by_activity[activity]
            if current_partition is not None and partition != current_partition:
                variants_by_partition[current_partition][tuple(segment)] += count
                segment = []
            current_partition = partition
            segment.append(activity)

        if current_partition is not None and segment:
            variants_by_partition[current_partition][tuple(segment)] += count

    return tuple(
        dfg_from_variants(variants, threshold=dfg.threshold)
        for variants in variants_by_partition
    )
