"""Loop cut.

Group 0 is the loop *body* (it always contains the start and end activities
of the DFG). Groups 1..n are *redo* parts. The miner merges any redo group
back into the body when its activities can reach/be reached by starts/ends
in ways that contradict a clean ``do…redo…`` shape.

Projection splits each trace into segments whenever the active partition
changes; the resulting per-partition DFGs are mined recursively.
"""

from collections import Counter

import networkx as nx

from oceldb.case_centric.inductive_miner._graph import Recurse, components
from oceldb.case_centric.inductive_miner.dfg import (
    DirectlyFollowsGraph,
    dfg_from_variants,
)
from oceldb.case_centric.inductive_miner.tree import ProcessTree


def apply(dfg: DirectlyFollowsGraph, recurse: Recurse) -> ProcessTree | None:
    partition = _find(dfg)
    if partition is None:
        return None
    children = tuple(recurse(sub) for sub in _project(dfg, partition))
    return ProcessTree.loop(*children)


def _find(dfg: DirectlyFollowsGraph) -> tuple[frozenset[str], ...] | None:
    if not dfg.edge_counts:
        return None

    starts, ends = set(dfg.start_counts), set(dfg.end_counts)
    groups = [set(starts | ends)]
    groups.extend(set(c) for c in _redo_components(dfg, groups[0]))

    changed = True
    while changed:
        changed = False
        changed |= _merge_successors_of_starts(dfg, starts, ends, groups)
        changed |= _merge_predecessors_of_ends(dfg, starts, ends, groups)
        changed |= _merge_start_incomplete(dfg, starts, groups)
        changed |= _merge_end_incomplete(dfg, ends, groups)

    groups = [g for g in groups if g]
    if len(groups) <= 1:
        return None

    redo: set[str] = set()
    for g in groups[1:]:
        redo.update(g)
    if not redo:
        return None
    return frozenset(groups[0]), frozenset(redo)


def _redo_components(
    dfg: DirectlyFollowsGraph, boundary: set[str]
) -> tuple[frozenset[str], ...]:
    graph: nx.Graph[str] = nx.Graph()
    graph.add_nodes_from(dfg.activities - boundary)
    for src, tgt in dfg.edge_counts:
        if src not in boundary and tgt not in boundary:
            graph.add_edge(src, tgt)
    return components(nx.connected_components(graph))


def _merge_successors_of_starts(
    dfg: DirectlyFollowsGraph,
    starts: set[str],
    ends: set[str],
    groups: list[set[str]],
) -> bool:
    for start in sorted(starts - ends):
        for src, tgt in sorted(dfg.edge_counts):
            if src != start:
                continue
            if _merge_into_body(groups, tgt):
                return True
    return False


def _merge_predecessors_of_ends(
    dfg: DirectlyFollowsGraph,
    starts: set[str],
    ends: set[str],
    groups: list[set[str]],
) -> bool:
    for end in sorted(ends - starts):
        for src, tgt in sorted(dfg.edge_counts):
            if tgt != end:
                continue
            if _merge_into_body(groups, src):
                return True
    return False


def _merge_start_incomplete(
    dfg: DirectlyFollowsGraph, starts: set[str], groups: list[set[str]]
) -> bool:
    for group in groups[1:]:
        for act in sorted(group):
            outgoing = {
                tgt for src, tgt in dfg.edge_counts if src == act and tgt in starts
            }
            if outgoing and outgoing != starts:
                groups[0].update(group)
                group.clear()
                return True
    return False


def _merge_end_incomplete(
    dfg: DirectlyFollowsGraph, ends: set[str], groups: list[set[str]]
) -> bool:
    for group in groups[1:]:
        for act in sorted(group):
            incoming = {
                src for src, tgt in dfg.edge_counts if src in ends and tgt == act
            }
            if incoming and incoming != ends:
                groups[0].update(group)
                group.clear()
                return True
    return False


def _merge_into_body(groups: list[set[str]], activity: str) -> bool:
    for index, group in enumerate(groups):
        if activity not in group:
            continue
        if index == 0:
            return False
        groups[0].update(group)
        group.clear()
        return True
    return False


def _project(
    dfg: DirectlyFollowsGraph, partition: tuple[frozenset[str], ...]
) -> tuple[DirectlyFollowsGraph, ...]:
    partition_of: dict[str, int] = {}
    for index, acts in enumerate(partition):
        for act in acts:
            partition_of[act] = index

    per_partition: list[Counter[tuple[str, ...]]] = [Counter() for _ in partition]
    for variant, count in dfg.variants.items():
        current: int | None = None
        segment: list[str] = []
        for act in variant:
            part = partition_of[act]
            if current is not None and part != current:
                per_partition[current][tuple(segment)] += count
                segment = []
            current = part
            segment.append(act)
        if current is not None and segment:
            per_partition[current][tuple(segment)] += count

    return tuple(dfg_from_variants(v, threshold=dfg.threshold) for v in per_partition)
