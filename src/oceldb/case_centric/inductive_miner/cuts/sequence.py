"""Sequence cut.

Condense the DFG by strongly connected components, then merge SCCs that
sit on parallel paths in the condensation (no transitive precedence
between them). The remaining order across merged groups is the sequence.
"""

from collections.abc import Iterable
from itertools import combinations
from typing import cast

import networkx as nx

from oceldb.case_centric.inductive_miner._graph import Recurse, components, directed
from oceldb.case_centric.inductive_miner.dfg import DirectlyFollowsGraph
from oceldb.case_centric.inductive_miner.tree import ProcessTree


def apply(dfg: DirectlyFollowsGraph, recurse: Recurse) -> ProcessTree | None:
    partition = _find(dfg)
    if partition is None:
        return None
    children = tuple(recurse(dfg.project(part)) for part in partition)
    return ProcessTree.sequence(*children)


def _find(dfg: DirectlyFollowsGraph) -> tuple[frozenset[str], ...] | None:
    graph = directed(dfg)
    scc: list[set[str]] = [
        set(c) for c in components(nx.strongly_connected_components(graph))
    ]
    if len(scc) <= 1:
        return None

    condensation: nx.DiGraph[int] = nx.condensation(graph, scc=scc)
    closure: nx.DiGraph[int] = nx.transitive_closure_dag(condensation)
    ids = sorted(condensation.nodes)
    parent = {cid: cid for cid in ids}

    def find(cid: int) -> int:
        root = cid
        while parent[root] != root:
            root = parent[root]
        while parent[cid] != cid:
            nxt = parent[cid]
            parent[cid] = root
            cid = nxt
        return root

    def union(left: int, right: int) -> None:
        lr, rr = find(left), find(right)
        if lr == rr:
            return
        if lr < rr:
            parent[rr] = lr
        else:
            parent[lr] = rr

    for left, right in combinations(ids, 2):
        if not closure.has_edge(left, right) and not closure.has_edge(right, left):
            union(left, right)

    groups: dict[int, set[int]] = {}
    for cid in ids:
        groups.setdefault(find(cid), set()).add(cid)
    if len(groups) <= 1:
        return None

    group_graph: nx.DiGraph[int] = nx.DiGraph()
    group_graph.add_nodes_from(sorted(groups))
    for src, tgt in condensation.edges:
        sg, tg = find(src), find(tgt)
        if sg != tg:
            group_graph.add_edge(sg, tg)

    if not nx.is_directed_acyclic_graph(group_graph):
        return None

    partition: list[frozenset[str]] = []
    for gid in cast(Iterable[int], nx.topological_sort(group_graph)):
        acts: set[str] = set()
        for cid in sorted(groups[gid]):
            acts.update(scc[cid])
        partition.append(frozenset(acts))

    if len(partition) <= 1 or any(part == dfg.activities for part in partition):
        return None
    return tuple(partition)
