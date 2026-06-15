"""Sequence cut.

Condense the DFG by strongly connected components, then merge SCCs that
sit on parallel paths in the condensation (no transitive precedence
between them). The remaining order across merged groups is the sequence.

After the maximal partition is found, a *strict* merge step folds together
adjacent groups that together can be skipped — see
:func:`_apply_strict_merge`. This matches the formulation on page 233 of
Leemans (2018), *Robust Process Mining with Guarantees*, and lines up with
pm4py's behaviour.
"""

import math
from collections.abc import Iterable
from itertools import combinations, product
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

    partition: list[set[str]] = []
    for gid in cast(Iterable[int], nx.topological_sort(group_graph)):
        acts: set[str] = set()
        for cid in sorted(groups[gid]):
            acts.update(scc[cid])
        partition.append(acts)

    partition = _apply_strict_merge(partition, dfg)

    if len(partition) <= 1 or any(
        frozenset(part) == dfg.activities for part in partition
    ):
        return None
    return tuple(frozenset(part) for part in partition)


def _apply_strict_merge(
    partition: list[set[str]], dfg: DirectlyFollowsGraph
) -> list[set[str]]:
    """Merge skippable groups into their neighbours.

    Implements the *strict* sequence cut from Leemans (2018, p. 233): a
    group is skippable when activities in earlier groups have a direct
    edge to activities in later groups (bypassing the group), when a
    later group contains a start activity, or when an earlier group
    contains an end activity. Skippable groups are absorbed into the
    neighbour they can reach (``mt``) or be reached from (``mf``) so the
    surviving partitions only split where the log truly enforces an
    order.
    """
    starts = set(dfg.start_counts)
    ends = set(dfg.end_counts)
    edges = set(dfg.edge_counts)

    cmap: dict[str, int] = {act: idx for idx, group in enumerate(partition) for act in group}
    n = len(partition)
    mf = [-math.inf if group & starts else math.inf for group in partition]
    mt = [math.inf if group & ends else -math.inf for group in partition]
    for src, tgt in edges:
        i, j = cmap[src], cmap[tgt]
        if mf[j] > i:
            mf[j] = i
        if mt[i] < j:
            mt[i] = j

    for p in range(n):
        if not partition[p]:
            continue
        if not _skippable(p, partition, starts, ends, edges):
            continue
        q = p - 1
        while q >= 0 and mt[q] <= p:
            partition[p].update(partition[q])
            partition[q] = set()
            q -= 1
        q = p + 1
        while q < n and mf[q] >= p:
            partition[p].update(partition[q])
            partition[q] = set()
            q += 1

    return [group for group in partition if group]


def _skippable(
    p: int,
    partition: list[set[str]],
    starts: set[str],
    ends: set[str],
    edges: set[tuple[str, str]],
) -> bool:
    for i, j in product(range(p), range(p + 1, len(partition))):
        for a, b in product(partition[i], partition[j]):
            if (a, b) in edges:
                return True
    for j in range(p + 1, len(partition)):
        if partition[j] & starts:
            return True
    for i in range(p):
        if partition[i] & ends:
            return True
    return False
