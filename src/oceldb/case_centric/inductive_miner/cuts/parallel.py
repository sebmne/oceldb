"""Parallel cut.

Build the *negation* graph — connect two activities iff at least one of
the two DFG edges between them is missing. Connected components of that
graph are parallel branches, provided each branch contains at least one
start and one end activity.
"""

from itertools import combinations

import networkx as nx

from oceldb.case_centric.inductive_miner._graph import Recurse, components
from oceldb.case_centric.inductive_miner.dfg import DirectlyFollowsGraph
from oceldb.case_centric.inductive_miner.tree import ProcessTree


def apply(dfg: DirectlyFollowsGraph, recurse: Recurse) -> ProcessTree | None:
    partition = _find(dfg)
    if partition is None:
        return None
    children = tuple(recurse(dfg.project(part)) for part in partition)
    return ProcessTree.parallel(*children)


def _find(dfg: DirectlyFollowsGraph) -> tuple[frozenset[str], ...] | None:
    graph: nx.Graph[str] = nx.Graph()
    graph.add_nodes_from(dfg.activities)
    for left, right in combinations(sorted(dfg.activities), 2):
        l_to_r = (left, right) in dfg.edge_counts
        r_to_l = (right, left) in dfg.edge_counts
        if not (l_to_r and r_to_l):
            graph.add_edge(left, right)

    parts = components(nx.connected_components(graph))
    if len(parts) <= 1:
        return None

    starts, ends = set(dfg.start_counts), set(dfg.end_counts)
    if any(not (p & starts) or not (p & ends) for p in parts):
        return None
    return parts
