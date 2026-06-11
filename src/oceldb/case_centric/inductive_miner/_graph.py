"""NetworkX helpers shared by cuts and fallthroughs."""

from __future__ import annotations

from collections.abc import Callable, Iterable

import networkx as nx

from oceldb.case_centric.inductive_miner.dfg import DirectlyFollowsGraph
from oceldb.case_centric.inductive_miner.tree import ProcessTree

Recurse = Callable[[DirectlyFollowsGraph], ProcessTree]
"""The recursion callable cuts and fallthroughs use to mine sub-DFGs."""


def undirected(dfg: DirectlyFollowsGraph) -> nx.Graph[str]:
    graph: nx.Graph[str] = nx.Graph()
    graph.add_nodes_from(dfg.activities)
    graph.add_edges_from(dfg.edge_counts)
    return graph


def directed(dfg: DirectlyFollowsGraph) -> nx.DiGraph[str]:
    graph: nx.DiGraph[str] = nx.DiGraph()
    graph.add_nodes_from(dfg.activities)
    graph.add_edges_from(dfg.edge_counts)
    return graph


def components(parts: Iterable[set[str]]) -> tuple[frozenset[str], ...]:
    """Return component sets as a sorted tuple of frozensets for deterministic output."""
    return tuple(sorted((frozenset(c) for c in parts), key=lambda c: sorted(c)))
