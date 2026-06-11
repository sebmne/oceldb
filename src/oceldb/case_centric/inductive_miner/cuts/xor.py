"""Exclusive-choice cut.

Partition the activities by connected components of the (undirected) DFG.
Each component becomes a child of the resulting ``xor`` node.
"""

from collections import Counter

import networkx as nx

from oceldb.case_centric.inductive_miner._graph import Recurse, components, undirected
from oceldb.case_centric.inductive_miner.dfg import (
    DirectlyFollowsGraph,
    dfg_from_variants,
)
from oceldb.case_centric.inductive_miner.tree import ProcessTree


def apply(dfg: DirectlyFollowsGraph, recurse: Recurse) -> ProcessTree | None:
    partition = _find(dfg)
    if partition is None:
        return None
    children = tuple(recurse(_project(dfg, part)) for part in partition)
    return ProcessTree.xor(*children)


def _find(dfg: DirectlyFollowsGraph) -> tuple[frozenset[str], ...] | None:
    parts = components(nx.connected_components(undirected(dfg)))
    if len(parts) <= 1:
        return None
    return parts


def _project(
    dfg: DirectlyFollowsGraph, activities: frozenset[str]
) -> DirectlyFollowsGraph:
    """Project variants to one xor branch.

    Empty projections are dropped because xor branches see only the traces
    in which their activities actually occur — other branches absorb the rest.
    """
    variants: Counter[tuple[str, ...]] = Counter()
    for variant, count in dfg.variants.items():
        projected = tuple(a for a in variant if a in activities)
        if projected:
            variants[projected] += count
    return dfg_from_variants(variants, threshold=dfg.threshold)
