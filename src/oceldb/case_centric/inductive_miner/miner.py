"""Inductive-miner composition.

A single recursion ties everything together:

1. Try the base cases (empty log, single activity).
2. Peel off empty traces by wrapping the rest in ``xor(tau, …)``.
3. Try each cut in :data:`CUTS` in order.
4. Try each fallthrough in :data:`FALLTHROUGHS` — the last one (``flower``)
   always succeeds.
"""

from oceldb.case_centric.inductive_miner import base_cases
from oceldb.case_centric.inductive_miner.cuts import CUTS
from oceldb.case_centric.inductive_miner.dfg import (
    DirectlyFollowsGraph,
    dfg_from_log,
)
from oceldb.case_centric.inductive_miner.fallthroughs import FALLTHROUGHS
from oceldb.case_centric.inductive_miner.tree import ProcessTree
from oceldb.case_centric.types import CaseCentricEventLog


def discover_process_tree(
    case_log: CaseCentricEventLog,
    *,
    case_id: str = "case:concept:name",
    activity: str = "concept:name",
    timestamp: str = "time:timestamp",
    event_id: str = "ocel_event_id",
    threshold: float = 0.0,
) -> ProcessTree:
    """Discover a process tree from a case-centric event log.

    ``threshold`` is the relative noise-filtering parameter in ``[0, 1]``
    applied to the underlying directly-follows graph. ``0.0`` keeps every
    activity and edge; higher values drop infrequent behaviour.
    """
    dfg = dfg_from_log(
        case_log,
        case_id=case_id,
        activity=activity,
        timestamp=timestamp,
        event_id=event_id,
        threshold=threshold,
    )
    return mine(dfg)


def mine(dfg: DirectlyFollowsGraph) -> ProcessTree:
    """Run the inductive-miner recursion on a directly-follows graph."""
    tree = base_cases.apply(dfg)
    if tree is not None:
        return tree

    if dfg.has_empty_traces:
        return ProcessTree.xor(
            ProcessTree.tau(),
            mine(dfg.without_empty_traces()),
        )

    for cut in CUTS:
        tree = cut(dfg, mine)
        if tree is not None:
            return tree

    for fallthrough in FALLTHROUGHS:
        tree = fallthrough(dfg, mine)
        if tree is not None:
            return tree

    raise RuntimeError("flower fallthrough should always succeed")
