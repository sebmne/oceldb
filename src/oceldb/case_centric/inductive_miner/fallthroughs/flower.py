"""Flower fallthrough.

Last-resort model that lets any activity execute in any order. Always
succeeds, so it terminates the miner's search.
"""

from oceldb.case_centric.inductive_miner._graph import Recurse
from oceldb.case_centric.inductive_miner.dfg import DirectlyFollowsGraph
from oceldb.case_centric.inductive_miner.tree import ProcessTree


def apply(dfg: DirectlyFollowsGraph, _recurse: Recurse) -> ProcessTree:
    leaves = tuple(ProcessTree.activity(a) for a in sorted(dfg.activities))
    return ProcessTree.loop(ProcessTree.tau(), ProcessTree.xor(*leaves))
