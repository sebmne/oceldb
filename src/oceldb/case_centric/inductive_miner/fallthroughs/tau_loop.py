"""Tau-loop fallthrough.

Like :mod:`strict_tau_loop` but does not require the previous activity to
be an end activity: any occurrence of a start activity mid-trace marks
a restart.
"""

from oceldb.case_centric.inductive_miner._graph import Recurse
from oceldb.case_centric.inductive_miner.dfg import DirectlyFollowsGraph
from oceldb.case_centric.inductive_miner.fallthroughs._projection import (
    tau_loop_projection,
)
from oceldb.case_centric.inductive_miner.tree import ProcessTree


def apply(dfg: DirectlyFollowsGraph, recurse: Recurse) -> ProcessTree | None:
    projected = tau_loop_projection(dfg, require_previous_end=False)
    if projected is None:
        return None
    return ProcessTree.loop(recurse(projected), ProcessTree.tau())
