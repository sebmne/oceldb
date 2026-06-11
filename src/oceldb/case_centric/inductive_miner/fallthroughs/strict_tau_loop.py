"""Strict tau-loop fallthrough.

Cuts a trace at every position where a start activity follows an end
activity, treating the suffix as a fresh execution. The fallthrough only
fires when this projection actually introduces extra trace segments.
"""

from oceldb.case_centric.inductive_miner._graph import Recurse
from oceldb.case_centric.inductive_miner.dfg import DirectlyFollowsGraph
from oceldb.case_centric.inductive_miner.fallthroughs._projection import (
    tau_loop_projection,
)
from oceldb.case_centric.inductive_miner.tree import ProcessTree


def apply(dfg: DirectlyFollowsGraph, recurse: Recurse) -> ProcessTree | None:
    projected = tau_loop_projection(dfg, require_previous_end=True)
    if projected is None:
        return None
    return ProcessTree.loop(recurse(projected), ProcessTree.tau())
