"""Base cases that short-circuit the inductive-miner recursion.

Every iteration adds an operator node and one arc
per partition, but two situations let the miner add a complete subtree on
the spot:

* an empty sublog - a silent (tau) leaf.
* a single-activity sublog ``{a}`` - four shapes distinguished by the
  trace variants present in the sublog:

  ===========================  =====================================
  Variants in sublog            Process tree
  ===========================  =====================================
  ``{<a>}``                     ``activity(a)``
  ``{<>, <a>}``                 ``xor(activity(a), tau)``
  ``{<a>, <a,a>, ...}``         ``loop(activity(a), tau)``
  ``{<>, <a>, <a,a>, ...}``     ``loop(tau, activity(a))``
  ===========================  =====================================
"""

from typing import Optional

from oceldb.case_centric.inductive_miner.dfg import DirectlyFollowsGraph
from oceldb.case_centric.inductive_miner.tree import ProcessTree


def apply(dfg: DirectlyFollowsGraph) -> Optional[ProcessTree]:
    """Return a base-case subtree if one applies, else None."""
    if not dfg.activities:
        return ProcessTree.tau()
    if len(dfg.activities) == 1:
        return _single_activity(dfg)
    return None


def _single_activity(dfg: DirectlyFollowsGraph) -> ProcessTree:
    label = next(iter(dfg.activities))
    has_empty = dfg.has_empty_traces
    has_repeat = (label, label) in dfg.edge_counts
    activity = ProcessTree.activity(label)

    if not has_empty and not has_repeat:
        return activity
    if has_empty and not has_repeat:
        return ProcessTree.xor(activity, ProcessTree.tau())
    if not has_empty and has_repeat:
        return ProcessTree.loop(activity, ProcessTree.tau())
    return ProcessTree.loop(ProcessTree.tau(), activity)
