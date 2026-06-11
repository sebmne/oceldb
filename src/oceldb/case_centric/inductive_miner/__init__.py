"""Inductive miner over case-centric event logs.

The miner is organised as a tiny composition loop:

* :mod:`base_cases` — empty log, single-activity (4 trace-variant shapes).
* :mod:`cuts` — ``xor``, ``sequence``, ``parallel``, ``loop``; one file each.
* :mod:`fallthroughs` — ``strict_tau_loop``, ``tau_loop``, ``flower``;
  one file each.

Only :func:`discover_petri_net` is part of the public API. The directly-
follows graph and the process tree are intermediate representations and
not exposed.
"""

from oceldb.case_centric.inductive_miner.petri_net import discover_petri_net

__all__ = ["discover_petri_net"]
