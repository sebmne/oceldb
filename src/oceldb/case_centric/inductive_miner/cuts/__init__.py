"""Cuts tried by the inductive miner, in priority order.

Each cut module exposes a single ``apply(dfg, recurse) -> ProcessTree | None``
function. The miner walks ``CUTS`` and returns the first non-None result.
"""

from oceldb.case_centric.inductive_miner.cuts import loop, parallel, sequence, xor

CUTS = (xor.apply, sequence.apply, parallel.apply, loop.apply)

__all__ = ["CUTS"]
