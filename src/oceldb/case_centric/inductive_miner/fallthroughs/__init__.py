"""Fallthroughs tried by the inductive miner, in priority order.

Each fallthrough module exposes a single
``apply(dfg, recurse) -> ProcessTree | None`` function. The miner walks
``FALLTHROUGHS`` after no cut applied and returns the first non-None
result. ``flower`` is the last entry — it always succeeds.
"""

from oceldb.case_centric.inductive_miner.fallthroughs import (
    flower,
    strict_tau_loop,
    tau_loop,
)

FALLTHROUGHS = (strict_tau_loop.apply, tau_loop.apply, flower.apply)

__all__ = ["FALLTHROUGHS"]
