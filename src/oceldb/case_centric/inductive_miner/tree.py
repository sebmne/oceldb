"""Process tree data type."""

from dataclasses import dataclass
from typing import Literal

ProcessTreeOperator = Literal["activity", "tau", "xor", "sequence", "parallel", "loop"]


@dataclass(frozen=True)
class ProcessTree:
    """A process tree discovered by the inductive miner.

    The mined tree is returned as-is: no structural rewrites are applied,
    so the result faithfully reflects the cuts and fallthroughs taken by
    the miner. Minimisation is the job of the resulting Petri net (see
    :meth:`PetriNet.reduce_silent_transitions`).
    """

    operator: ProcessTreeOperator
    label: str | None = None
    children: tuple["ProcessTree", ...] = ()

    @classmethod
    def activity(cls, label: str) -> "ProcessTree":
        return ProcessTree("activity", label=label)

    @classmethod
    def tau(cls) -> "ProcessTree":
        return ProcessTree("tau")

    @classmethod
    def xor(cls, *children: "ProcessTree") -> "ProcessTree":
        return ProcessTree("xor", children=children)

    @classmethod
    def sequence(cls, *children: "ProcessTree") -> "ProcessTree":
        return ProcessTree("sequence", children=children)

    @classmethod
    def parallel(cls, *children: "ProcessTree") -> "ProcessTree":
        return ProcessTree("parallel", children=children)

    @classmethod
    def loop(cls, body: "ProcessTree", redo: "ProcessTree") -> "ProcessTree":
        return ProcessTree("loop", children=(body, redo))
