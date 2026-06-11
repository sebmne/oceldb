"""Petri-net synthesis on top of the inductive-miner process tree."""

from oceldb.case_centric.inductive_miner.miner import discover_process_tree
from oceldb.case_centric.inductive_miner.tree import ProcessTree
from oceldb.case_centric.types import CaseCentricEventLog
from oceldb.models import PetriNet


def discover_petri_net(
    case_log: CaseCentricEventLog,
    *,
    case_id: str = "case:concept:name",
    activity: str = "concept:name",
    timestamp: str = "time:timestamp",
    event_id: str = "ocel_event_id",
    threshold: float = 0.0,
    simplify: bool = True,
) -> PetriNet:
    """Discover a (case-centric) Petri net from a case-centric event log.

    Runs the inductive miner end-to-end: builds the directly-follows graph,
    derives a process tree, and synthesizes the corresponding workflow net.
    When ``simplify`` is true (the default), redundant silent transitions
    are eliminated via :meth:`PetriNet.reduce_silent_transitions`.

    The resulting net uses the default object type. Embedding it into an
    object-centric Petri net is the caller's responsibility (see
    :func:`oceldb.discovery.discover_ocpn`).
    """
    tree = discover_process_tree(
        case_log,
        case_id=case_id,
        activity=activity,
        timestamp=timestamp,
        event_id=event_id,
        threshold=threshold,
    )
    return synthesize(tree, simplify=simplify)


def synthesize(tree: ProcessTree, *, simplify: bool = True) -> PetriNet:
    """Translate a process tree into a workflow net."""
    net = PetriNet()
    builder = _Builder(net)
    source = builder.add_place(("source",), initial=True)
    sink = builder.add_place(("sink",), final=True)
    builder.translate(tree, source, sink, ("root",))

    if simplify:
        net.reduce_silent_transitions()
    return net


class _Builder:
    """Translates a :class:`ProcessTree` into a workflow-net subnet."""

    def __init__(self, net: PetriNet) -> None:
        self._net = net

    def translate(
        self,
        tree: ProcessTree,
        source: str,
        target: str,
        path: tuple[str, ...],
    ) -> None:
        handler = getattr(self, f"_handle_{tree.operator}")
        handler(tree, source, target, path)

    def _handle_activity(
        self, tree: ProcessTree, source: str, target: str, path: tuple[str, ...]
    ) -> None:
        if tree.label is None:
            raise ValueError("Activity process-tree nodes require a label.")
        transition = self._add_visible_transition(tree.label)
        self._connect(source, transition)
        self._connect(transition, target)

    def _handle_tau(
        self, _tree: ProcessTree, source: str, target: str, path: tuple[str, ...]
    ) -> None:
        transition = self._add_silent_transition(path)
        self._connect(source, transition)
        self._connect(transition, target)

    def _handle_sequence(
        self, tree: ProcessTree, source: str, target: str, path: tuple[str, ...]
    ) -> None:
        if not tree.children:
            self._handle_tau(tree, source, target, (*path, "empty"))
            return

        current = source
        for index, child in enumerate(tree.children):
            is_last = index == len(tree.children) - 1
            next_target = target if is_last else self.add_place((*path, f"seq_{index}"))
            self.translate(child, current, next_target, (*path, f"child_{index}"))
            current = next_target

    def _handle_xor(
        self, tree: ProcessTree, source: str, target: str, path: tuple[str, ...]
    ) -> None:
        if not tree.children:
            self._handle_tau(tree, source, target, (*path, "empty"))
            return

        for index, child in enumerate(tree.children):
            self.translate(child, source, target, (*path, f"branch_{index}"))

    def _handle_parallel(
        self, tree: ProcessTree, source: str, target: str, path: tuple[str, ...]
    ) -> None:
        if not tree.children:
            self._handle_tau(tree, source, target, (*path, "empty"))
            return

        split = self._add_silent_transition((*path, "split"))
        join = self._add_silent_transition((*path, "join"))
        self._connect(source, split)
        self._connect(join, target)

        for index, child in enumerate(tree.children):
            branch_source = self.add_place((*path, f"branch_{index}", "source"))
            branch_target = self.add_place((*path, f"branch_{index}", "target"))
            self._connect(split, branch_source)
            self.translate(
                child, branch_source, branch_target, (*path, f"branch_{index}")
            )
            self._connect(branch_target, join)

    def _handle_loop(
        self, tree: ProcessTree, source: str, target: str, path: tuple[str, ...]
    ) -> None:
        if not tree.children:
            self._handle_tau(tree, source, target, (*path, "empty"))
            return

        body = tree.children[0]
        redo = (
            tree.children[1]
            if len(tree.children) == 2
            else ProcessTree("xor", children=tree.children[1:])
        )
        entry = self._loop_entry_place(source, path)

        if body.operator == "tau":
            leave = self._add_silent_transition((*path, "leave"))
            self._connect(entry, leave)
            self._connect(leave, target)
            self.translate(redo, entry, entry, (*path, "redo"))
            return

        body_target = self.add_place((*path, "body_target"))
        leave = self._add_silent_transition((*path, "leave"))
        self.translate(body, entry, body_target, (*path, "body"))
        self._connect(body_target, leave)
        self._connect(leave, target)
        self.translate(redo, body_target, entry, (*path, "redo"))

    def _loop_entry_place(self, source: str, path: tuple[str, ...]) -> str:
        """Return a fresh entry place reached from ``source`` by a silent tau.

        Each loop gets its own entry/exit pair so the translation stays
        strictly block-structured — nested loops don't share an entry
        place, and the workflow-net convention (initial place has no
        incoming arcs) holds automatically. Any redundant tau is collapsed
        by :meth:`PetriNet.reduce_silent_transitions`.
        """
        entry = self.add_place((*path, "loop_entry"))
        enter = self._add_silent_transition((*path, "enter"))
        self._connect(source, enter)
        self._connect(enter, entry)
        return entry

    def add_place(
        self,
        path: tuple[str, ...],
        *,
        initial: bool = False,
        final: bool = False,
    ) -> str:
        name = "/".join(path)
        if not self._net.has_place(name):
            self._net.add_place(name, initial=initial, final=final)
        return name

    def _add_visible_transition(self, label: str) -> str:
        if not self._net.has_transition(label):
            self._net.add_transition(label)
        return label

    def _add_silent_transition(self, path: tuple[str, ...]) -> str:
        name = "tau/" + "/".join(path)
        if not self._net.has_transition(name):
            self._net.add_silent_transition(name)
        return name

    def _connect(self, source: str, target: str) -> None:
        self._net.add_arc(source, target, if_exists="ignore")
