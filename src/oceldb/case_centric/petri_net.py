"""Petri-net synthesis from case-centric process trees."""

from collections.abc import Iterable

from oceldb.case_centric.process_tree import ProcessTree
from oceldb.models import PetriNet


def synthesize_petri_net(
    tree: ProcessTree,
    *,
    object_type: str = "case",
    variable_activities: Iterable[str] = (),
    net: PetriNet | None = None,
    simplify: bool = True,
) -> PetriNet:
    """Synthesize a Petri net from a case-centric process tree.

    When ``net`` is provided the subnet is added in place, merging visible
    transitions by label across previously added object types. When
    ``simplify`` is true (the default), redundant silent transitions are
    eliminated via :meth:`PetriNet.reduce_silent_transitions` right after
    the subnet is built, so each object type's subnet is minimized in
    isolation before more subnets are added.
    """
    target = net if net is not None else PetriNet()
    target.declare_object_type(object_type)

    builder = _Builder(
        target,
        object_type=object_type,
        variable_activities=frozenset(variable_activities),
    )
    source = builder.add_place(("source",), initial=True)
    sink = builder.add_place(("sink",), final=True)
    builder.translate(tree, source, sink, ("root",))

    if simplify:
        target.reduce_silent_transitions()
    return target


class _Builder:
    """Translates a :class:`ProcessTree` into a Petri-net subnet."""

    def __init__(
        self,
        net: PetriNet,
        *,
        object_type: str,
        variable_activities: frozenset[str],
    ) -> None:
        self._net = net
        self._object_type = object_type
        self._variable = variable_activities

    def translate(
        self,
        tree: ProcessTree,
        source: str,
        target: str,
        path: tuple[str, ...],
    ) -> None:
        """Translate ``tree`` into arcs between the ``source`` and ``target`` places."""
        handler = getattr(self, f"_handle_{tree.operator}")
        handler(tree, source, target, path)

    def _handle_activity(
        self, tree: ProcessTree, source: str, target: str, path: tuple[str, ...]
    ) -> None:
        if tree.label is None:
            raise ValueError("Activity process-tree nodes require a label.")
        transition = self._add_visible_transition(tree.label)
        variable = tree.label in self._variable
        self._connect(source, transition, variable=variable)
        self._connect(transition, target, variable=variable)

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
        """Return a place safe to receive redo arcs.

        The initial place must not have incoming arcs (workflow-net
        convention), so when a loop sits directly at the source we insert a
        fresh entry place reached by a silent transition.
        """
        if not self._net.place(source).initial:
            return source

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
        """Add a uniquely-named place identified by its tree path."""
        name = self._place_name(path)
        if not self._net.has_place(name):
            self._net.add_place(
                name,
                object_type=self._object_type,
                initial=initial,
                final=final,
            )
        return name

    def _add_visible_transition(self, label: str) -> str:
        if not self._net.has_transition(label):
            self._net.add_transition(label)
        return label

    def _add_silent_transition(self, path: tuple[str, ...]) -> str:
        name = self._silent_transition_name(path)
        if not self._net.has_transition(name):
            self._net.add_silent_transition(name)
        return name

    def _connect(self, source: str, target: str, *, variable: bool = False) -> None:
        self._net.add_arc(
            source,
            target,
            object_type=self._object_type,
            variable=variable,
            if_exists="ignore",
        )

    def _place_name(self, path: tuple[str, ...]) -> str:
        return f"{self._object_type}__" + "/".join(path)

    def _silent_transition_name(self, path: tuple[str, ...]) -> str:
        return f"tau__{self._object_type}__" + "/".join(path)
