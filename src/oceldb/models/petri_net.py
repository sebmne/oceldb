"""Petri net model.

The Petri net is a first-class, user-constructible structure. Build a net
incrementally via :meth:`PetriNet.add_place`, :meth:`add_transition`, and
:meth:`add_arc`, then call :meth:`PetriNet.validate` to confirm
well-formedness before using it for conformance checking or visualization.

The net is *object-centric*: places and arcs carry an ``object_type``. For
classical (case-centric) Petri nets, simply omit ``object_type`` and a
default type (``"object"``) is used internally.

Example (case-centric)
----------------------

>>> net = PetriNet()
>>> net.add_place("start", initial=True)
>>> net.add_place("end", final=True)
>>> net.add_transition("place order")
>>> net.add_arc("start", "place order")
>>> net.add_arc("place order", "end")
>>> net.validate()

Example (object-centric)
------------------------

>>> net = PetriNet()
>>> net.add_place("o_start", object_type="order", initial=True)
>>> net.add_place("i_start", object_type="item", initial=True)
>>> net.add_transition("place order")
>>> net.add_arc("o_start", "place order", object_type="order")
>>> net.add_arc("i_start", "place order", object_type="item", variable=True)
"""

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal


DEFAULT_OBJECT_TYPE = "object"


@dataclass(frozen=True)
class Place:
    """A place in a Petri net."""

    name: str
    object_type: str = DEFAULT_OBJECT_TYPE
    initial: bool = False
    final: bool = False
    label: str | None = None

    @property
    def display_label(self) -> str:
        """Return the user-facing label (falls back to the place name)."""
        return self.label if self.label is not None else self.name


@dataclass(frozen=True)
class Transition:
    """A transition in a Petri net.

    A transition is *silent* when its label is ``None``.
    """

    name: str
    label: str | None = None

    @property
    def silent(self) -> bool:
        """Return whether this transition is silent (has no visible label)."""
        return self.label is None


@dataclass(frozen=True)
class Arc:
    """A typed arc between a place and a transition."""

    source: str
    target: str
    object_type: str = DEFAULT_OBJECT_TYPE
    variable: bool = False


_MISSING: object = object()

NodeKind = Literal["place", "transition"]
IfExists = Literal["error", "ignore", "replace"]


class PetriNet:
    """A Petri net.

    Construct an empty net and add places, transitions, and arcs. Names are
    used as identifiers and must be unique across places and transitions.

    Object types are tracked automatically. When no ``object_type`` is given
    to :meth:`add_place` or :meth:`add_arc`, the default type
    (:data:`DEFAULT_OBJECT_TYPE`) is used — this is the natural choice for
    case-centric (classical) Petri nets.
    """

    def __init__(self, object_types: Iterable[str] = ()) -> None:
        self._places: dict[str, Place] = {}
        self._transitions: dict[str, Transition] = {}
        self._arcs: dict[tuple[str, str, str], Arc] = {}
        self._object_types: list[str] = []
        for object_type in object_types:
            self.declare_object_type(object_type)

    @property
    def places(self) -> tuple[Place, ...]:
        """All places, in insertion order."""
        return tuple(self._places.values())

    @property
    def transitions(self) -> tuple[Transition, ...]:
        """All transitions, in insertion order."""
        return tuple(self._transitions.values())

    @property
    def arcs(self) -> tuple[Arc, ...]:
        """All arcs, in insertion order."""
        return tuple(self._arcs.values())

    @property
    def object_types(self) -> tuple[str, ...]:
        """All declared object types, in insertion order."""
        return tuple(self._object_types)

    @property
    def is_object_centric(self) -> bool:
        """Return whether the net uses more than the default object type."""
        return any(
            object_type != DEFAULT_OBJECT_TYPE
            for object_type in self._object_types
        )

    def declare_object_type(self, object_type: str) -> "PetriNet":
        """Register an object type without yet adding nodes for it."""
        if object_type not in self._object_types:
            self._object_types.append(object_type)
        return self

    def add_place(
        self,
        name: str,
        object_type: str = DEFAULT_OBJECT_TYPE,
        *,
        initial: bool = False,
        final: bool = False,
        label: str | None = None,
    ) -> Place:
        """Add a place. Raises ``ValueError`` if the name is already used."""
        self._reject_existing_name(name)
        self.declare_object_type(object_type)
        place = Place(
            name=name,
            object_type=object_type,
            initial=initial,
            final=final,
            label=label,
        )
        self._places[name] = place
        return place

    def add_transition(
        self,
        name: str,
        *,
        label: str | None | object = _MISSING,
    ) -> Transition:
        """Add a transition.

        If ``label`` is omitted, the transition is visible and labelled with
        ``name``. Pass ``label=None`` explicitly for a silent transition, or
        use :meth:`add_silent_transition`.
        """
        self._reject_existing_name(name)
        actual_label: str | None = name if label is _MISSING else label  # type: ignore[assignment]
        transition = Transition(name=name, label=actual_label)
        self._transitions[name] = transition
        return transition

    def add_silent_transition(self, name: str) -> Transition:
        """Add a silent (unlabelled) transition."""
        return self.add_transition(name, label=None)

    def add_arc(
        self,
        source: str,
        target: str,
        object_type: str = DEFAULT_OBJECT_TYPE,
        *,
        variable: bool = False,
        if_exists: IfExists = "error",
    ) -> Arc:
        """Add an arc from a place to a transition (or vice versa).

        Both endpoints must already exist. The arc's ``object_type`` must
        match the connected place's ``object_type``. Use ``if_exists`` to
        control behavior when an arc with the same (source, target,
        object_type) already exists.
        """
        source_kind = self._node_kind(source)
        target_kind = self._node_kind(target)
        if source_kind is None:
            raise ValueError(
                f"Arc source {source!r} is not a known place or transition."
            )
        if target_kind is None:
            raise ValueError(
                f"Arc target {target!r} is not a known place or transition."
            )
        if source_kind == target_kind:
            raise ValueError(
                f"Arcs must connect a place to a transition "
                f"(got {source_kind} → {target_kind})."
            )

        place_name = source if source_kind == "place" else target
        place = self._places[place_name]
        if place.object_type != object_type:
            raise ValueError(
                f"Arc object type {object_type!r} does not match place "
                f"{place_name!r} (object type {place.object_type!r})."
            )

        self.declare_object_type(object_type)
        key = (source, target, object_type)
        arc = Arc(
            source=source,
            target=target,
            object_type=object_type,
            variable=variable,
        )

        if key in self._arcs:
            if if_exists == "error":
                raise ValueError(
                    f"Arc {source!r} → {target!r} for object type "
                    f"{object_type!r} already exists."
                )
            if if_exists == "ignore":
                return self._arcs[key]

        self._arcs[key] = arc
        return arc

    def remove_arc(
        self,
        source: str,
        target: str,
        object_type: str = DEFAULT_OBJECT_TYPE,
    ) -> None:
        """Remove an arc. Raises ``KeyError`` if it doesn't exist."""
        key = (source, target, object_type)
        if key not in self._arcs:
            raise KeyError(
                f"No arc {source!r} → {target!r} for object type {object_type!r}."
            )
        del self._arcs[key]

    def remove_place(self, name: str) -> None:
        """Remove a place along with every arc that touches it."""
        if name not in self._places:
            raise KeyError(f"No place named {name!r}.")
        for key in [k for k in self._arcs if name in (k[0], k[1])]:
            del self._arcs[key]
        del self._places[name]

    def remove_transition(self, name: str) -> None:
        """Remove a transition along with every arc that touches it."""
        if name not in self._transitions:
            raise KeyError(f"No transition named {name!r}.")
        for key in [k for k in self._arcs if name in (k[0], k[1])]:
            del self._arcs[key]
        del self._transitions[name]

    def has_place(self, name: str) -> bool:
        """Return whether a place with the given name exists."""
        return name in self._places

    def has_transition(self, name: str) -> bool:
        """Return whether a transition with the given name exists."""
        return name in self._transitions

    def has_arc(
        self,
        source: str,
        target: str,
        object_type: str = DEFAULT_OBJECT_TYPE,
    ) -> bool:
        """Return whether an arc with the given key exists."""
        return (source, target, object_type) in self._arcs

    def place(self, name: str) -> Place:
        """Return a place by name."""
        try:
            return self._places[name]
        except KeyError:
            raise KeyError(f"No place named {name!r}.") from None

    def transition(self, name: str) -> Transition:
        """Return a transition by name."""
        try:
            return self._transitions[name]
        except KeyError:
            raise KeyError(f"No transition named {name!r}.") from None

    def transition_by_label(self, label: str) -> Transition:
        """Return the visible transition with the given label."""
        for transition in self._transitions.values():
            if transition.label == label:
                return transition
        raise KeyError(f"No transition with label {label!r}.")

    def preset(self, name: str) -> tuple[Arc, ...]:
        """Return arcs flowing into the given node."""
        return tuple(arc for arc in self._arcs.values() if arc.target == name)

    def postset(self, name: str) -> tuple[Arc, ...]:
        """Return arcs flowing out of the given node."""
        return tuple(arc for arc in self._arcs.values() if arc.source == name)

    def input_arcs(self, transition_name: str) -> tuple[Arc, ...]:
        """Return the input arcs of a transition (alias for :meth:`preset`)."""
        return self.preset(transition_name)

    def output_arcs(self, transition_name: str) -> tuple[Arc, ...]:
        """Return the output arcs of a transition (alias for :meth:`postset`)."""
        return self.postset(transition_name)

    def initial_places(
        self, object_type: str | None = None
    ) -> tuple[Place, ...]:
        """Return places marked initial, optionally filtered by object type."""
        return tuple(
            place
            for place in self._places.values()
            if place.initial
            and (object_type is None or place.object_type == object_type)
        )

    def final_places(
        self, object_type: str | None = None
    ) -> tuple[Place, ...]:
        """Return places marked final, optionally filtered by object type."""
        return tuple(
            place
            for place in self._places.values()
            if place.final
            and (object_type is None or place.object_type == object_type)
        )

    def reduce_silent_transitions(self) -> "PetriNet":
        """Eliminate redundant silent transitions in place (returns self).

        Iteratively fuses any silent transition ``τ`` with exactly one input
        arc and one output arc — both of the same object type and not
        variable — whenever one of the bordering places has no other use
        (analogous to ε-NFA → NFA elimination). The redundant place is
        removed and any arcs that touched it are rewired to the surviving
        place; variable flags are merged conservatively (variable wins).
        """
        while self._reduce_one_silent_transition():
            pass
        return self

    def _reduce_one_silent_transition(self) -> bool:
        for transition in list(self._transitions.values()):
            if not transition.silent:
                continue
            inputs = self.input_arcs(transition.name)
            outputs = self.output_arcs(transition.name)
            if len(inputs) != 1 or len(outputs) != 1:
                continue
            in_arc, out_arc = inputs[0], outputs[0]
            if in_arc.object_type != out_arc.object_type:
                continue
            if in_arc.variable or out_arc.variable:
                continue
            p_in = self._places[in_arc.source]
            p_out = self._places[out_arc.target]
            if p_in.name == p_out.name:
                continue

            # Case A: τ is the only producer of p_out → fuse p_out into p_in.
            if (
                not p_out.initial
                and not p_out.final
                and len(self.preset(p_out.name)) == 1
            ):
                self._fuse_silent(transition, keep=p_in, drop=p_out)
                return True

            # Case B: τ is the only consumer of p_in → fuse p_in into p_out.
            if (
                not p_in.initial
                and not p_in.final
                and len(self.postset(p_in.name)) == 1
            ):
                self._fuse_silent(transition, keep=p_out, drop=p_in)
                return True
        return False

    def _fuse_silent(
        self, transition: Transition, *, keep: Place, drop: Place
    ) -> None:
        self.remove_transition(transition.name)
        for arc in [
            arc for arc in self._arcs.values()
            if drop.name in (arc.source, arc.target)
        ]:
            del self._arcs[(arc.source, arc.target, arc.object_type)]
            new_source = keep.name if arc.source == drop.name else arc.source
            new_target = keep.name if arc.target == drop.name else arc.target
            new_key = (new_source, new_target, arc.object_type)
            existing = self._arcs.get(new_key)
            variable = arc.variable or (existing.variable if existing else False)
            self._arcs[new_key] = Arc(
                source=new_source,
                target=new_target,
                object_type=arc.object_type,
                variable=variable,
            )
        self.remove_place(drop.name)

    def validate(self) -> None:
        """Validate the net's well-formedness.

        Raises :class:`ValueError` with all problems found. Checks:

        - every used object type has at least one initial and one final
          place;
        - every transition has at least one input and one output arc;
        - declared object types are actually used by a place or arc.
        """
        errors: list[str] = []

        for object_type in self._object_types:
            if not self.initial_places(object_type):
                errors.append(
                    f"Object type {object_type!r} has no initial place."
                )
            if not self.final_places(object_type):
                errors.append(
                    f"Object type {object_type!r} has no final place."
                )

        for transition in self._transitions.values():
            if not self.preset(transition.name):
                errors.append(
                    f"Transition {transition.name!r} has no input arc."
                )
            if not self.postset(transition.name):
                errors.append(
                    f"Transition {transition.name!r} has no output arc."
                )

        used = {place.object_type for place in self._places.values()}
        used.update(arc.object_type for arc in self._arcs.values())
        unused = [
            object_type
            for object_type in self._object_types
            if object_type not in used
        ]
        if unused:
            errors.append(
                f"Declared object types are unused: {sorted(unused)}."
            )

        if errors:
            joined = "\n  - ".join(errors)
            raise ValueError(f"Invalid Petri net:\n  - {joined}")

    def __repr__(self) -> str:
        return (
            "PetriNet("
            f"places={len(self._places)}, "
            f"transitions={len(self._transitions)}, "
            f"arcs={len(self._arcs)}, "
            f"object_types={list(self._object_types)})"
        )

    def _node_kind(self, name: str) -> NodeKind | None:
        if name in self._places:
            return "place"
        if name in self._transitions:
            return "transition"
        return None

    def _reject_existing_name(self, name: str) -> None:
        if name in self._places:
            raise ValueError(f"A place named {name!r} already exists.")
        if name in self._transitions:
            raise ValueError(f"A transition named {name!r} already exists.")
