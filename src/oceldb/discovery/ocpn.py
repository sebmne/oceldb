"""Object-centric Petri net discovery via the inductive miner."""

from typing import cast

from oceldb.case_centric import discover_petri_net
from oceldb.expr import col
from oceldb.models import PetriNet
from oceldb.ocel import OCEL


def discover_ocpn(
    ocel: OCEL,
    *object_types: str,
    threshold: float = 0.0,
    simplify: bool = True,
) -> PetriNet:
    """Discover an object-centric Petri net with the inductive miner.

    For each selected object type the OCEL is flattened into a
    case-centric event log, :func:`discover_petri_net` mines a workflow
    net for it, and the result is composed into a single OCPN — places and
    silent transitions get an ``{object_type}/`` prefix, visible
    transitions are shared by label across object types, and arcs are
    marked ``variable`` when the activity involves more than one object
    of the arc's type.

    ``threshold`` is a relative noise-filtering parameter in ``[0, 1]``
    applied per subnet:

    - An activity ``a`` is retained if
      ``count(a) ≥ threshold × max_activity_count``.
    - An edge ``(a, b)`` is retained if
      ``count(a, b) ≥ threshold × max_outgoing_count(a)``.

    The default ``0.0`` applies no filtering. Higher values remove
    infrequent behaviour; a typical starting point is ``0.2``.

    When ``simplify`` is true (the default), redundant silent transitions
    are eliminated per subnet via
    :meth:`PetriNet.reduce_silent_transitions` before composition.
    """
    if not (0.0 <= threshold <= 1.0):
        raise ValueError(f"threshold must be in [0, 1], got {threshold!r}.")

    selected = _select_object_types(ocel, object_types)
    variable = _variable_activities_per_type(ocel, selected)

    ocpn = PetriNet(object_types=selected)
    for object_type in selected:
        subnet = discover_petri_net(
            ocel.flatten(object_type),
            threshold=threshold,
            simplify=simplify,
        )
        _merge_subnet(
            ocpn,
            subnet,
            object_type=object_type,
            variable_activities=variable[object_type],
        )

    ocpn.validate()
    return ocpn


def _merge_subnet(
    ocpn: PetriNet,
    subnet: PetriNet,
    *,
    object_type: str,
    variable_activities: frozenset[str],
) -> None:
    """Add the case-centric ``subnet`` into ``ocpn`` under ``object_type``.

    Place and silent-transition names get an ``{object_type}/`` prefix to
    avoid collisions across object types. Visible transitions are shared
    by label — the first object type that mines an activity creates the
    transition, the rest just connect to it.
    """
    rename: dict[str, str] = {}

    for place in subnet.places:
        new_name = f"{object_type}/{place.name}"
        ocpn.add_place(
            new_name,
            object_type=object_type,
            initial=place.initial,
            final=place.final,
            label=place.label,
        )
        rename[place.name] = new_name

    for transition in subnet.transitions:
        if transition.silent:
            new_name = f"{object_type}/{transition.name}"
            if not ocpn.has_transition(new_name):
                ocpn.add_silent_transition(new_name)
            rename[transition.name] = new_name
        else:
            label = cast(str, transition.label)
            if not ocpn.has_transition(label):
                ocpn.add_transition(label)
            rename[transition.name] = label

    for arc in subnet.arcs:
        new_source = rename[arc.source]
        new_target = rename[arc.target]
        transition_name = (
            new_target if subnet.has_transition(arc.target) else new_source
        )
        transition = ocpn.transition(transition_name)
        is_variable = (
            transition.label is not None and transition.label in variable_activities
        )
        ocpn.add_arc(
            new_source,
            new_target,
            object_type=object_type,
            variable=is_variable,
            if_exists="ignore",
        )


def _select_object_types(ocel: OCEL, object_types: tuple[str, ...]) -> tuple[str, ...]:
    if not object_types:
        return tuple(sorted(ocel.manifest.object_types))

    unknown = sorted(set(object_types) - set(ocel.manifest.object_types))
    if unknown:
        names = ", ".join(repr(name) for name in unknown)
        raise ValueError(f"Unknown object type(s): {names}.")
    return object_types


def _variable_activities_per_type(
    ocel: OCEL, object_types: tuple[str, ...]
) -> dict[str, frozenset[str]]:
    """Return, per object type, activities where some event involves >1 object."""
    if not object_types:
        return {}

    result: dict[str, set[str]] = {object_type: set() for object_type in object_types}
    counts = (
        ocel.event_object.filter(col("ocel_object_type").isin(list(object_types)))
        .group_by("ocel_event_id", "ocel_event_type", "ocel_object_type")
        .aggregate(n=col("ocel_object_id").nunique())
        .execute()
    )

    for row in counts.iter_rows(named=True):
        if cast(int, row["n"]) > 1:
            object_type = cast(str, row["ocel_object_type"])
            activity = cast(str, row["ocel_event_type"])
            result[object_type].add(activity)

    return {
        object_type: frozenset(activities) for object_type, activities in result.items()
    }
