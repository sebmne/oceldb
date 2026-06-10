"""Object-centric Petri net discovery via the inductive miner."""

from typing import cast

from oceldb.case_centric import (
    discover_dfg,
    discover_process_tree,
    synthesize_petri_net,
)
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

    For each selected object type, the OCEL is flattened into a case-centric
    event log, a directly-follows graph is discovered, the inductive miner
    derives a process tree, and the resulting subnet is added to a shared
    Petri net (merging visible transitions by label). Arcs are marked
    *variable* when at least one event of the activity involves multiple
    objects of the arc's type.

    ``threshold`` is a relative noise-filtering parameter in ``[0, 1]`` applied
    when building the directly-follows graph for each object type:

    - An activity ``a`` is retained if
      ``count(a) ≥ threshold × max_activity_count``.
    - An edge ``(a, b)`` is retained if
      ``count(a, b) ≥ threshold × max_outgoing_count(a)``.

    The default ``0.0`` applies no filtering. Higher values remove infrequent
    behaviour; a typical starting point is ``0.2``.

    When ``simplify`` is true (the default), redundant silent transitions are
    eliminated per subnet via :meth:`PetriNet.reduce_silent_transitions`.
    """
    if not (0.0 <= threshold <= 1.0):
        raise ValueError(f"threshold must be in [0, 1], got {threshold!r}.")

    selected = _select_object_types(ocel, object_types)
    variable = _variable_activities_per_type(ocel, selected)

    net = PetriNet(object_types=selected)
    for object_type in selected:
        _add_subnet(
            net,
            ocel,
            object_type,
            threshold=threshold,
            variable_activities=variable[object_type],
            simplify=simplify,
        )

    net.validate()
    return net


def _add_subnet(
    net: PetriNet,
    ocel: OCEL,
    object_type: str,
    *,
    threshold: float,
    variable_activities: frozenset[str],
    simplify: bool,
) -> None:
    dfg = discover_dfg(ocel.flatten(object_type), threshold=threshold)
    tree = discover_process_tree(dfg)
    synthesize_petri_net(
        tree,
        object_type=object_type,
        variable_activities=variable_activities,
        net=net,
        simplify=simplify,
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
