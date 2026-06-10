"""Petri net visualization with Graphviz.

Returns a :class:`graphviz.Digraph`. Use its native methods to render or
save: ``gviz.render("net.svg")``, ``gviz.view()``, ``gviz.pipe(format="png")``.
In Jupyter the graph renders inline through Graphviz's ``_repr_*_`` hooks.
"""

from graphviz import Digraph

from oceldb.models import DEFAULT_OBJECT_TYPE, PetriNet, Place


class _NotebookDigraph(Digraph):
    """Digraph that fits the SVG to its notebook cell."""

    def _repr_mimebundle_(  # type: ignore[override]
        self, include: object = None, exclude: object = None
    ) -> tuple[dict[str, bytes], dict[str, dict[str, str]]]:
        return {"image/png": self.pipe(format="png")}, {}


_COLOR_PALETTE = (
    "#6366f1",  # indigo
    "#f59e0b",  # amber
    "#10b981",  # emerald
    "#ef4444",  # red
    "#06b6d4",  # cyan
    "#8b5cf6",  # violet
    "#84cc16",  # lime
    "#ec4899",  # pink
    "#fb923c",  # orange
    "#64748b",  # slate
)
_INK = "#1f2937"
_SILENT = "#111827"


def visualize_petri_net(
    net: PetriNet,
    *,
    name: str = "PetriNet",
    rankdir: str = "LR",
    bgcolor: str = "white",
) -> Digraph:
    """Return a :class:`graphviz.Digraph` rendering of ``net``."""
    colors = _object_type_colors(net)

    dot = _NotebookDigraph(
        name=name,
        engine="dot",
        graph_attr={
            "rankdir": rankdir,
            "bgcolor": bgcolor,
            "nodesep": "0.55",
            "ranksep": "0.7",
            "fontname": "Helvetica",
            "forcelabels": "true",
        },
        node_attr={"fontname": "Helvetica", "fontsize": "11"},
        edge_attr={"fontname": "Helvetica", "fontsize": "9", "arrowsize": "0.8"},
    )

    for place in net.places:
        color = colors[place.object_type]
        if place.initial or place.final:
            _add_boundary_place(dot, place, color)
        else:
            dot.node(place.name, **_place_attrs(place, color))  # pyright: ignore[reportUnknownMemberType]

    for transition in net.transitions:
        if transition.silent:
            dot.node(  # pyright: ignore[reportUnknownMemberType]
                transition.name,
                label="",
                shape="box",
                style="filled",
                width="0.12",
                height="0.5",
                fixedsize="true",
                color=_SILENT,
                fillcolor=_SILENT,
            )
        else:
            label = transition.label or transition.name
            dot.node(  # pyright: ignore[reportUnknownMemberType]
                transition.name,
                label=label,
                shape="box",
                style="filled,rounded",
                fillcolor="white",
                color=_INK,
                fontcolor=_INK,
                margin="0.14,0.06",
                penwidth="1.2",
            )

    for arc in net.arcs:
        color = colors[arc.object_type]
        if arc.variable:
            dot.edge(  # pyright: ignore[reportUnknownMemberType]
                arc.source,
                arc.target,
                color=f"{color}:invis:{color}",
                penwidth="1.0",
            )
        else:
            dot.edge(  # pyright: ignore[reportUnknownMemberType]
                arc.source,
                arc.target,
                color=color,
                penwidth="1.4",
            )

    return dot


def _add_boundary_place(dot: Digraph, place: Place, color: str) -> None:
    """Render an initial/final place as a circle with the type name beneath.

    Each boundary place is wrapped in its own invisible cluster subgraph;
    the cluster's label sits at the bottom and provides a reliably-placed
    caption — something a node ``xlabel`` cannot guarantee in LR layouts.
    """
    icon = "▶" if place.initial else "■"
    with dot.subgraph(name=f"cluster_{place.name}") as group:  # pyright: ignore[reportUnknownMemberType, reportOptionalContextManager]
        group.attr(  # pyright: ignore[reportUnknownMemberType]
            label=place.object_type,
            labelloc="b",
            labeljust="c",
            fontsize="10",
            fontcolor=color,
            pencolor="transparent",
            margin="2",
        )
        group.node(  # pyright: ignore[reportUnknownMemberType]
            place.name,
            label=icon,
            shape="circle",
            style="filled",
            fixedsize="true",
            width="0.45",
            height="0.45",
            color=color,
            fillcolor=color,
            fontcolor="white",
            fontsize="16",
            tooltip=place.display_label,
        )


def _object_type_colors(net: PetriNet) -> dict[str, str]:
    if not net.is_object_centric:
        return {DEFAULT_OBJECT_TYPE: _INK}
    return {
        object_type: _COLOR_PALETTE[index % len(_COLOR_PALETTE)]
        for index, object_type in enumerate(net.object_types)
    }


def _place_attrs(place: Place, color: str) -> dict[str, str]:
    tooltip = place.display_label
    return {
        "label": "",
        "shape": "circle",
        "style": "filled",
        "fixedsize": "true",
        "width": "0.35",
        "height": "0.35",
        "color": color,
        "fillcolor": color,
        "tooltip": tooltip,
    }
