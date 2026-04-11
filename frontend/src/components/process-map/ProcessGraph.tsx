import { useMemo, useCallback } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  type Node,
  type Edge,
  type NodeTypes,
  Position,
  MarkerType,
  ConnectionLineType,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import dagre from "@dagrejs/dagre";
import type { DfgNode, DfgEdge } from "../../api/types";
import ActivityNode from "./ActivityNode";
import GatewayNode from "./GatewayNode";

type Props = {
  nodes: DfgNode[];
  edges: DfgEdge[];
  maxEdgeFreq: number;
};

const nodeTypes: NodeTypes = {
  activity: ActivityNode,
  gateway: GatewayNode,
};

const EDGE_COLOR = "#0891b2"; // cyan-600
const EDGE_MUTED = "#94a3b8"; // slate-400

function layoutGraph(
  dfgNodes: DfgNode[],
  dfgEdges: DfgEdge[],
  maxEdgeFreq: number,
): { nodes: Node[]; edges: Edge[] } {
  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({ rankdir: "TB", nodesep: 70, ranksep: 90, marginx: 40, marginy: 40 });

  const hasStart = dfgNodes.some((n) => n.is_start);
  const hasEnd = dfgNodes.some((n) => n.is_end);

  if (hasStart) g.setNode("__start__", { width: 40, height: 40 });
  if (hasEnd) g.setNode("__end__", { width: 40, height: 40 });

  for (const n of dfgNodes) {
    g.setNode(n.id, { width: 200, height: 52 });
  }

  if (hasStart) {
    for (const n of dfgNodes) {
      if (n.is_start) g.setEdge("__start__", n.id);
    }
  }

  for (const e of dfgEdges) {
    g.setEdge(e.source, e.target);
  }

  if (hasEnd) {
    for (const n of dfgNodes) {
      if (n.is_end) g.setEdge(n.id, "__end__");
    }
  }

  dagre.layout(g);

  const nodes: Node[] = [];

  if (hasStart) {
    const pos = g.node("__start__");
    nodes.push({
      id: "__start__",
      type: "gateway",
      position: { x: pos.x - 20, y: pos.y - 20 },
      data: { label: "Start", variant: "start" },
      draggable: true,
    });
  }

  if (hasEnd) {
    const pos = g.node("__end__");
    nodes.push({
      id: "__end__",
      type: "gateway",
      position: { x: pos.x - 20, y: pos.y - 20 },
      data: { label: "End", variant: "end" },
      draggable: true,
    });
  }

  for (const n of dfgNodes) {
    const pos = g.node(n.id);
    nodes.push({
      id: n.id,
      type: "activity",
      position: { x: pos.x - 100, y: pos.y - 26 },
      data: { label: n.label, frequency: n.frequency },
      sourcePosition: Position.Bottom,
      targetPosition: Position.Top,
      draggable: true,
    });
  }

  const edges: Edge[] = [];

  // Start edges
  if (hasStart) {
    for (const n of dfgNodes) {
      if (n.is_start) {
        edges.push({
          id: `__start__-${n.id}`,
          source: "__start__",
          target: n.id,
          type: "smoothstep",
          style: { stroke: EDGE_MUTED, strokeWidth: 1.5 },
          markerEnd: { type: MarkerType.ArrowClosed, color: EDGE_MUTED, width: 14, height: 14 },
          label: String(n.start_count),
          labelStyle: { fontSize: 10, fill: EDGE_MUTED, fontWeight: 600 },
          labelBgStyle: { fill: "#f8fafc", fillOpacity: 0.95 },
          labelBgPadding: [6, 3] as [number, number],
          labelBgBorderRadius: 10,
        });
      }
    }
  }

  // DFG edges with frequency-based styling
  for (const e of dfgEdges) {
    const ratio = maxEdgeFreq > 1 ? e.frequency / maxEdgeFreq : 1;
    const width = 1.5 + ratio * 4.5;
    const opacity = 0.35 + ratio * 0.65;

    edges.push({
      id: `${e.source}-${e.target}`,
      source: e.source,
      target: e.target,
      type: "smoothstep",
      style: { stroke: EDGE_COLOR, strokeWidth: width, opacity },
      markerEnd: {
        type: MarkerType.ArrowClosed,
        color: EDGE_COLOR,
        width: 14,
        height: 14,
      },
      label: String(e.frequency),
      labelStyle: { fontSize: 10, fill: "#334155", fontWeight: 700 },
      labelBgStyle: { fill: "#f0fdfa", fillOpacity: 0.95 },
      labelBgPadding: [6, 3] as [number, number],
      labelBgBorderRadius: 10,
    });
  }

  // End edges
  if (hasEnd) {
    for (const n of dfgNodes) {
      if (n.is_end) {
        edges.push({
          id: `${n.id}-__end__`,
          source: n.id,
          target: "__end__",
          type: "smoothstep",
          style: { stroke: EDGE_MUTED, strokeWidth: 1.5 },
          markerEnd: { type: MarkerType.ArrowClosed, color: EDGE_MUTED, width: 14, height: 14 },
          label: String(n.end_count),
          labelStyle: { fontSize: 10, fill: EDGE_MUTED, fontWeight: 600 },
          labelBgStyle: { fill: "#f8fafc", fillOpacity: 0.95 },
          labelBgPadding: [6, 3] as [number, number],
          labelBgBorderRadius: 10,
        });
      }
    }
  }

  return { nodes, edges };
}

export default function ProcessGraph({ nodes: dfgNodes, edges: dfgEdges, maxEdgeFreq }: Props) {
  const { nodes, edges } = useMemo(
    () => layoutGraph(dfgNodes, dfgEdges, maxEdgeFreq),
    [dfgNodes, dfgEdges, maxEdgeFreq],
  );

  const onInit = useCallback((instance: { fitView: () => void }) => {
    setTimeout(() => instance.fitView(), 50);
  }, []);

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      nodeTypes={nodeTypes}
      onInit={onInit}
      fitView
      minZoom={0.1}
      maxZoom={2.5}
      connectionLineType={ConnectionLineType.SmoothStep}
      proOptions={{ hideAttribution: true }}
      defaultEdgeOptions={{ type: "smoothstep" }}
    >
      <Background color="#e2e8f0" gap={24} size={1} />
      <Controls showInteractive={false} className="!rounded-lg !border-slate-200 !shadow-sm" />
    </ReactFlow>
  );
}
