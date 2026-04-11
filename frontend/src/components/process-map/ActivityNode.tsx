import { Handle, Position } from "@xyflow/react";

type Props = {
  data: { label: string; frequency: number };
};

export default function ActivityNode({ data }: Props) {
  return (
    <div className="group relative flex min-w-[180px] items-center rounded-lg bg-white shadow-[0_1px_4px_rgba(0,0,0,0.08)] ring-1 ring-slate-200/80 transition-all hover:shadow-[0_2px_8px_rgba(0,0,0,0.12)] hover:ring-slate-300">
      <Handle type="target" position={Position.Top} className="!bg-transparent !w-3 !h-3 !border-0" />

      {/* Left accent bar */}
      <div className="absolute left-0 top-2 bottom-2 w-[3px] rounded-full bg-cyan-500" />

      <div className="flex w-full items-center justify-between px-4 py-3">
        <span className="text-[13px] font-semibold text-slate-800 leading-tight">
          {data.label}
        </span>
        <span className="ml-3 shrink-0 rounded-full bg-cyan-50 px-2.5 py-0.5 text-[11px] font-bold tabular-nums text-cyan-700">
          {data.frequency.toLocaleString()}
        </span>
      </div>

      <Handle type="source" position={Position.Bottom} className="!bg-transparent !w-3 !h-3 !border-0" />
    </div>
  );
}
