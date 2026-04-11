import { Handle, Position } from "@xyflow/react";

type Props = {
  data: { label: string; variant: "start" | "end"; count?: number };
};

export default function GatewayNode({ data }: Props) {
  const isStart = data.variant === "start";

  return (
    <div className="flex flex-col items-center gap-1">
      <div
        className={`flex h-10 w-10 items-center justify-center rounded-full shadow-sm ${
          isStart
            ? "bg-emerald-500 ring-4 ring-emerald-500/20"
            : "bg-slate-700 ring-4 ring-slate-700/20"
        }`}
      >
        {isStart ? (
          <>
            {/* Play icon */}
            <svg width="14" height="14" viewBox="0 0 14 14" fill="none" className="ml-0.5">
              <path d="M3 1.5v11l9-5.5L3 1.5z" fill="white" />
            </svg>
            <Handle type="source" position={Position.Bottom} className="!bg-transparent !w-3 !h-3 !border-0" />
          </>
        ) : (
          <>
            {/* Stop icon */}
            <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
              <rect x="1" y="1" width="10" height="10" rx="1.5" fill="white" />
            </svg>
            <Handle type="target" position={Position.Top} className="!bg-transparent !w-3 !h-3 !border-0" />
          </>
        )}
      </div>
    </div>
  );
}
