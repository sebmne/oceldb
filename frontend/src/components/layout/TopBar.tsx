import { useMetadata, useOverview } from "../../api/hooks";

export default function TopBar() {
  const { data: meta } = useMetadata();
  const { data: overview } = useOverview();

  return (
    <header className="flex h-12 shrink-0 items-center justify-between border-b border-slate-200 bg-white px-6">
      <div className="flex items-center gap-4">
        {meta && (
          <span className="text-sm text-slate-500">
            <span className="font-medium text-slate-700">{meta.source}</span>
          </span>
        )}
      </div>
      <div className="flex items-center gap-4 text-xs text-slate-400">
        {overview && (
          <>
            <span>
              {overview.overview.event_count.toLocaleString()} events
            </span>
            <span className="h-3 w-px bg-slate-200" />
            <span>
              {overview.overview.object_count.toLocaleString()} objects
            </span>
          </>
        )}
      </div>
    </header>
  );
}
