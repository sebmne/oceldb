import { useState } from "react";
import { useBrowse, useSchema } from "../api/hooks";
import type { TableSource } from "../api/types";
import DataGrid from "../components/common/DataGrid";
import Pagination from "../components/common/Pagination";
import ErrorBanner from "../components/common/ErrorBanner";

const SOURCES: TableSource[] = ["event", "object", "event_object", "object_object"];
const PAGE_SIZE = 100;

export default function BrowserPage() {
  const [source, setSource] = useState<TableSource>("event");
  const [offset, setOffset] = useState(0);

  const { data, isLoading, error } = useBrowse(source, PAGE_SIZE, offset);
  const { data: schema } = useSchema(source);

  function handleSourceChange(s: TableSource) {
    setSource(s);
    setOffset(0);
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        {SOURCES.map((s) => (
          <button
            key={s}
            onClick={() => handleSourceChange(s)}
            className={`rounded-lg px-3 py-1.5 text-sm font-medium transition-colors ${
              source === s
                ? "bg-slate-900 text-white"
                : "border border-slate-200 bg-white text-slate-600 hover:bg-slate-50"
            }`}
          >
            {s}
          </button>
        ))}
      </div>

      {schema && (
        <div className="flex flex-wrap gap-2">
          {schema.columns.map((col) => (
            <span
              key={col.name}
              className="inline-flex items-center gap-1.5 rounded-md bg-slate-100 px-2 py-1 text-xs"
            >
              <span className="font-medium text-slate-700">{col.name}</span>
              <span className="text-slate-400">{col.type}</span>
            </span>
          ))}
        </div>
      )}

      {error && <ErrorBanner message={error.message} />}

      {isLoading ? (
        <div className="text-sm text-slate-400">Loading...</div>
      ) : data ? (
        <>
          <DataGrid columns={data.columns} rows={data.rows} maxHeight="calc(100vh - 280px)" />
          <Pagination
            offset={offset}
            limit={PAGE_SIZE}
            total={data.total_count}
            onOffsetChange={setOffset}
          />
        </>
      ) : null}
    </div>
  );
}
