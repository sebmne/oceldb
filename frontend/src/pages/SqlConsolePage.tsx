import { useState, useCallback } from "react";
import Editor from "@monaco-editor/react";
import { useSqlExecute, useSchema, useTypes } from "../api/hooks";
import type { TableSource } from "../api/types";
import DataGrid from "../components/common/DataGrid";
import ErrorBanner from "../components/common/ErrorBanner";

const TABLES: TableSource[] = ["event", "object", "event_object", "object_object"];

export default function SqlConsolePage() {
  const [query, setQuery] = useState("SELECT * FROM event LIMIT 100");
  const [limit, setLimit] = useState(1000);
  const sql = useSqlExecute();
  const { data: types } = useTypes();

  const run = useCallback(() => {
    if (query.trim()) {
      sql.mutate({ query: query.trim(), limit });
    }
  }, [query, limit, sql]);

  const handleEditorMount = useCallback(
    (editor: { addCommand: (keybinding: number, handler: () => void) => void }, monaco: { KeyMod: { CtrlCmd: number }; KeyCode: { Enter: number } }) => {
      editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => {
        run();
      });
    },
    [run],
  );

  return (
    <div className="flex h-full gap-4">
      {/* Schema Explorer */}
      <aside className="w-52 shrink-0 overflow-auto rounded-xl border border-slate-200 bg-white p-3">
        <h3 className="mb-2 text-xs font-semibold text-slate-500 uppercase tracking-wide">
          Schema
        </h3>
        {TABLES.map((t) => (
          <SchemaTree key={t} table={t} />
        ))}
        {types && (
          <>
            <h3 className="mb-1 mt-4 text-xs font-semibold text-slate-500 uppercase tracking-wide">
              Types
            </h3>
            <div className="space-y-0.5">
              {types.event_types.map((t) => (
                <div key={t} className="text-xs text-slate-600">
                  <span className="text-slate-400">event:</span> {t}
                </div>
              ))}
              {types.object_types.map((t) => (
                <div key={t} className="text-xs text-slate-600">
                  <span className="text-slate-400">object:</span> {t}
                </div>
              ))}
            </div>
          </>
        )}
      </aside>

      {/* Editor + Results */}
      <div className="flex flex-1 flex-col gap-4">
        <div className="overflow-hidden rounded-xl border border-slate-200">
          <Editor
            height="200px"
            defaultLanguage="sql"
            value={query}
            onChange={(v) => setQuery(v ?? "")}
            onMount={handleEditorMount}
            theme="vs"
            options={{
              minimap: { enabled: false },
              fontSize: 13,
              lineNumbers: "on",
              scrollBeyondLastLine: false,
              wordWrap: "on",
              padding: { top: 8 },
            }}
          />
        </div>

        <div className="flex items-center gap-3">
          <button
            onClick={run}
            disabled={sql.isPending}
            className="rounded-lg bg-slate-900 px-4 py-2 text-sm font-medium text-white hover:bg-slate-800 disabled:opacity-50"
          >
            {sql.isPending ? "Running..." : "Run"}
          </button>
          <span className="text-xs text-slate-400">Ctrl+Enter</span>
          <label className="ml-auto flex items-center gap-2 text-xs text-slate-500">
            Limit
            <input
              type="number"
              value={limit}
              onChange={(e) => setLimit(Number(e.target.value) || 1000)}
              className="w-20 rounded border border-slate-200 px-2 py-1 text-xs"
            />
          </label>
        </div>

        {sql.error && <ErrorBanner message={sql.error.message} />}

        {sql.data && (
          <>
            <div className="flex items-center gap-3 text-xs text-slate-500">
              <span>
                {sql.data.row_count} rows in {sql.data.execution_time_ms.toFixed(1)}ms
              </span>
            </div>
            <DataGrid
              columns={sql.data.columns}
              rows={sql.data.rows}
              maxHeight="calc(100vh - 420px)"
            />
          </>
        )}
      </div>
    </div>
  );
}

function SchemaTree({ table }: { table: TableSource }) {
  const { data } = useSchema(table);
  const [open, setOpen] = useState(false);

  return (
    <div className="mb-1">
      <button
        onClick={() => setOpen(!open)}
        className="flex w-full items-center gap-1 rounded px-1 py-0.5 text-xs font-medium text-slate-700 hover:bg-slate-50"
      >
        <span className="text-[10px] text-slate-400">{open ? "\u25BC" : "\u25B6"}</span>
        {table}
      </button>
      {open && data && (
        <div className="ml-4 space-y-0.5">
          {data.columns.map((col) => (
            <div key={col.name} className="flex items-center gap-1.5 text-xs">
              <span className="text-slate-600">{col.name}</span>
              <span className="text-[10px] text-slate-400">{col.type}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
