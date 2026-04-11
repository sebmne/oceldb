import { useState, useMemo, useCallback } from "react";
import { useTypes, useDfg, useVariants } from "../api/hooks";
import ErrorBanner from "../components/common/ErrorBanner";
import ProcessGraph from "../components/process-map/ProcessGraph";
import VariantPanel from "../components/process-map/VariantPanel";

const ALL_TYPES = "__all__";

export default function ProcessMapPage() {
  const { data: types } = useTypes();
  const objectTypes = types?.object_types ?? [];

  const [objectType, setObjectType] = useState(ALL_TYPES);
  const [minFrequency, setMinFrequency] = useState(1);
  const [selectedVariants, setSelectedVariants] = useState<Set<number>>(new Set());

  const activeType = objectType;

  const { data: dfg, isLoading: dfgLoading, error: dfgError } = useDfg(activeType);
  const { data: variantData } = useVariants(activeType);

  const variants = variantData?.variants ?? [];

  // Filter edges by min frequency and selected variants
  const filteredDfg = useMemo(() => {
    if (!dfg) return null;

    let allowedEdges = new Set<string>();
    let filterByVariant = selectedVariants.size > 0;

    if (filterByVariant) {
      for (const v of variants) {
        if (!selectedVariants.has(v.id)) continue;
        for (let i = 0; i < v.activities.length - 1; i++) {
          allowedEdges.add(`${v.activities[i]}→${v.activities[i + 1]}`);
        }
      }
    }

    const edges = dfg.edges.filter((e) => {
      if (e.frequency < minFrequency) return false;
      if (filterByVariant && !allowedEdges.has(`${e.source}→${e.target}`)) return false;
      return true;
    });

    const connectedNodes = new Set<string>();
    for (const e of edges) {
      connectedNodes.add(e.source);
      connectedNodes.add(e.target);
    }

    const nodes = dfg.nodes.filter((n) => connectedNodes.has(n.id));

    return { nodes, edges };
  }, [dfg, minFrequency, selectedVariants, variants]);

  const maxEdgeFreq = useMemo(
    () => Math.max(1, ...(dfg?.edges.map((e) => e.frequency) ?? [1])),
    [dfg],
  );

  const toggleVariant = useCallback((id: number) => {
    setSelectedVariants((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  const clearVariants = useCallback(() => {
    setSelectedVariants(new Set());
  }, []);

  return (
    <div className="flex h-full gap-4">
      {/* Main graph area */}
      <div className="flex flex-1 flex-col gap-3">
        {/* Controls bar */}
        <div className="flex items-center gap-4 rounded-xl border border-slate-200 bg-white px-4 py-2.5 shadow-sm">
          <label className="flex items-center gap-2 text-xs font-medium text-slate-600">
            <span className="text-slate-400">Scope</span>
            <select
              value={activeType}
              onChange={(e) => {
                setObjectType(e.target.value);
                setSelectedVariants(new Set());
                setMinFrequency(1);
              }}
              className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-1.5 text-xs font-semibold text-slate-700 transition-colors hover:border-slate-300 focus:border-cyan-400 focus:outline-none focus:ring-2 focus:ring-cyan-400/20"
            >
              <option value={ALL_TYPES}>All Object Types</option>
              {objectTypes.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
          </label>

          <div className="h-5 w-px bg-slate-200" />

          <label className="flex items-center gap-2 text-xs font-medium text-slate-600">
            <span className="text-slate-400">Min Frequency</span>
            <input
              type="range"
              min={1}
              max={maxEdgeFreq}
              value={minFrequency}
              onChange={(e) => setMinFrequency(Number(e.target.value))}
              className="h-1.5 w-36 cursor-pointer appearance-none rounded-full bg-slate-200 accent-cyan-600 [&::-webkit-slider-thumb]:h-3.5 [&::-webkit-slider-thumb]:w-3.5 [&::-webkit-slider-thumb]:appearance-none [&::-webkit-slider-thumb]:rounded-full [&::-webkit-slider-thumb]:bg-cyan-600 [&::-webkit-slider-thumb]:shadow-sm"
            />
            <span className="w-10 text-right font-mono text-xs font-semibold text-slate-500">
              {minFrequency}
            </span>
          </label>

          {selectedVariants.size > 0 && (
            <>
              <div className="h-5 w-px bg-slate-200" />
              <span className="rounded-full bg-cyan-50 px-2.5 py-0.5 text-xs font-semibold text-cyan-700">
                {selectedVariants.size} variant{selectedVariants.size > 1 ? "s" : ""} selected
              </span>
              <button
                onClick={clearVariants}
                className="text-xs text-slate-400 hover:text-slate-600 transition-colors"
              >
                Clear
              </button>
            </>
          )}
        </div>

        {/* Graph */}
        {dfgError && <ErrorBanner message={dfgError.message} />}

        {dfgLoading ? (
          <div className="flex flex-1 items-center justify-center">
            <div className="flex flex-col items-center gap-2">
              <div className="h-8 w-8 animate-spin rounded-full border-2 border-cyan-200 border-t-cyan-600" />
              <span className="text-sm text-slate-400">Computing process map...</span>
            </div>
          </div>
        ) : filteredDfg ? (
          <div className="flex-1 overflow-hidden rounded-xl border border-slate-200 bg-slate-50/50 shadow-sm">
            <ProcessGraph
              nodes={filteredDfg.nodes}
              edges={filteredDfg.edges}
              maxEdgeFreq={maxEdgeFreq}
            />
          </div>
        ) : (
          <div className="flex flex-1 items-center justify-center text-sm text-slate-400">
            Select a scope to view the process map
          </div>
        )}
      </div>

      {/* Variant sidebar */}
      <VariantPanel
        variants={variants}
        selectedVariants={selectedVariants}
        onToggle={toggleVariant}
        onClear={clearVariants}
      />
    </div>
  );
}
