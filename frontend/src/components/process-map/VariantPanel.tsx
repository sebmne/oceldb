import type { VariantEntry } from "../../api/types";

type Props = {
  variants: VariantEntry[];
  selectedVariants: Set<number>;
  onToggle: (id: number) => void;
  onClear: () => void;
};

export default function VariantPanel({
  variants,
  selectedVariants,
  onToggle,
  onClear,
}: Props) {
  if (variants.length === 0) {
    return (
      <aside className="w-80 shrink-0 rounded-xl border border-slate-200 bg-white p-4">
        <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wide">
          Variants
        </h3>
        <p className="mt-3 text-xs text-slate-400">No variants computed yet</p>
      </aside>
    );
  }

  const maxFreq = Math.max(...variants.map((v) => v.frequency));

  return (
    <aside className="flex w-80 shrink-0 flex-col rounded-xl border border-slate-200 bg-white">
      <div className="flex items-center justify-between border-b border-slate-100 px-4 py-3">
        <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wide">
          Variants
          <span className="ml-1.5 rounded-full bg-slate-100 px-2 py-0.5 text-[10px] font-bold text-slate-500">
            {variants.length}
          </span>
        </h3>
        {selectedVariants.size > 0 && (
          <button
            onClick={onClear}
            className="rounded-md px-2 py-0.5 text-[10px] font-medium text-cyan-600 hover:bg-cyan-50 transition-colors"
          >
            Clear
          </button>
        )}
      </div>

      <div className="flex-1 overflow-auto">
        {variants.map((v) => {
          const isSelected = selectedVariants.has(v.id);
          const isFiltered = selectedVariants.size > 0 && !isSelected;
          const barWidth = maxFreq > 0 ? (v.frequency / maxFreq) * 100 : 0;

          return (
            <button
              key={v.id}
              onClick={() => onToggle(v.id)}
              className={`group block w-full border-b border-slate-50 px-4 py-3 text-left transition-all ${
                isSelected
                  ? "bg-cyan-50/60"
                  : isFiltered
                    ? "bg-white opacity-35"
                    : "bg-white hover:bg-slate-50/80"
              }`}
            >
              {/* Header row */}
              <div className="flex items-center justify-between mb-1.5">
                <span className={`text-[11px] font-semibold ${isSelected ? "text-cyan-700" : "text-slate-500"}`}>
                  Variant {v.id + 1}
                </span>
                <div className="flex items-center gap-2">
                  <span className="text-[11px] font-medium tabular-nums text-slate-500">
                    {v.frequency.toLocaleString()}x
                  </span>
                  <span
                    className={`rounded-full px-2 py-0.5 text-[10px] font-bold tabular-nums ${
                      isSelected
                        ? "bg-cyan-600 text-white"
                        : "bg-slate-100 text-slate-500"
                    }`}
                  >
                    {v.percentage}%
                  </span>
                </div>
              </div>

              {/* Frequency bar */}
              <div className="h-1.5 w-full rounded-full bg-slate-100 mb-2">
                <div
                  className={`h-full rounded-full transition-all ${
                    isSelected ? "bg-cyan-500" : "bg-slate-300 group-hover:bg-cyan-400"
                  }`}
                  style={{ width: `${barWidth}%` }}
                />
              </div>

              {/* Activity sequence */}
              <div className="flex flex-wrap gap-0.5">
                {v.activities.map((act, i) => (
                  <span key={i} className="flex items-center">
                    <span className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${
                      isSelected ? "bg-cyan-100 text-cyan-800" : "bg-slate-100 text-slate-600"
                    }`}>
                      {act}
                    </span>
                    {i < v.activities.length - 1 && (
                      <span className="mx-0.5 text-[9px] text-slate-300">&rarr;</span>
                    )}
                  </span>
                ))}
              </div>
            </button>
          );
        })}
      </div>
    </aside>
  );
}
