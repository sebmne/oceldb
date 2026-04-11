type Props = {
  columns: string[];
  rows: unknown[][];
  maxHeight?: string;
};

export default function DataGrid({ columns, rows, maxHeight = "480px" }: Props) {
  return (
    <div
      className="overflow-auto rounded-xl border border-slate-200 bg-white"
      style={{ maxHeight }}
    >
      <table className="min-w-full text-[13px]">
        <thead className="sticky top-0 z-10 bg-slate-50/95 backdrop-blur-sm">
          <tr>
            {columns.map((col) => (
              <th
                key={col}
                className="border-b border-slate-200 px-4 py-2.5 text-left text-[11px] font-semibold uppercase tracking-wide text-slate-500"
              >
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {rows.length === 0 ? (
            <tr>
              <td
                colSpan={columns.length}
                className="px-4 py-8 text-center text-sm text-slate-400"
              >
                No data
              </td>
            </tr>
          ) : (
            rows.map((row, i) => (
              <tr key={i} className="hover:bg-blue-50/40 transition-colors">
                {row.map((cell, j) => (
                  <td key={j} className="px-4 py-2 align-top text-slate-700">
                    {cell === null ? (
                      <span className="italic text-slate-300">null</span>
                    ) : typeof cell === "object" ? (
                      <span className="font-mono text-xs text-slate-500">
                        {JSON.stringify(cell)}
                      </span>
                    ) : (
                      String(cell)
                    )}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}
