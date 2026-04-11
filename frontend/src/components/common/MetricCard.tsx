type Props = {
  label: string;
  value: string | number | null | undefined;
};

export default function MetricCard({ label, value }: Props) {
  return (
    <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
      <div className="text-[11px] font-medium uppercase tracking-wide text-slate-400">
        {label}
      </div>
      <div className="mt-1.5 text-xl font-bold text-slate-900">
        {value == null ? "\u2014" : String(value)}
      </div>
    </div>
  );
}
