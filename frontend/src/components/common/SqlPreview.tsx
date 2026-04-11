type Props = {
  sql: string;
};

export default function SqlPreview({ sql }: Props) {
  return (
    <pre className="overflow-auto rounded-xl border border-slate-200 bg-slate-50 p-4 font-mono text-xs leading-relaxed text-slate-700">
      {sql}
    </pre>
  );
}
