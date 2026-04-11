type Props = {
  message: string;
};

export default function ErrorBanner({ message }: Props) {
  return (
    <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
      {message}
    </div>
  );
}
