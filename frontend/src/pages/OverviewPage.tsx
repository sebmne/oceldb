import { useOverview } from "../api/hooks";
import MetricCard from "../components/common/MetricCard";
import DataGrid from "../components/common/DataGrid";
import ErrorBanner from "../components/common/ErrorBanner";

export default function OverviewPage() {
  const { data, isLoading, error } = useOverview();

  if (isLoading) return <div className="text-sm text-slate-400">Loading overview...</div>;
  if (error) return <ErrorBanner message={error.message} />;
  if (!data) return null;

  const { overview: ov, event_type_counts, object_type_counts, event_object_stats: eo } = data;

  return (
    <div className="space-y-8">
      <section>
        <h2 className="mb-3 text-sm font-semibold text-slate-500 uppercase tracking-wide">
          Summary
        </h2>
        <div className="grid grid-cols-2 gap-3 xl:grid-cols-4">
          <MetricCard label="Events" value={ov.event_count.toLocaleString()} />
          <MetricCard label="Objects" value={ov.object_count.toLocaleString()} />
          <MetricCard label="Event Types" value={ov.event_type_count} />
          <MetricCard label="Object Types" value={ov.object_type_count} />
          <MetricCard label="Earliest Event" value={ov.earliest_event_time} />
          <MetricCard label="Latest Event" value={ov.latest_event_time} />
        </div>
      </section>

      <section>
        <h2 className="mb-3 text-sm font-semibold text-slate-500 uppercase tracking-wide">
          Event-Object Relationships
        </h2>
        <div className="grid grid-cols-2 gap-3 xl:grid-cols-3">
          <MetricCard label="Avg Objects / Event" value={eo.avg_objects_per_event?.toFixed(2)} />
          <MetricCard label="Min Objects / Event" value={eo.min_objects_per_event} />
          <MetricCard label="Max Objects / Event" value={eo.max_objects_per_event} />
          <MetricCard label="Avg Events / Object" value={eo.avg_events_per_object?.toFixed(2)} />
          <MetricCard label="Min Events / Object" value={eo.min_events_per_object} />
          <MetricCard label="Max Events / Object" value={eo.max_events_per_object} />
        </div>
      </section>

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-2">
        <section>
          <h2 className="mb-3 text-sm font-semibold text-slate-500 uppercase tracking-wide">
            Event Types
          </h2>
          <DataGrid
            columns={["Type", "Count"]}
            rows={Object.entries(event_type_counts).map(([k, v]) => [k, v])}
            maxHeight="360px"
          />
        </section>

        <section>
          <h2 className="mb-3 text-sm font-semibold text-slate-500 uppercase tracking-wide">
            Object Types
          </h2>
          <DataGrid
            columns={["Type", "Count"]}
            rows={Object.entries(object_type_counts).map(([k, v]) => [k, v])}
            maxHeight="360px"
          />
        </section>
      </div>
    </div>
  );
}
