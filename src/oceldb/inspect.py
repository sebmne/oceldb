"""Manifest-based inspection helpers."""

from __future__ import annotations

from dataclasses import dataclass
from html import escape
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from oceldb.ocel import OCEL


_STYLE = """
<style>
.oceldb-inspect {
  --oceldb-bg: #ffffff;
  --oceldb-surface: #f6f8fa;
  --oceldb-surface-soft: #fbfcfd;
  --oceldb-border: #d0d7de;
  --oceldb-border-soft: #eaeef2;
  --oceldb-text: #24292f;
  --oceldb-text-strong: #1f2328;
  --oceldb-text-muted: #57606a;
  --oceldb-token-bg: #f6f8fa;
  --oceldb-token-border: #d8dee4;
  --oceldb-token-text: #24292f;
  color: var(--oceldb-text);
  background: var(--oceldb-bg);
  border: 1px solid var(--oceldb-border);
  border-radius: 6px;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  line-height: 1.4;
  margin: 0.75rem 0;
  max-width: 960px;
  overflow: hidden;
}
.oceldb-inspect__header {
  align-items: baseline;
  background: var(--oceldb-surface);
  border-bottom: 1px solid var(--oceldb-border);
  display: flex;
  gap: 12px;
  justify-content: space-between;
  padding: 10px 12px;
}
.oceldb-inspect__title {
  color: var(--oceldb-text-strong);
  font-size: 14px;
  font-weight: 600;
  margin: 0;
}
.oceldb-inspect__subtitle {
  color: var(--oceldb-text-muted);
  font-size: 12px;
  white-space: nowrap;
}
.oceldb-inspect__body {
  padding: 0;
}
.oceldb-inspect__metrics {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(136px, 1fr));
  margin: 0;
}
.oceldb-inspect__metric {
  background: var(--oceldb-surface-soft);
  border-bottom: 1px solid var(--oceldb-border-soft);
  border-right: 1px solid var(--oceldb-border-soft);
  padding: 10px 12px;
}
.oceldb-inspect__metric dt {
  color: var(--oceldb-text-muted);
  font-size: 12px;
  font-weight: 500;
  margin: 0 0 2px;
}
.oceldb-inspect__metric dd {
  color: var(--oceldb-text-strong);
  font-size: 17px;
  font-weight: 600;
  margin: 0;
}
.oceldb-inspect__metric small,
.oceldb-inspect__muted {
  color: var(--oceldb-text-muted);
}
.oceldb-inspect__section {
  display: grid;
  gap: 12px;
  grid-template-columns: 132px minmax(0, 1fr);
  padding: 10px 12px;
}
.oceldb-inspect__section + .oceldb-inspect__section {
  border-top: 1px solid var(--oceldb-border-soft);
}
.oceldb-inspect__section-title {
  color: var(--oceldb-text-muted);
  font-size: 12px;
  font-weight: 600;
  margin: 2px 0 0;
}
.oceldb-inspect__chips {
  display: flex;
  flex-wrap: wrap;
  gap: 5px;
}
.oceldb-inspect__chip,
.oceldb-inspect__attr {
  background: var(--oceldb-token-bg);
  border: 1px solid var(--oceldb-token-border);
  border-radius: 4px;
  color: var(--oceldb-token-text);
  display: inline-flex;
  font-size: 12px;
  gap: 4px;
  padding: 2px 6px;
}
.oceldb-inspect__attrs {
  border-collapse: collapse;
  font-size: 13px;
  width: 100%;
}
.oceldb-inspect__attrs th,
.oceldb-inspect__attrs td,
.oceldb-inspect__table th,
.oceldb-inspect__table td {
  border-bottom: 1px solid var(--oceldb-border-soft);
  padding: 7px 10px;
  text-align: left;
  vertical-align: top;
}
.oceldb-inspect__attrs th {
  color: var(--oceldb-text-muted);
  font-weight: 500;
  width: 32%;
}
.oceldb-inspect__table {
  border-collapse: collapse;
  font-size: 13px;
  width: 100%;
}
.oceldb-inspect__table thead th {
  background: var(--oceldb-surface);
  color: var(--oceldb-text-muted);
  font-size: 12px;
  font-weight: 600;
  letter-spacing: 0;
}
.oceldb-inspect__table tbody tr:last-child th,
.oceldb-inspect__table tbody tr:last-child td,
.oceldb-inspect__attrs tbody tr:last-child th,
.oceldb-inspect__attrs tbody tr:last-child td {
  border-bottom: 0;
}
.oceldb-inspect__name {
  color: var(--oceldb-text-strong);
  font-weight: 600;
}
.oceldb-inspect__number {
  font-variant-numeric: tabular-nums;
  text-align: right;
  white-space: nowrap;
}
.oceldb-inspect code {
  background: transparent;
  color: inherit;
  font-size: 12px;
  font-weight: 500;
  padding: 0;
}
@media (max-width: 640px) {
  .oceldb-inspect__header,
  .oceldb-inspect__section {
    display: block;
  }
  .oceldb-inspect__subtitle {
    margin-top: 2px;
    white-space: normal;
  }
  .oceldb-inspect__section-title {
    margin-bottom: 6px;
  }
}
@media (prefers-color-scheme: dark) {
  .oceldb-inspect {
    --oceldb-bg: #111827;
    --oceldb-surface: #1f2937;
    --oceldb-surface-soft: #172033;
    --oceldb-border: #374151;
    --oceldb-border-soft: #2f3a49;
    --oceldb-text: #d1d5db;
    --oceldb-text-strong: #f3f4f6;
    --oceldb-text-muted: #9ca3af;
    --oceldb-token-bg: #1f2937;
    --oceldb-token-border: #374151;
    --oceldb-token-text: #e5e7eb;
  }
}
</style>
"""


def _panel_html(title: str, body: str, subtitle: str | None = None) -> str:
    subtitle_html = ""
    if subtitle:
        subtitle_html = (
            f'<div class="oceldb-inspect__subtitle">{escape(subtitle)}</div>'
        )
    return (
        _STYLE
        + '<div class="oceldb-inspect">'
        + '<div class="oceldb-inspect__header">'
        + f'<div class="oceldb-inspect__title">{escape(title)}</div>'
        + subtitle_html
        + "</div>"
        + f'<div class="oceldb-inspect__body">{body}</div>'
        + "</div>"
    )


def _metric_html(
    label: str,
    value: str,
    detail: str | None = None,
    *,
    value_is_html: bool = False,
) -> str:
    detail_html = ""
    if detail:
        detail_html = f"<small>{escape(detail)}</small>"
    value_html = value if value_is_html else escape(value)
    return (
        '<div class="oceldb-inspect__metric">'
        + f"<dt>{escape(label)}</dt>"
        + f"<dd>{value_html}</dd>"
        + detail_html
        + "</div>"
    )


def _section_html(title: str, body: str) -> str:
    return (
        '<div class="oceldb-inspect__section">'
        + f'<div class="oceldb-inspect__section-title">{escape(title)}</div>'
        + body
        + "</div>"
    )


def _time_range_html(time_min: str | None, time_max: str | None) -> str:
    if not time_min and not time_max:
        return '<span class="oceldb-inspect__muted">Not available</span>'
    if not time_min:
        return (
            '<span class="oceldb-inspect__muted">Unknown</span> &rarr; '
            f"{escape(time_max or '')}"
        )
    if not time_max:
        return (
            f"{escape(time_min)} &rarr; "
            '<span class="oceldb-inspect__muted">Unknown</span>'
        )
    return f"{escape(time_min)} &rarr; {escape(time_max)}"


def _chips_html(values: list[str], limit: int = 24) -> str:
    if not values:
        return '<span class="oceldb-inspect__muted">None</span>'
    visible = values[:limit]
    chips = [
        f'<span class="oceldb-inspect__chip">{escape(value)}</span>'
        for value in visible
    ]
    remaining = len(values) - len(visible)
    if remaining:
        chips.append(f'<span class="oceldb-inspect__chip">+ {remaining:,} more</span>')
    return '<div class="oceldb-inspect__chips">' + "".join(chips) + "</div>"


def _attributes_table_html(attributes: dict[str, str]) -> str:
    if not attributes:
        return '<span class="oceldb-inspect__muted">No attributes</span>'
    rows = "".join(
        "<tr>"
        + f"<th>{escape(name)}</th>"
        + f"<td><code>{escape(type_name)}</code></td>"
        + "</tr>"
        for name, type_name in attributes.items()
    )
    return f'<table class="oceldb-inspect__attrs"><tbody>{rows}</tbody></table>'


def _inline_attributes_html(attributes: dict[str, str], limit: int = 8) -> str:
    if not attributes:
        return '<span class="oceldb-inspect__muted">None</span>'
    visible = list(attributes.items())[:limit]
    attrs = [
        '<span class="oceldb-inspect__attr">'
        + f"<code>{escape(name)}</code>: {escape(type_name)}"
        + "</span>"
        for name, type_name in visible
    ]
    remaining = len(attributes) - len(visible)
    if remaining:
        attrs.append(f'<span class="oceldb-inspect__attr">+ {remaining:,} more</span>')
    return '<div class="oceldb-inspect__chips">' + "".join(attrs) + "</div>"


@dataclass(frozen=True)
class LogOverview:
    """High-level summary of an OCEL log."""

    event_count: int
    object_count: int
    e2o_count: int
    o2o_count: int
    time_min: str | None
    time_max: str | None
    event_types: list[str]
    object_types: list[str]

    def __str__(self) -> str:
        lines = [
            f"Events:        {self.event_count:>10,}  ({len(self.event_types)} types)",
            f"Objects:       {self.object_count:>10,}  ({len(self.object_types)} types)",
            f"E2O relations: {self.e2o_count:>10,}",
            f"O2O relations: {self.o2o_count:>10,}",
        ]
        if self.time_min:
            lines.append(f"Time range:    {self.time_min}  →  {self.time_max}")
        return "\n".join(lines)

    def _repr_html_(self) -> str:
        metrics = (
            '<dl class="oceldb-inspect__metrics">'
            + _metric_html(
                "Events", f"{self.event_count:,}", f"{len(self.event_types)} types"
            )
            + _metric_html(
                "Objects",
                f"{self.object_count:,}",
                f"{len(self.object_types)} types",
            )
            + _metric_html("E2O relations", f"{self.e2o_count:,}")
            + _metric_html("O2O relations", f"{self.o2o_count:,}")
            + "</dl>"
        )
        body = (
            metrics
            + _section_html(
                "Time range", _time_range_html(self.time_min, self.time_max)
            )
            + _section_html("Event types", _chips_html(self.event_types))
            + _section_html("Object types", _chips_html(self.object_types))
        )
        return _panel_html("OCEL overview", body)


@dataclass(frozen=True)
class EventTypeSummary:
    """Summary for one event type."""

    name: str
    count: int
    time_min: str | None
    time_max: str | None
    attributes: dict[str, str]

    def __str__(self) -> str:
        parts = [f"{self.name!r}  ({self.count:,} events)"]
        if self.attributes:
            attr_str = ", ".join(f"{k}: {v}" for k, v in self.attributes.items())
            parts.append(f"  attributes: {attr_str}")
        return "\n".join(parts)

    def _repr_html_(self) -> str:
        metrics = (
            '<dl class="oceldb-inspect__metrics">'
            + _metric_html("Events", f"{self.count:,}")
            + _metric_html(
                "Time range",
                _time_range_html(self.time_min, self.time_max),
                value_is_html=True,
            )
            + "</dl>"
        )
        body = metrics + _section_html(
            "Attributes", _attributes_table_html(self.attributes)
        )
        return _panel_html(f"Event type: {self.name}", body)

    def table_row_html(self) -> str:
        return (
            "<tr>"
            + f'<th scope="row" class="oceldb-inspect__name">{escape(self.name)}</th>'
            + f'<td class="oceldb-inspect__number">{self.count:,}</td>'
            + f"<td>{_time_range_html(self.time_min, self.time_max)}</td>"
            + f"<td>{_inline_attributes_html(self.attributes)}</td>"
            + "</tr>"
        )


@dataclass(frozen=True)
class ObjectTypeSummary:
    """Summary for one object type."""

    name: str
    object_count: int
    attributes: dict[str, str]

    def __str__(self) -> str:
        parts = [f"{self.name!r}  ({self.object_count:,} objects)"]
        if self.attributes:
            attr_str = ", ".join(f"{k}: {v}" for k, v in self.attributes.items())
            parts.append(f"  attributes: {attr_str}")
        return "\n".join(parts)

    def _repr_html_(self) -> str:
        metrics = (
            '<dl class="oceldb-inspect__metrics">'
            + _metric_html("Objects", f"{self.object_count:,}")
            + "</dl>"
        )
        body = metrics + _section_html(
            "Attributes", _attributes_table_html(self.attributes)
        )
        return _panel_html(f"Object type: {self.name}", body)

    def table_row_html(self) -> str:
        return (
            "<tr>"
            + f'<th scope="row" class="oceldb-inspect__name">{escape(self.name)}</th>'
            + f'<td class="oceldb-inspect__number">{self.object_count:,}</td>'
            + f"<td>{_inline_attributes_html(self.attributes)}</td>"
            + "</tr>"
        )


class EventTypeSummaries(list[EventTypeSummary]):
    """List of event type summaries with notebook HTML rendering."""

    def _repr_html_(self) -> str:
        if not self:
            return _panel_html(
                "Event types",
                '<span class="oceldb-inspect__muted">No event types</span>',
            )
        rows = "".join(summary.table_row_html() for summary in self)
        table = (
            '<table class="oceldb-inspect__table">'
            + "<thead><tr>"
            + "<th>Name</th><th>Events</th><th>Time range</th><th>Attributes</th>"
            + "</tr></thead>"
            + f"<tbody>{rows}</tbody>"
            + "</table>"
        )
        return _panel_html("Event types", table, f"{len(self):,} types")


class ObjectTypeSummaries(list[ObjectTypeSummary]):
    """List of object type summaries with notebook HTML rendering."""

    def _repr_html_(self) -> str:
        if not self:
            return _panel_html(
                "Object types",
                '<span class="oceldb-inspect__muted">No object types</span>',
            )
        rows = "".join(summary.table_row_html() for summary in self)
        table = (
            '<table class="oceldb-inspect__table">'
            + "<thead><tr>"
            + "<th>Name</th><th>Objects</th><th>Attributes</th>"
            + "</tr></thead>"
            + f"<tbody>{rows}</tbody>"
            + "</table>"
        )
        return _panel_html("Object types", table, f"{len(self):,} types")


def overview(ocel: OCEL) -> LogOverview:
    """Return a high-level summary of *ocel*."""
    m = ocel.manifest
    time_range: list[str | None] = m.totals.get("time_range") or [None, None]
    return LogOverview(
        event_count=m.totals["event_count"],
        object_count=m.totals["object_count"],
        e2o_count=m.totals["e2o_count"],
        o2o_count=m.totals["o2o_count"],
        time_min=time_range[0],
        time_max=time_range[1],
        event_types=list(m.event_types),
        object_types=list(m.object_types),
    )


def event_types(ocel: OCEL) -> list[EventTypeSummary]:
    """Return event types sorted by descending count."""
    summaries = sorted(
        [
            EventTypeSummary(
                name=name,
                count=info.count,
                time_min=info.time_range[0],
                time_max=info.time_range[1],
                attributes=dict(info.attributes),
            )
            for name, info in ocel.manifest.event_types.items()
        ],
        key=lambda s: s.count,
        reverse=True,
    )
    return EventTypeSummaries(summaries)


def object_types(ocel: OCEL) -> list[ObjectTypeSummary]:
    """Return object types sorted by descending count."""
    summaries = sorted(
        [
            ObjectTypeSummary(
                name=name,
                object_count=info.object_count,
                attributes=dict(info.attributes),
            )
            for name, info in ocel.manifest.object_types.items()
        ],
        key=lambda s: s.object_count,
        reverse=True,
    )
    return ObjectTypeSummaries(summaries)
